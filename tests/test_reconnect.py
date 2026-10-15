import os
import unittest
from datetime import date, datetime, timedelta, timezone
from unittest import mock

from core import rules
from core.bot import ReconnectBot
from core.store import (
    LOCK_NAME,
    STATE_DISCONNECT_REQUESTED,
    STATE_DISCONNECTED,
    STATE_RECONNECT_REQUESTED,
    STATUS_CONNECTED,
    STATUS_DISCONNECTED,
    WATCHED_STATES,
)

TODAY = datetime.now(rules.BUSINESS_TZ).date()


def _ts(day):
    return str(int(datetime(day.year, day.month, day.day, 12, tzinfo=rules.BUSINESS_TZ).timestamp()))


class FakeStore:
    """Imita ReconnectStore con transiciones condicionadas al estado e inventario connex_services."""

    def __init__(self, circuits):
        self.circuits = {sid: dict(row) for sid, row in circuits.items()}
        self.failed = {}
        self.requests = []
        self.packs = {}
        self.services_status = {}

    def acquire_lock(self, ttl):
        return "tok"

    def refresh_lock(self, token, ttl):
        return True

    def release_lock(self, token):
        pass

    def clients_with_states(self, states=WATCHED_STATES):
        return sorted({row["client_id"] for row in self.circuits.values() if row["state"] in states})

    def get_client_circuits(self, client_id, states=None):
        return {
            sid: {"service_id": sid, **row}
            for sid, row in self.circuits.items()
            if row["client_id"] == str(client_id) and (states is None or row["state"] in states)
        }

    def set_circuit(self, client_id, service_id, state, expected_state, **fields):
        row = self.circuits.get(service_id)
        if not row or row["client_id"] != client_id or row["state"] != expected_state:
            return False
        if state == "active":
            del self.circuits[service_id]
            return True
        self.circuits[service_id] = {"client_id": client_id, "state": state, **fields}
        return True

    def delete_circuit(self, client_id, service_id, expected_state):
        return self.set_circuit(client_id, service_id, "active", expected_state)

    def set_service_status(self, client_id, service_id, estado):
        self.services_status[(str(client_id), str(service_id))] = estado

    def sync_client_services(self, client_id, rows):
        client_id = str(client_id)
        wanted = {str(sid): estado for sid, estado in rows}
        for (cid, sid) in list(self.services_status.keys()):
            if cid == client_id and sid not in wanted:
                del self.services_status[(cid, sid)]
        for sid, estado in wanted.items():
            self.services_status[(client_id, sid)] = estado

    def mark_failed(self, service_id, action, ticket_id, ttl):
        self.failed[(service_id, action)] = ticket_id

    def recently_failed(self, service_id, action):
        return self.failed.get((service_id, action))

    def get_invoice_packs(self, invid):
        return self.packs.get(invid)

    def cache_invoice_packs(self, invid, packs):
        self.packs[invid] = packs

    def record_request(self, client_id, ticket_id, queue, service_ids, created_at):
        self.requests.append((client_id, ticket_id, queue, list(service_ids)))


class FakeUbersmith:
    page_size = 1000

    def __init__(self, services=None, invoices=None, invoice_details=None, tickets=None, open_tickets=None):
        self.services = services or {}
        self.invoices = invoices or {}
        self.invoice_details = invoice_details or {}
        self.tickets = tickets or {}
        self.open_tickets = open_tickets or {}
        self.submitted = []
        self.calls = []

    def get(self, method, params=None):
        self.calls.append(method)
        if method == "support.ticket_type_list":
            return {"data": [
                {"ticket_type_id": "1", "name": "Open", "status": "1"},
                {"ticket_type_id": "3", "name": "Closed", "status": "3"},
                {"ticket_type_id": "5", "name": "Closed Incomplete", "status": "3"},
            ]}
        if method == "support.ticket_get":
            return {"data": self.tickets[params["ticket_id"]]}
        if method == "client.invoice_get":
            return {"data": self.invoice_details[params["invoice_id"]]}
        raise AssertionError(f"llamada inesperada {method}")

    def get_paginated(self, method, params=None, page_size=None):
        self.calls.append(method)
        assert method != "client.list", "no se debe listar toda la base"
        cid = str(params["client_id"])
        if method == "client.service_list":
            return {"data": self.services.get(cid, {})}
        if method == "client.invoice_list":
            return {"data": self.invoices.get(cid, {})}
        if method == "support.ticket_list":
            return {"data": self.open_tickets.get(cid, {})}
        raise AssertionError(f"llamada inesperada {method}")

    def submit_ticket(self, **kwargs):
        self.submitted.append(kwargs)
        return str(9000 + len(self.submitted))


def svc(sid, title="Internet 50M", active="1", period="1", code="INET", provider="OSNET"):
    return {"packid": sid, "title": title, "active": active, "period": period, "code": code,
            "metadata": {"provider": provider, "cid": f"CID-{sid}"}}


def make_bot(store, client, dry_run="0"):
    with mock.patch.dict(os.environ, {"CONNEX_DRY_RUN": dry_run, "CONNEX_QUEUE_RECONNECT": "77"}):
        return ReconnectBot(client=client, store=store)


class RulesTest(unittest.TestCase):
    def test_line_unpaid(self):
        self.assertTrue(rules.invoice_line_is_unpaid({"unpaid": "10.00"}))
        self.assertFalse(rules.invoice_line_is_unpaid({"unpaid": "0"}))
        self.assertFalse(rules.invoice_line_is_unpaid({"unpaid": "5", "canceled": "1"}))

    def test_fiberx(self):
        self.assertTrue(rules.is_fiberx_service({"title": "Fiber Internet Circuit", "provider": "OSNET"}))
        self.assertTrue(rules.is_fiberx_service({"title": "Internet", "provider": "FIBERX"}))
        self.assertFalse(rules.is_fiberx_service({"title": "Internet", "provider": "OSNET"}))

    def test_classify(self):
        now = 10**10
        self.assertEqual(rules.classify_service(None), rules.SERVICE_GONE)
        self.assertEqual(rules.classify_service(rules.parse_service(svc("1", active="4"), now)), rules.SERVICE_GONE)
        self.assertEqual(rules.classify_service(rules.parse_service(svc("1", period="0"), now)), rules.SERVICE_IGNORED)
        self.assertEqual(rules.classify_service(rules.parse_service(svc("1", active="3"), now)), rules.SERVICE_EVALUABLE)

    def test_circuit_state(self):
        circuits = {"10": {"state": "disconnected"}, "20": {"state": "disconnect_requested"}}
        self.assertEqual(rules.circuit_state({"service_id": "10", "status_code": "1"}, circuits), "disconnected")
        self.assertEqual(rules.circuit_state({"service_id": "20", "status_code": "1"}, circuits), "disconnect_requested")
        self.assertEqual(rules.circuit_state({"service_id": "30", "status_code": "3"}, circuits), "disconnected")
        self.assertEqual(rules.circuit_state({"service_id": "30", "status_code": "1"}, circuits), "active")

    def test_overdue_attribution_and_baseline_note(self):
        invoices = [
            {"invid": "a", "due_date": TODAY - timedelta(days=3), "packs": [("99", "10")]},  # add-on de 10
            {"invid": "b", "due_date": TODAY, "packs": [("20", "0")]},  # vence hoy: no vencida
        ]
        by_service, _ = rules.group_overdue_by_service(invoices, {"10", "20"}, TODAY)
        self.assertEqual(set(by_service), {"10"})
        candidates = [({"source": "ticket"}, {"service_id": "10"}), ({"source": "baseline"}, {"service_id": "20"})]
        reconnect, blocked = rules.decide_reconnects(candidates, by_service)
        self.assertEqual([b["service_id"] for b in blocked], ["10"])
        self.assertIn(rules.BASELINE_NOTE, reconnect[0]["reason"])

    def test_ticket_is_reconnect(self):
        self.assertTrue(rules.ticket_is_reconnect({"subject": "Reconexión de servicios - Cliente 1"}))
        self.assertFalse(rules.ticket_is_reconnect({"subject": "Desconexión de servicios - Cliente 1"}))
        self.assertFalse(rules.ticket_is_reconnect({"subject": "Consulta general"}))


class BotTest(unittest.TestCase):
    def test_lock_name_distinct(self):
        self.assertEqual(LOCK_NAME, "reconnect_cycle")

    def test_full_cycle_and_inventory(self):
        overdue = _ts(TODAY - timedelta(days=10))
        store = FakeStore({
            "10": {"client_id": "1", "state": STATE_DISCONNECTED, "source": "baseline"},  # sin mora -> reconectar
            "11": {"client_id": "1", "state": STATE_DISCONNECTED, "source": "ticket"},    # con mora -> bloqueado
            "12": {"client_id": "1", "state": STATE_DISCONNECTED},                        # FiberX
            "13": {"client_id": "1", "state": STATE_DISCONNECTED},                        # cancelado -> borrar
            "14": {"client_id": "1", "state": STATE_DISCONNECT_REQUESTED, "ticket_id": "5"},  # bot cortes: no tocar
            "20": {"client_id": "2", "state": STATE_RECONNECT_REQUESTED, "ticket_id": "700", "source": "baseline"},
            "30": {"client_id": "3", "state": STATE_RECONNECT_REQUESTED, "ticket_id": "800", "source": "ticket"},
        })
        client = FakeUbersmith(
            services={
                "1": {"10": svc("10"), "11": svc("11"), "12": svc("12", title="FiberX 100"),
                      "13": svc("13", active="4"), "14": svc("14")},
                "2": {"20": svc("20")},
                "3": {"30": svc("30")},
            },
            invoices={"1": {"i1": {"invid": "i1", "paid": "0", "due": overdue}}},
            invoice_details={"i1": {"current_packs": {"x": {"packid": "11", "parentpack": "0", "unpaid": "30"}}}},
            tickets={"700": {"type_id": "3"}, "800": {"type_id": "5"}},
        )
        bot = make_bot(store, client)
        summary = bot.process_clients()

        self.assertEqual(len(client.submitted), 1)
        sub = client.submitted[0]
        self.assertEqual(sub["queue"], "77")
        self.assertEqual(sub["subject"], "Reconexión de servicios - Cliente 1 - Servicio 10")
        self.assertIn(rules.BASELINE_NOTE, sub["body"])
        self.assertEqual(store.circuits["10"]["state"], STATE_RECONNECT_REQUESTED)
        self.assertEqual(store.circuits["10"]["source"], "baseline")
        self.assertEqual(store.circuits["10"]["previous_state"], STATE_DISCONNECTED)
        self.assertEqual(store.circuits["11"]["state"], STATE_DISCONNECTED)
        self.assertEqual(store.circuits["12"]["state"], STATE_DISCONNECTED)
        self.assertNotIn("13", store.circuits)
        self.assertEqual(store.circuits["14"], {"client_id": "1", "state": STATE_DISCONNECT_REQUESTED, "ticket_id": "5"})
        self.assertNotIn("20", store.circuits)  # Closed -> activo (borrado de connex_circuits)
        self.assertEqual(store.circuits["30"]["state"], STATE_DISCONNECTED)  # Closed Incomplete
        self.assertEqual(store.failed[("30", "reconnect")], "800")
        self.assertEqual(store.requests[0][3], ["10"])
        self.assertEqual(summary["reconnected"], 1)
        self.assertEqual(summary["reconnect_failed"], 1)
        self.assertEqual(summary["blocked_by_debt"], 1)
        self.assertEqual(summary["skipped_fiberx"], 1)
        self.assertEqual(summary["deleted_gone"], 1)

        # Verificación del inventario connex_services:
        # Cliente 2: ticket 700 Closed OK -> conectado
        self.assertEqual(store.services_status.get(("2", "20")), STATUS_CONNECTED)
        # Cliente 3: ticket 800 Closed Incomplete -> desconectado
        self.assertEqual(store.services_status.get(("3", "30")), STATUS_DISCONNECTED)
        # Cliente 1: servicio 10 pasa a reconnect_requested -> desconectado
        self.assertEqual(store.services_status.get(("1", "10")), STATUS_DISCONNECTED)
        # Cliente 1: servicio 11 con mora -> desconectado
        self.assertEqual(store.services_status.get(("1", "11")), STATUS_DISCONNECTED)
        # Cliente 1: servicio 14 en disconnect_requested (del bot de cortes) -> NUNCA conectado, siempre desconectado
        self.assertEqual(store.services_status.get(("1", "14")), STATUS_DISCONNECTED)
        # Cliente 1: servicio 12 (FiberX) y 13 (cancelado) -> eliminados de connex_services
        self.assertNotIn(("1", "12"), store.services_status)
        self.assertNotIn(("1", "13"), store.services_status)

    def test_dry_run_is_read_only(self):
        store = FakeStore({
            "10": {"client_id": "1", "state": STATE_DISCONNECTED},
            "13": {"client_id": "1", "state": STATE_DISCONNECTED},
        })
        client = FakeUbersmith(services={"1": {"10": svc("10"), "13": svc("13", active="4")}})
        summary = make_bot(store, client, dry_run="1").process_clients()
        self.assertEqual(client.submitted, [])
        self.assertEqual(store.circuits["10"]["state"], STATE_DISCONNECTED)
        self.assertIn("13", store.circuits)
        self.assertEqual(summary["tickets_created"], 1)
        self.assertTrue(summary["actions"][0]["dry_run"])
        # En dry run no se pone conectado ni se altera connex_services
        self.assertEqual(store.services_status, {})

    def test_open_reconnect_ticket_blocks_but_disconnect_ticket_does_not(self):
        recent = str(int(datetime.now(timezone.utc).timestamp()) - 3600)
        store = FakeStore({"10": {"client_id": "1", "state": STATE_DISCONNECTED},
                           "40": {"client_id": "4", "state": STATE_DISCONNECTED}})
        client = FakeUbersmith(
            services={"1": {"10": svc("10")}, "4": {"40": svc("40")}},
            open_tickets={
                "1": {"t1": {"ticket_id": "t1", "subject": "Reconexión de servicios - Cliente 1", "timestamp": recent}},
                "4": {"t2": {"ticket_id": "t2", "subject": "Desconexión de servicios - Cliente 4", "timestamp": recent}},
            },
        )
        summary = make_bot(store, client).process_clients()
        self.assertEqual(summary["skipped_open_ticket"], 1)
        self.assertEqual([s["client_id"] for s in client.submitted], ["4"])

    def test_recently_failed_skips(self):
        store = FakeStore({"10": {"client_id": "1", "state": STATE_DISCONNECTED}})
        store.failed[("10", "reconnect")] = "800"
        client = FakeUbersmith(services={"1": {"10": svc("10")}})
        summary = make_bot(store, client).process_clients()
        self.assertEqual(client.submitted, [])
        self.assertEqual(summary["skipped_recently_failed"], 1)
        self.assertNotIn("client.invoice_list", client.calls)

    def test_one_ticket_per_client_multiple_services(self):
        store = FakeStore({"10": {"client_id": "1", "state": STATE_DISCONNECTED},
                           "11": {"client_id": "1", "state": STATE_DISCONNECTED}})
        client = FakeUbersmith(services={"1": {"10": svc("10"), "11": svc("11")}})
        make_bot(store, client).process_clients()
        self.assertEqual(len(client.submitted), 1)
        self.assertEqual(client.submitted[0]["subject"], "Reconexión de servicios - Cliente 1 - 2 servicios")
        self.assertIsNone(client.submitted[0]["service_id"])

    def test_client_id_filter_only_db_clients(self):
        store = FakeStore({"10": {"client_id": "1", "state": STATE_DISCONNECTED}})
        client = FakeUbersmith(services={"1": {"10": svc("10")}})
        summary = make_bot(store, client).process_clients(client_ids=["999"])
        self.assertEqual(summary["clients_total"], 0)
        self.assertEqual(client.calls, [])


if __name__ == "__main__":
    unittest.main()
