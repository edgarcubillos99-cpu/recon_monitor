"""
Servicio de reconexiones. Hermano de ConnexMonitor (que solo desconecta).

Ciclo:
1. Candado propio en MySQL (connex_lock.name='reconnect_cycle').
2. Arranca de MySQL, no de client.list: clientes con circuitos en
   'disconnected' o 'reconnect_requested'.
3. Por cliente:
   a. Servicios en Ubersmith (client.service_list). Circuito cancelado/terminado/
      inexistente -> DELETE (solo si sigue en un estado que vigila este servicio).
   b. reconnect_requested con ticket: support.ticket_get. Closed -> se borra la
      fila (activo). Closed Incomplete / Deleted -> vuelve a disconnected y se
      pausa en connex_failed.
   c. disconnected evaluable (no FiberX, period != 0, active 1/2/3): si el
      servicio tiene 0 facturas vencidas -> reconectar.
   d. Un ticket por cliente con todos los servicios a reconectar, salvo ticket
      de reconexión abierto reciente en la cola o pausa en connex_failed.

Nunca toca disconnect_requested (lo cierra el bot de cortes), no crea tickets
de desconexión, no usa OpenAI y no marca línea base.
"""

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone

from core import rules
from core.rules import BUSINESS_TZ
from core.store import (
    SOURCE_TICKET,
    STATE_ACTIVE,
    STATE_DISCONNECTED,
    STATE_RECONNECT_REQUESTED,
    ReconnectStore,
)
from core.ubersmith_client import UbersmithAPIError, UbersmithClient

logger = logging.getLogger(__name__)

ACTION = "reconnect"

COUNTERS = (
    "circuits_seen",
    "candidates",
    "blocked_by_debt",
    "skipped_fiberx",
    "skipped_ignored",
    "skipped_recently_failed",
    "skipped_open_ticket",
    "skipped_state_changed",
    "skipped_no_services",
    "tickets_created",
    "reconnect_pending",
    "reconnected",
    "reconnect_failed",
    "deleted_gone",
)


class ReconnectBot:
    def __init__(self, client=None, store=None):
        self.client = client or UbersmithClient()
        self.store = store or ReconnectStore()
        self.dry_run = os.getenv("CONNEX_DRY_RUN", "1") != "0"
        self.page_size = int(os.getenv("CONNEX_PAGE_SIZE", self.client.page_size))
        self.lock_ttl = int(os.getenv("CONNEX_LOCK_TTL_SECONDS", "1800"))
        self.queue = str(os.getenv("CONNEX_QUEUE_RECONNECT") or os.getenv("SUPPORT_QUEUE_ID") or "57")
        self.open_ticket_max_age_days = float(os.getenv("CONNEX_OPEN_TICKET_MAX_AGE_DAYS", "5"))
        self.concurrency = max(1, int(os.getenv("CONNEX_CONCURRENCY", "8")))
        self._ticket_types = {}
        self._ticket_types_lock = threading.Lock()

    # --- Ciclo ---

    def process_clients(self, max_clients=None, client_ids=None):
        summary = {
            "clients_total": 0,
            "clients_scanned": 0,
            **{key: 0 for key in COUNTERS},
            "errors": 0,
            "dry_run": self.dry_run,
            "queue": self.queue,
            "actions": [],
            "blocked_report": [],
        }
        lock_token = self.store.acquire_lock(self.lock_ttl)
        if not lock_token:
            logger.warning("Hay otro ciclo de reconexión en curso; se omite este.")
            summary["skipped_locked"] = True
            return summary
        try:
            return self._process_locked(summary, max_clients, client_ids, lock_token)
        finally:
            self.store.release_lock(lock_token)

    def _process_locked(self, summary, max_clients, client_ids, lock_token):
        # Los tipos de ticket por cola se releen en cada ciclo.
        with self._ticket_types_lock:
            self._ticket_types = {}

        clients = self.store.clients_with_states()
        if client_ids:
            wanted = [str(client_id) for client_id in client_ids]
            known = set(clients)
            missing = [client_id for client_id in wanted if client_id not in known]
            if missing:
                logger.info(
                    "Sin circuitos desconectados/en reconexión en MySQL para: %s", ", ".join(missing)
                )
            clients = [client_id for client_id in wanted if client_id in known]
        if max_clients is not None:
            clients = clients[: max(0, max_clients)]
        summary["clients_total"] = len(clients)
        workers = min(self.concurrency, len(clients)) or 1
        logger.info(
            "Clientes a revisar: %s (concurrencia=%s, cola=%s, dry_run=%s)",
            len(clients),
            workers,
            self.queue,
            self.dry_run,
        )
        if not clients:
            return summary

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(self._process_one, client_id): client_id for client_id in clients}
            for future in as_completed(futures):
                client_id = futures[future]
                if not self.store.refresh_lock(lock_token, self.lock_ttl):
                    logger.error("Se perdió el candado del ciclo; se espera a los hilos en curso y se corta.")
                    summary["lock_lost"] = True
                    for pending in futures:
                        pending.cancel()
                    break
                summary["clients_scanned"] += 1
                try:
                    result = future.result()
                except UbersmithAPIError as exc:
                    logger.error("Cliente %s omitido por error de Ubersmith: %s", client_id, exc)
                    summary["errors"] += 1
                    continue
                except Exception:
                    logger.exception("Error procesando cliente %s", client_id)
                    summary["errors"] += 1
                    continue
                for key in COUNTERS:
                    summary[key] += result.get(key, 0)
                summary["actions"].extend(result["actions"])
                summary["blocked_report"].extend(result["blocked_report"])

        logger.info(
            "Ciclo terminado: clientes=%s circuitos=%s candidatos=%s con_mora=%s tickets=%s "
            "reconectados=%s fallidos=%s pendientes=%s borrados=%s errores=%s dry_run=%s",
            summary["clients_scanned"],
            summary["circuits_seen"],
            summary["candidates"],
            summary["blocked_by_debt"],
            summary["tickets_created"],
            summary["reconnected"],
            summary["reconnect_failed"],
            summary["reconnect_pending"],
            summary["deleted_gone"],
            summary["errors"],
            self.dry_run,
        )
        return summary

    # --- Un cliente ---

    def _process_one(self, client_id):
        client_id = str(client_id)
        result = {key: 0 for key in COUNTERS}
        result["actions"] = []
        result["blocked_report"] = []

        circuits = self.store.get_client_circuits(client_id)
        if not circuits:
            return result
        result["circuits_seen"] = len(circuits)

        services = self._list_services(client_id)
        if not services:
            # Sin ningún servicio vigente no se borra nada a ciegas: puede ser un fallo puntual.
            logger.warning(
                "Cliente %s: client.service_list vacío con %s circuitos guardados; se omite",
                client_id,
                len(circuits),
            )
            result["skipped_no_services"] = len(circuits)
            return result

        candidates = []
        outcomes = {}
        for service_id, record in circuits.items():
            state = record.get("state")
            service = services.get(service_id)
            kind = rules.classify_service(service)

            if kind == rules.SERVICE_GONE:
                self._delete_gone(client_id, service_id, state, service)
                result["deleted_gone"] += 1
                continue

            if state == STATE_RECONNECT_REQUESTED:
                key = self._sync_reconnect_ticket(client_id, service_id, record, outcomes)
                result[key] += 1
                continue

            if kind == rules.SERVICE_IGNORED:
                result["skipped_ignored"] += 1
                continue
            if kind == rules.SERVICE_FIBERX:
                result["skipped_fiberx"] += 1
                continue

            failed_ticket = self.store.recently_failed(service_id, ACTION)
            if failed_ticket is not None:
                logger.info(
                    "Cliente %s servicio %s omitido: reconexión %s no completada hace poco",
                    client_id,
                    service_id,
                    failed_ticket,
                )
                result["skipped_recently_failed"] += 1
                continue
            candidates.append((record, service))

        result["candidates"] = len(candidates)
        if not candidates:
            return result

        today = datetime.now(BUSINESS_TZ).date()
        evaluable_ids = {
            sid for sid, service in services.items() if rules.classify_service(service) == rules.SERVICE_EVALUABLE
        }
        invoices = self._list_overdue_invoices(client_id, today)
        overdue_by_service, unattributed = rules.group_overdue_by_service(invoices, evaluable_ids, today)
        reconnects, blocked = rules.decide_reconnects(candidates, overdue_by_service)
        result["blocked_by_debt"] = len(blocked)
        result["blocked_report"] = [{"client_id": client_id, **item} for item in blocked]
        if not reconnects:
            return result

        event = self._request_reconnect(client_id, reconnects, len(unattributed))
        if event["status"] == "created":
            result["tickets_created"] += 1
            result["actions"].append(event)
        elif event["status"] == "open_ticket":
            result["skipped_open_ticket"] += 1
        elif event["status"] == "state_changed":
            result["skipped_state_changed"] += 1
        return result

    def _delete_gone(self, client_id, service_id, state, service):
        status = service["status_code"] if service else "no existe"
        if self.dry_run:
            logger.info(
                "Dry-run: se borraría circuito %s de cliente %s (estado %s, servicio %s)",
                service_id,
                client_id,
                state,
                status,
            )
            return
        if self.store.delete_circuit(client_id, service_id, expected_state=state):
            logger.info(
                "Cliente %s servicio %s: cancelado/terminado (%s); circuito borrado", client_id, service_id, status
            )

    # --- Sincronización de tickets de reconexión ---

    def _sync_reconnect_ticket(self, client_id, service_id, record, outcomes):
        """Devuelve el contador a incrementar."""
        ticket_id = record.get("ticket_id")
        if not ticket_id:
            logger.warning(
                "Cliente %s servicio %s: reconnect_requested sin ticket_id; vuelve a disconnected",
                client_id,
                service_id,
            )
            if not self.dry_run:
                self._revert_to_disconnected(client_id, service_id, record, "reconnect_requested sin ticket")
            return "reconnect_failed"

        if ticket_id not in outcomes:
            outcomes[ticket_id] = self._ticket_outcome(ticket_id, record.get("queue") or self.queue)
        outcome = outcomes[ticket_id]
        if outcome is None:
            return "reconnect_pending"

        logger.info(
            "Cliente %s servicio %s: ticket de reconexión %s %s", client_id, service_id, ticket_id, outcome
        )
        if self.dry_run:
            return "reconnected" if outcome == "done" else "reconnect_failed"

        if outcome == "done":
            # state=active borra la fila: misma semántica que ConnexMonitor.
            self.store.set_circuit(client_id, service_id, STATE_ACTIVE, expected_state=STATE_RECONNECT_REQUESTED)
            return "reconnected"

        self._revert_to_disconnected(client_id, service_id, record, f"Reconexión no completada (ticket {ticket_id})")
        # Sin esta pausa, un "Closed Incomplete" haría pedir lo mismo en cada ciclo.
        self.store.mark_failed(service_id, ACTION, ticket_id, self.open_ticket_max_age_days * 86400)
        return "reconnect_failed"

    def _revert_to_disconnected(self, client_id, service_id, record, reason):
        self.store.set_circuit(
            client_id,
            service_id,
            STATE_DISCONNECTED,
            expected_state=STATE_RECONNECT_REQUESTED,
            cid=record.get("cid"),
            ticket_id=record.get("ticket_id"),
            queue=record.get("queue"),
            previous_state=STATE_RECONNECT_REQUESTED,
            reason=reason,
            requested_at=record.get("requested_at"),
            # Se conserva el origen (baseline sigue siendo baseline).
            source=record.get("source") or SOURCE_TICKET,
        )

    def _ticket_outcome(self, ticket_id, queue):
        """'done', 'failed' o None si el ticket sigue en proceso o no se pudo leer."""
        try:
            ticket = self.client.get("support.ticket_get", params={"ticket_id": ticket_id}).get("data") or {}
        except UbersmithAPIError as exc:
            logger.warning("No se pudo leer el ticket %s: %s", ticket_id, exc)
            return None
        type_id = str(ticket.get("type_id") or ticket.get("type") or "")
        type_name = str(ticket.get("type_name") or "")
        known = self._queue_ticket_types(queue).get(type_id)
        if known:
            type_name = known["name"] or type_name
            status = known["status"]
        else:
            status = {
                "closed": rules.CLOSED_TICKET_STATUS,
                "deleted": rules.DELETED_TICKET_STATUS,
            }.get(type_name.lower(), "")
        return rules.ticket_outcome_from_type(type_name, status)

    def _queue_ticket_types(self, queue):
        queue = str(queue)
        with self._ticket_types_lock:
            if queue not in self._ticket_types:
                payload = self.client.get("support.ticket_type_list", params={"queue": queue})
                self._ticket_types[queue] = {
                    str(item.get("ticket_type_id")): {
                        "name": str(item.get("name") or ""),
                        "status": str(item.get("status") or ""),
                    }
                    for item in rules.as_items(payload.get("data"))
                }
            return self._ticket_types[queue]

    # --- Pedido de reconexión ---

    def _request_reconnect(self, client_id, reconnects, unattributed_count):
        """Un solo ticket por cliente y ciclo con todos sus servicios a reconectar."""
        service_ids = [item["service_id"] for item in reconnects]
        single_service = service_ids[0] if len(service_ids) == 1 else None

        open_ticket = self._find_open_reconnect_ticket(client_id, single_service)
        if open_ticket:
            logger.info(
                "Cliente %s omitido (%s servicios): ticket de reconexión %s abierto en cola %s desde %s",
                client_id,
                len(service_ids),
                open_ticket["ticket_id"],
                self.queue,
                open_ticket["created_at"],
            )
            return {"status": "open_ticket", "client_id": client_id, "ticket_id": open_ticket["ticket_id"]}

        if not self.dry_run:
            # El bot de cortes pudo mover alguna fila mientras se consultaba Ubersmith.
            still = self.store.get_client_circuits(client_id, states=(STATE_DISCONNECTED,))
            reconnects = [item for item in reconnects if item["service_id"] in still]
            if not reconnects:
                logger.info("Cliente %s: los circuitos cambiaron de estado; no se pide reconexión", client_id)
                return {"status": "state_changed", "client_id": client_id}
            service_ids = [item["service_id"] for item in reconnects]
            single_service = service_ids[0] if len(service_ids) == 1 else None

        subject = rules.build_subject(client_id, service_ids)
        body = rules.build_body(client_id, reconnects, unattributed_count)

        ticket_id = None
        if self.dry_run:
            logger.info(
                "Dry-run: no se crea ticket queue=%s client=%s servicios=%s",
                self.queue,
                client_id,
                ",".join(service_ids),
            )
        else:
            ticket_id = self.client.submit_ticket(
                client_id=client_id,
                subject=subject,
                body=body,
                queue=self.queue,
                service_id=single_service,
            )
            if not ticket_id:
                raise UbersmithAPIError(f"support.ticket_submit no devolvió ticket_id para cliente {client_id}")
            logger.info(
                "Ticket de reconexión creado id=%s queue=%s client=%s servicios=%s",
                ticket_id,
                self.queue,
                client_id,
                ",".join(service_ids),
            )
            requested_at = datetime.now(timezone.utc)
            for item in reconnects:
                applied = self.store.set_circuit(
                    client_id,
                    item["service_id"],
                    STATE_RECONNECT_REQUESTED,
                    expected_state=STATE_DISCONNECTED,
                    cid=item.get("cid"),
                    ticket_id=ticket_id,
                    queue=self.queue,
                    previous_state=STATE_DISCONNECTED,
                    reason=item["reason"],
                    requested_at=requested_at.isoformat(),
                    source=item["source"],
                )
                if not applied:
                    logger.error(
                        "Cliente %s servicio %s: la fila dejó de estar en disconnected tras crear el ticket %s",
                        client_id,
                        item["service_id"],
                        ticket_id,
                    )
            self.store.record_request(client_id, ticket_id, self.queue, service_ids, requested_at)

        return {
            "status": "created",
            "type": ACTION,
            "client_id": client_id,
            "service_ids": service_ids,
            "ticket_id": ticket_id,
            "queue": self.queue,
            "reason": reconnects[0]["reason"] if single_service else f"{len(service_ids)} servicios",
            "dry_run": self.dry_run,
        }

    def _find_open_reconnect_ticket(self, client_id, service_id):
        """Ticket abierto de reconexión (por asunto) del cliente en la cola, de menos de N días."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.open_ticket_max_age_days)
        wanted_service = str(service_id or "")
        for ticket in self._list_open_queue_tickets(client_id, since=cutoff):
            created_at = rules.parse_unix_datetime(ticket.get("timestamp"))
            if created_at is None or created_at.astimezone(timezone.utc) < cutoff:
                continue
            if not rules.ticket_is_reconnect(ticket):
                continue
            ticket_service = str(ticket.get("service_id") or "")
            if wanted_service and ticket_service and ticket_service not in {"0", wanted_service}:
                continue
            return {
                "ticket_id": str(ticket.get("ticket_id") or ticket.get("id") or ""),
                "created_at": created_at.isoformat(),
            }
        return None

    def _list_open_queue_tickets(self, client_id, since):
        open_types = [
            type_id
            for type_id, info in self._queue_ticket_types(self.queue).items()
            if info["status"] in rules.OPEN_TICKET_STATUSES
        ]
        if not open_types:
            raise UbersmithAPIError(f"La cola {self.queue} no devolvió tipos de ticket abiertos")
        tickets = []
        seen = set()
        since_ts = int(since.timestamp())
        for ticket_type in open_types:
            payload = self.client.get_paginated(
                "support.ticket_list",
                params={
                    "client_id": client_id,
                    "queue": self.queue,
                    "type": ticket_type,
                    "begin": since_ts,
                    "internal_ticket": 2,
                    "order_by": "timestamp",
                    "direction": "desc",
                },
                page_size=self.page_size,
            )
            for item in (payload.get("data") or {}).values():
                if not isinstance(item, dict):
                    continue
                ticket_id = str(item.get("ticket_id") or item.get("id") or "")
                if ticket_id and ticket_id in seen:
                    continue
                if ticket_id:
                    seen.add(ticket_id)
                tickets.append(item)
        return tickets

    # --- Ubersmith: servicios y facturas ---

    def _list_services(self, client_id):
        """Servicios vigentes (pack_type_select=4) indexados por service_id, ya normalizados."""
        payload = self.client.get_paginated(
            "client.service_list",
            params={"client_id": client_id, "pack_type_select": 4, "metadata": 1},
            page_size=self.page_size,
        )
        now_ts = int(datetime.now(timezone.utc).timestamp())
        services = {}
        for item in (payload.get("data") or {}).values():
            if not isinstance(item, dict):
                continue
            service = rules.parse_service(item, now_ts)
            if service:
                services[service["service_id"]] = service
        return services

    def _list_overdue_invoices(self, client_id, today):
        payload = self.client.get_paginated(
            "client.invoice_list",
            params={"client_id": client_id, "paid": 0, "order_by": "due", "direction": "asc"},
            page_size=self.page_size,
        )
        invoices = []
        for item in (payload.get("data") or {}).values():
            if not isinstance(item, dict) or str(item.get("paid", "0")) != "0":
                continue
            due_dt = rules.parse_unix_datetime(item.get("due"))
            due_day = due_dt.date() if due_dt else None
            if not due_day or due_day >= today:
                continue
            invid = str(item.get("invid") or "")
            if not invid:
                continue
            invoices.append({"invid": invid, "due_date": due_day, "packs": self._invoice_packs(invid)})
        invoices.sort(key=lambda item: item["due_date"] or date.max)
        return invoices

    def _invoice_packs(self, invid):
        """
        Pares (packid, parentpack) con saldo. Si Ubersmith falla se propaga el error:
        reconectar sin saber a qué servicio pertenece la deuda sería peligroso.
        """
        cached = self.store.get_invoice_packs(invid)
        if cached is not None:
            return [tuple(pair) for pair in cached]
        detail = self.client.get("client.invoice_get", params={"invoice_id": invid}).get("data") or {}
        packs = rules.unpaid_packs_from_invoice(detail)
        self.store.cache_invoice_packs(invid, packs)
        return [tuple(pair) for pair in packs]
