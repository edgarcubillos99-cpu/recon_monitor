"""
Reglas de reconexión en Python puro (sin I/O, sin IA).

Las funciones de parseo, atribución de facturas y exclusión de FiberX son
copia de ConnexMonitor (core/bot.py) para que ambos servicios interpreten la
deuda exactamente igual. La decisión de reconexión es propia de este servicio.
"""

import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

BUSINESS_TZ = ZoneInfo("America/Puerto_Rico")

# Campo "active" del servicio en Ubersmith: 1 activo, 2 pendiente, 3 suspendido, 4 cancelado.
EVALUABLE_SERVICE_CODES = {"1", "2", "3"}

FIBERX_PROVIDERS = {"fiberx", "fiber x", "fibra"}
OSNET_PROVIDERS = {"osnet", "osn"}
FIBERX_TITLE_RE = re.compile(r"(?i)\bfiberx\b|\bfiber\b|\bfibra\b")
FIBERX_CODE_RE = re.compile(r"(?i)^(fiber|fdata|fbus|fres|fx)")

# Estado del tipo de ticket en Ubersmith: 1 abierto, 2 en espera, 3 cerrado, 4 borrado.
OPEN_TICKET_STATUSES = {"1", "2"}
CLOSED_TICKET_STATUS = "3"
DELETED_TICKET_STATUS = "4"

# Pistas de asunto. La cola puede ser la misma que la de cortes (57), así que
# solo cuenta como "ticket de reconexión" el que lo dice en el asunto.
RECONNECT_SUBJECT_HINTS = ("reconex", "reconect", "reconnect")

BASELINE_NOTE = "(marcado como desconectado en la línea base inicial; confirmar estado físico)"

# Clasificación de un circuito guardado frente a lo que dice Ubersmith.
SERVICE_GONE = "gone"            # ya no existe, cancelado o terminado: se borra la fila
SERVICE_IGNORED = "ignored"      # period=0: no es un circuito recurrente; no se toca
SERVICE_FIBERX = "fiberx"        # FiberX: nunca se reconecta; no se toca
SERVICE_EVALUABLE = "evaluable"  # se evalúa la mora

SUSPENDED_SERVICE_CODE = "3"


def circuit_state(service, circuits):
    record = circuits.get(service["service_id"])
    if record:
        return record.get("state") or "active"
    if service.get("status_code") == SUSPENDED_SERVICE_CODE:
        return "disconnected"
    return "active"


def parse_money(value):
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value).replace(",", "").strip())
    except (InvalidOperation, ValueError):
        return None


def parse_unix_datetime(value):
    try:
        timestamp = int(value)
    except (TypeError, ValueError):
        return None
    if timestamp <= 0:
        return None
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).astimezone(BUSINESS_TZ)


def as_items(value):
    if isinstance(value, dict):
        return [item for item in value.values() if isinstance(item, dict)]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def invoice_line_is_unpaid(line):
    """
    Una factura impaga lista todos los servicios del periodo, también los ya cubiertos.
    Solo cuenta la línea si Ubersmith le dejó un saldo (unpaid > 0) y no está cancelada.
    """
    if str(line.get("canceled") or "0") not in {"0", ""}:
        return False
    unpaid = parse_money(line.get("unpaid"))
    if unpaid is not None:
        return unpaid > 0
    value = parse_money(line.get("value") or line.get("cost"))
    paid = parse_money(line.get("paid")) or Decimal("0")
    return value is not None and paid < value


def unpaid_packs_from_invoice(detail):
    """Pares [packid, parentpack] con saldo, leídos de client.invoice_get."""
    packs = []
    for section in ("current_packs", "pre_billed_packs"):
        for line in as_items(detail.get(section)):
            if not invoice_line_is_unpaid(line):
                continue
            packid = str(line.get("packid") or "")
            if packid:
                packs.append([packid, str(line.get("parentpack") or "0")])
    return packs


def is_fiberx_service(service):
    """
    FiberX y OSNet conviven en el mismo Ubersmith.
    Título o código con Fiber/FiberX/Fibra se excluye siempre, aunque
    metadata.provider diga OSNET. Si no hay pista en el nombre, el provider decide.
    """
    title = service.get("title") or ""
    code = service.get("code") or ""
    if FIBERX_TITLE_RE.search(title) or FIBERX_CODE_RE.search(code):
        return True
    provider = str(service.get("provider") or "").strip().lower()
    if provider in FIBERX_PROVIDERS:
        return True
    return False


def parse_service(item, now_ts):
    """Normaliza un ítem de client.service_list. None si no trae id."""
    service_id = item.get("packid") or item.get("service_id")
    if not service_id:
        return None
    metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
    try:
        end_ts = int(item.get("end") or 0)
    except (TypeError, ValueError):
        end_ts = 0
    return {
        "service_id": str(service_id),
        "title": (item.get("title") or item.get("desserv") or item.get("code") or "").strip(),
        "code": str(item.get("code") or "").strip(),
        "status_code": str(item.get("active") or ""),
        "period": str(item.get("period") or "0"),
        "provider": str(metadata.get("provider") or "").strip(),
        "cid": metadata.get("cid"),
        "terminated": bool(end_ts and end_ts < now_ts),
    }


def classify_service(service):
    """Qué hacer con un circuito guardado según el servicio en Ubersmith (None = no existe)."""
    if service is None:
        return SERVICE_GONE
    if service["period"] == "0":
        return SERVICE_IGNORED
    if service["status_code"] not in EVALUABLE_SERVICE_CODES or service["terminated"]:
        return SERVICE_GONE
    if is_fiberx_service(service):
        return SERVICE_FIBERX
    return SERVICE_EVALUABLE


def group_overdue_by_service(invoices, service_ids, today):
    """
    Asigna cada factura vencida (due_date < hoy) a sus servicios por packid o,
    si la línea es un add-on, por parentpack. Lo que no es de ningún servicio
    evaluable queda aparte (no bloquea reconexiones).
    """
    by_service = {}
    unattributed = []
    for invoice in invoices:
        if not invoice.get("due_date") or invoice["due_date"] >= today:
            continue
        targets = set()
        for packid, parentpack in invoice.get("packs") or []:
            if packid in service_ids:
                targets.add(packid)
            elif parentpack in service_ids:
                targets.add(parentpack)
        if not targets:
            unattributed.append(invoice)
        for service_id in targets:
            by_service.setdefault(service_id, []).append(invoice)
    return by_service, unattributed


def decide_reconnects(candidates, overdue_by_service):
    """
    candidates: lista de (record, service) con record.state == disconnected y
    servicio evaluable. Devuelve (reconectar, bloqueados_por_mora).
    Se reconecta solo si ese servicio tiene 0 facturas vencidas.
    """
    reconnect = []
    blocked = []
    for record, service in candidates:
        service_id = service["service_id"]
        overdue = overdue_by_service.get(service_id, [])
        if overdue:
            blocked.append({"service_id": service_id, "overdue": len(overdue)})
            continue
        reason = "Sin facturas vencidas del servicio"
        if record.get("source") == "baseline":
            reason = f"{reason} {BASELINE_NOTE}"
        reconnect.append(
            {
                "service_id": service_id,
                "reason": reason,
                "source": record.get("source") or "ticket",
                "cid": service.get("cid") or record.get("cid"),
                "title": service.get("title") or "",
            }
        )
    return reconnect, blocked


def ticket_is_reconnect(ticket):
    """Un ticket abierto bloquea solo si su asunto dice que es de reconexión."""
    subject = (ticket.get("subject") or "").strip().lower()
    return any(hint in subject for hint in RECONNECT_SUBJECT_HINTS)


def ticket_outcome_from_type(type_name, status):
    """'done', 'failed' o None (sigue en proceso)."""
    if status == CLOSED_TICKET_STATUS:
        return "failed" if "incomplete" in (type_name or "").lower() else "done"
    if status == DELETED_TICKET_STATUS:
        return "failed"
    return None


def build_subject(client_id, service_ids):
    if len(service_ids) == 1:
        return f"Reconexión de servicios - Cliente {client_id} - Servicio {service_ids[0]}"
    return f"Reconexión de servicios - Cliente {client_id} - {len(service_ids)} servicios"


def build_body(client_id, reconnects, unattributed_overdue):
    lines = []
    for item in reconnects:
        cid = f" (CID {item['cid']})" if item.get("cid") else ""
        lines.append(f"- Servicio {item['service_id']}: {item['title']}{cid} — {item['reason']}")
    note = (
        f"El cliente mantiene {unattributed_overdue} factura(s) vencida(s) no atribuida(s) a "
        "estos servicios; no bloquean la reconexión."
        if unattributed_overdue
        else "No hay otras facturas vencidas relevantes."
    )
    return (
        "Solicitud automática de reconexión de servicios.\n\n"
        f"Cliente: {client_id}\n"
        "Acción: reconnect\n\n"
        "Servicios a reconectar (sin facturas vencidas del servicio):\n"
        f"{chr(10).join(lines)}\n\n"
        f"{note}\n"
    )
