import argparse
import json
import logging
import os
import time
from datetime import datetime, timedelta

from dotenv import load_dotenv

from core.bot import ReconnectBot
from core.rules import BUSINESS_TZ


WEEKDAYS = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}
DEFAULT_RUN_DAYS = "mon,tue,wed,thu"


def _parse_run_days(value):
    """'mon,tue,wed,thu' -> {0, 1, 2, 3} (weekday() de Python, lunes=0)."""
    days = set()
    for part in (value or DEFAULT_RUN_DAYS).split(","):
        key = part.strip().lower()[:3]
        if key not in WEEKDAYS:
            raise SystemExit(f"CONNEX_RUN_DAYS inválido: {part!r} (usa mon,tue,wed,thu,fri,sat,sun)")
        days.add(WEEKDAYS[key])
    return days


def _seconds_until(daily_at, run_days):
    """Segundos hasta la próxima ocurrencia de HH:MM en un día permitido (zona del negocio)."""
    hour, minute = (int(part) for part in daily_at.split(":"))
    now = datetime.now(BUSINESS_TZ)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    while target.weekday() not in run_days:
        target += timedelta(days=1)
    return max(1, int((target - now).total_seconds())), target


def _print_summary(summary):
    printable = dict(summary)
    printable["actions"] = [
        {
            "type": action.get("type"),
            "client_id": action.get("client_id"),
            "ticket_id": action.get("ticket_id"),
            "queue": action.get("queue"),
            "service_ids": action.get("service_ids"),
            "reason": action.get("reason"),
            "dry_run": action.get("dry_run"),
        }
        for action in summary.get("actions", [])
    ]
    print(json.dumps(printable, indent=2, ensure_ascii=False, default=str))


def main():
    # override=False: las variables ya exportadas en el entorno mandan sobre el .env.
    load_dotenv(override=False)
    parser = argparse.ArgumentParser(
        description="ConnexMonitor Reconexiones: pide reconexión de circuitos sin facturas vencidas"
    )
    parser.add_argument("--once", action="store_true", help="Ejecuta un solo ciclo y termina")
    parser.add_argument("--max-clients", type=int, default=None, help="Limita el número de clientes (pruebas)")
    parser.add_argument("--client-id", action="append", dest="client_ids", help="Procesa solo estos client_id")
    parser.add_argument(
        "--interval",
        type=int,
        default=None,
        help="Segundos entre ciclos (default: CONNEX_INTERVAL_SECONDS o 3600)",
    )
    parser.add_argument(
        "--daily-at",
        default=None,
        metavar="HH:MM",
        help="Ejecuta una vez al día a esa hora (zona America/Puerto_Rico). Default: CONNEX_DAILY_AT",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    bot = ReconnectBot()
    interval = args.interval if args.interval is not None else int(os.getenv("CONNEX_INTERVAL_SECONDS", "3600"))
    daily_at = args.daily_at or os.getenv("CONNEX_DAILY_AT") or None
    run_days = _parse_run_days(os.getenv("CONNEX_RUN_DAYS"))
    if daily_at:
        _seconds_until(daily_at, run_days)  # valida el formato antes de entrar al bucle
    run_on_start = os.getenv("CONNEX_RUN_ON_START", "0") == "1"

    cycle = 1
    while True:
        if daily_at and not args.once:
            if cycle == 1 and run_on_start and datetime.now(BUSINESS_TZ).weekday() in run_days:
                pass  # ejecuta ya, solo si hoy es un día permitido
            else:
                wait, target = _seconds_until(daily_at, run_days)
                logging.info("Próxima ejecución diaria: %s (en %s s)", target.isoformat(), wait)
                time.sleep(wait)
        logging.info("Inicio de ciclo de reconexión %s", cycle)
        try:
            summary = bot.process_clients(max_clients=args.max_clients, client_ids=args.client_ids)
        except Exception:
            logging.exception("El ciclo %s falló; se reintenta en el próximo ciclo", cycle)
            if args.once:
                raise SystemExit(1)
        else:
            _print_summary(summary)
        if args.once:
            return
        if not daily_at:
            logging.info("Esperando %s segundos hasta el próximo ciclo", interval)
            time.sleep(interval)
        cycle += 1


if __name__ == "__main__":
    main()
