import argparse
import json
import logging
import os
import time

from dotenv import load_dotenv

from core.bot import ReconnectBot


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
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    bot = ReconnectBot()
    interval = args.interval if args.interval is not None else int(os.getenv("CONNEX_INTERVAL_SECONDS", "3600"))

    cycle = 1
    while True:
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
        logging.info("Esperando %s segundos hasta el próximo ciclo", interval)
        time.sleep(interval)
        cycle += 1


if __name__ == "__main__":
    main()
