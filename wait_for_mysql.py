"""Espera a que el MySQL remoto acepte conexión antes de arrancar el bot."""
import os
import sys
import time

import pymysql

host = (os.getenv("MYSQL_HOST") or "").strip()
user = (os.getenv("MYSQL_USER") or "").strip()
if not host or not user:
    print("Faltan MYSQL_HOST y MYSQL_USER.", file=sys.stderr)
    sys.exit(1)
if host in {"localhost", "127.0.0.1", "::1"}:
    # Dentro de Docker, localhost es el contenedor. Si MySQL está en el host, se usa el gateway.
    host = "host.docker.internal"
    os.environ["MYSQL_HOST"] = host
    print("MYSQL_HOST=localhost no sirve en Docker; se usa host.docker.internal")

params = {
    "host": host,
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": user,
    "password": os.getenv("MYSQL_PASSWORD", ""),
    "connect_timeout": 8,
}
ssl_ca = os.getenv("MYSQL_SSL_CA")
if ssl_ca:
    params["ssl"] = {"ca": ssl_ca}

attempts = int(os.getenv("MYSQL_WAIT_ATTEMPTS", "30"))
delay = float(os.getenv("MYSQL_WAIT_SECONDS", "2"))

for attempt in range(1, attempts + 1):
    try:
        conn = pymysql.connect(**params)
        conn.close()
        print(f"MySQL listo en {host}:{params['port']}")
        sys.exit(0)
    except pymysql.err.OperationalError as exc:
        print(f"Esperando MySQL ({attempt}/{attempts}): {exc.args[0]}", flush=True)
        time.sleep(delay)

print(f"No se pudo conectar a MySQL en {host}:{params['port']}.", file=sys.stderr)
sys.exit(1)
