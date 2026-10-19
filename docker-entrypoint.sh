#!/bin/sh
set -e
case "${MYSQL_HOST:-}" in
  localhost|127.0.0.1|::1)
    export MYSQL_HOST=host.docker.internal
    echo "MYSQL_HOST=localhost no sirve en Docker; se usa host.docker.internal"
    ;;
esac
python /app/wait_for_mysql.py
exec "$@"
