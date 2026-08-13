#!/usr/bin/env bash
# Spins up a throwaway, project-local Postgres cluster for integration tests. Deliberately does
# NOT touch any system-wide Postgres install/service: data directory and port are both scoped to
# this repo so it's safe to run alongside other unrelated Postgres instances on the same machine.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PGDATA="$ROOT_DIR/.pgdata-test"
PGPORT="${SMORCH_TEST_PG_PORT:-55418}"
LOGFILE="$ROOT_DIR/.pgdata-test.log"

start() {
  if [ -f "$PGDATA/postmaster.pid" ]; then
    echo "Test cluster already running (PGDATA=$PGDATA)."
    exit 0
  fi
  if [ ! -d "$PGDATA" ]; then
    initdb -D "$PGDATA" -U postgres --auth=trust --no-sync >/dev/null
  fi
  pg_ctl -D "$PGDATA" -o "-p $PGPORT -k $PGDATA -c listen_addresses=''" -l "$LOGFILE" start
  for i in $(seq 1 30); do
    if pg_isready -h "$PGDATA" -p "$PGPORT" -U postgres >/dev/null 2>&1; then
      break
    fi
    sleep 0.2
  done
  psql -h "$PGDATA" -p "$PGPORT" -U postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'smorch_test'" | grep -q 1 \
    || psql -h "$PGDATA" -p "$PGPORT" -U postgres -c "CREATE DATABASE smorch_test" >/dev/null
  echo "Test cluster ready on unix socket $PGDATA, port $PGPORT, db smorch_test."
  echo "Connection string: postgresql://postgres@localhost/smorch_test?host=$PGDATA&port=$PGPORT"
}

stop() {
  if [ -f "$PGDATA/postmaster.pid" ]; then
    pg_ctl -D "$PGDATA" stop -m fast >/dev/null
    echo "Test cluster stopped."
  else
    echo "Test cluster not running."
  fi
}

destroy() {
  stop || true
  rm -rf "$PGDATA" "$LOGFILE"
  echo "Test cluster data removed."
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  destroy) destroy ;;
  *) echo "Usage: $0 {start|stop|destroy}"; exit 1 ;;
esac
