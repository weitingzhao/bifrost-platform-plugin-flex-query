#!/usr/bin/env bash
# Run the `db`-marked tests against a throwaway PostgreSQL server (TD-117; copied from
# bifrost-trade-core scripts/test_db.sh, TD-48).
#
#   make test-db                      # postgres:16-alpine in Docker, all db tests
#   make test-db PYTEST_ARGS='-k stats'
#   bash scripts/test_db.sh --sidecar # a postgres sidecar already on 127.0.0.1 (no Docker)
#
# Docker mode: the container listens on 127.0.0.1 only (a random free port), trusts local
# connections (no password: nothing in it outlives the run), and is removed on exit -- pass,
# fail or Ctrl-C.
#
# --sidecar mode is for the Tekton CI task (bifrost-trade-infra pipeline-ci-python.yaml), whose
# pod has no Docker: a postgres sidecar in the same pod listens on 127.0.0.1:${PGPORT:-5432}.
# Only a loopback PGHOST is accepted (anything else is refused), because a pod's loopback is
# reachable by that pod alone. Loopback alone is not proof, though: on a laptop 127.0.0.1 can be
# a `kubectl port-forward` to a shared database. So both modes also require the server to carry
# the marker setting `bifrost.throwaway=on`, which only these throwaway servers are started with
# (`-c bifrost.throwaway=on`); a real database never has it, and the run stops before any test.
#
# The tests commit (the code under test opens its own connections), into this disposable
# database, which is removed with the container.
#
# Never point this at a shared database: it sets PGHOST/PGPORT/PGUSER/PGDATABASE itself and
# ignores the caller's (except a loopback PGHOST and PGPORT in --sidecar mode), sends no
# password and reads no ~/.pgpass.
set -euo pipefail

MODE=docker
case "${1:-}" in
  "") ;;
  --sidecar) MODE=sidecar ;;
  *)
    echo "usage: $0 [--sidecar]" >&2
    exit 2
    ;;
esac

READY_TIMEOUT_S="${TEST_DB_READY_TIMEOUT_S:-60}"

# src/ first, so a worktree tests its own code rather than an editable install elsewhere.
export PYTHONPATH="$(pwd)/src${PYTHONPATH:+:$PYTHONPATH}"

# Connects as the env says; exits 0 once the server answers and carries the marker, 3 if it
# answers WITHOUT the marker (not a throwaway: stop), 1 if it never answered in time.
check_throwaway() {
  python - "$READY_TIMEOUT_S" <<'PY'
import sys
import time

import psycopg2

deadline = time.monotonic() + float(sys.argv[1])
last = None
while True:
    try:
        conn = psycopg2.connect(connect_timeout=3)
    except psycopg2.OperationalError as exc:
        last = exc
        if time.monotonic() >= deadline:
            print(f"test-db: no server answered: {last}", file=sys.stderr)
            sys.exit(1)
        time.sleep(1)
        continue
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_setting('bifrost.throwaway', true)")
            marker = cur.fetchone()[0]
    finally:
        conn.close()
    if marker != "on":
        print(
            "test-db: the server lacks bifrost.throwaway=on -- not a throwaway database; "
            "refusing to run the db tests against it",
            file=sys.stderr,
        )
        sys.exit(3)
    sys.exit(0)
PY
}

# PGPASSFILE names a path that cannot exist, so libpq reads no password file (and says nothing).
export PGUSER=bifrost PGDATABASE=bifrost PGPASSWORD= PGPASSFILE=/dev/null/no-pgpass
unset PGSERVICE PGSERVICEFILE PGHOSTADDR
unset GOLDEN_SOURCE_HOST GOLDEN_SOURCE_PORT GOLDEN_SOURCE_DATABASE GOLDEN_SOURCE_USER GOLDEN_SOURCE_PASSWORD

if [[ "$MODE" == sidecar ]]; then
  case "${PGHOST:-127.0.0.1}" in
    127.0.0.1 | localhost | ::1) ;;
    *)
      echo "test-db: --sidecar takes a loopback PGHOST only (got '${PGHOST}')" >&2
      exit 2
      ;;
  esac
  export PGHOST="${PGHOST:-127.0.0.1}" PGPORT="${PGPORT:-5432}"
  check_throwaway
  echo "test-db: sidecar on ${PGHOST}:${PGPORT}"
else
  IMAGE="${TEST_DB_IMAGE:-postgres:16-alpine}"
  NAME="bifrost-flex-testdb-$$"

  if ! command -v docker >/dev/null 2>&1; then
    echo "test-db: docker is required (start Docker Desktop, or run 'make test-all' with PGHOST set)" >&2
    exit 2
  fi

  cleanup() { docker rm -f "$NAME" >/dev/null 2>&1 || true; }
  trap cleanup EXIT INT TERM

  docker run -d --name "$NAME" \
    -e POSTGRES_USER=bifrost \
    -e POSTGRES_DB=bifrost \
    -e POSTGRES_HOST_AUTH_METHOD=trust \
    -p 127.0.0.1::5432 \
    "$IMAGE" -c bifrost.throwaway=on >/dev/null

  # The image restarts postgres once after initdb; wait for the final server, reachable over TCP.
  deadline=$((SECONDS + READY_TIMEOUT_S))
  until docker exec "$NAME" pg_isready -h 127.0.0.1 -U bifrost -d bifrost -q 2>/dev/null \
    && docker exec "$NAME" psql -h 127.0.0.1 -U bifrost -d bifrost -tAc 'SELECT 1' >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      echo "test-db: $IMAGE not ready after ${READY_TIMEOUT_S}s" >&2
      docker logs "$NAME" >&2 || true
      exit 1
    fi
    sleep 1
  done

  PORT="$(docker port "$NAME" 5432/tcp | head -n1 | sed 's/.*://')"
  export PGHOST=127.0.0.1 PGPORT="$PORT"
  check_throwaway
  echo "test-db: $IMAGE on 127.0.0.1:$PORT ($NAME)"
fi

# shellcheck disable=SC2086  # PYTEST_ARGS is a word list on purpose
pytest -q -m db ${PYTEST_ARGS:-}
