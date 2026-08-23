#!/usr/bin/env bash
# Local Flex Query API — inherit CNPG credentials from trade-infra .env when present.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

INFRA_ENV="${BIFROST_TRADE_INFRA_ENV:-$HOME/Desktop/stocks/bifrost-trade-infra/.env}"

if [[ -f "$INFRA_ENV" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$INFRA_ENV"
  set +a
fi

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

# CNPG Golden Source + Trade DB (override stale passwords in config/flex-query.yaml).
if [[ -n "${PGPASSWORD:-}" ]]; then
  export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-$PGPASSWORD}"
  export FLEX_TRADE_PG_PASSWORD="${FLEX_TRADE_PG_PASSWORD:-$PGPASSWORD}"
fi
export POSTGRES_DB="${POSTGRES_DB:-bifrost_golden_source}"
export FLEX_TRADE_PG_DB="${FLEX_TRADE_PG_DB:-bifrost_dev}"

exec .venv/bin/python scripts/run_api.py
