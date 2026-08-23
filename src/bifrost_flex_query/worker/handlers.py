"""Thin handlers — dispatch to in-package Flex orchestration."""

from __future__ import annotations

from typing import Any, Mapping

from bifrost_flex_query.config import trade_config_for_core
from bifrost_flex_query.schema.ddl import update_freshness


def _require_ok(result: Mapping[str, Any] | None, *, label: str) -> dict[str, Any]:
    data = dict(result or {})
    if data.get("ok") is False:
        raise RuntimeError(str(data.get("error") or f"{label} failed"))
    inserted = int(data.get("count") or data.get("inserted") or 0)
    return {"inserted": inserted, "ok": True, "result": data}


def handle_flex_trades(payload: Mapping[str, Any], config: Mapping[str, Any]) -> dict[str, Any]:
    from bifrost_flex_query.orchestration.trades import fetch_flex_trades_and_upsert_executions

    core_cfg = trade_config_for_core(dict(config))
    result = fetch_flex_trades_and_upsert_executions(core_cfg, dict(payload))
    return _require_ok(result, label="flex-trades")


def handle_flex_transactions(payload: Mapping[str, Any], config: Mapping[str, Any]) -> dict[str, Any]:
    from bifrost_flex_query.orchestration.transactions import fetch_cash_transactions_from_flex

    core_cfg = trade_config_for_core(dict(config))
    result = fetch_cash_transactions_from_flex(core_cfg, dict(payload))
    return _require_ok(result, label="flex-transactions")


HANDLERS = {
    "flex-trades": handle_flex_trades,
    "flex-transactions": handle_flex_transactions,
}


def dispatch(kind: str, payload: Mapping[str, Any], config: Mapping[str, Any], conn: Any | None = None) -> dict[str, Any]:
    _ = conn  # claim connection is not thread-safe; freshness uses its own connection
    fn = HANDLERS.get(str(kind).strip())
    if fn is None:
        raise ValueError(f"unknown job kind: {kind!r}")
    out = fn(payload, config)
    _record_ingest_freshness(str(kind).strip(), int(out.get("inserted") or 0), config)
    return out


def _record_ingest_freshness(dimension: str, row_count: int, config: Mapping[str, Any]) -> None:
    """Write flex_ops.ingest_freshness on a dedicated connection (worker runs dispatch in a thread pool)."""
    import psycopg2

    from bifrost_flex_query.config import postgres_connect_kwargs

    fresh_conn = psycopg2.connect(**{**postgres_connect_kwargs(dict(config)), "connect_timeout": 10})
    try:
        update_freshness(fresh_conn, dimension, row_count)
    finally:
        fresh_conn.close()
