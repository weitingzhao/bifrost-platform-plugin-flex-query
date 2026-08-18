"""Thin handlers — dispatch to bifrost-core Flex orchestration."""

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
    from bifrost_core.monitor.reader import StatusReader
    from bifrost_core.portfolio.services.executions_fetch_flex import (
        fetch_flex_trades_and_upsert_executions,
    )

    core_cfg = trade_config_for_core(dict(config))
    reader = StatusReader(core_cfg)
    result = fetch_flex_trades_and_upsert_executions(reader, core_cfg, dict(payload))
    return _require_ok(result, label="flex-trades")


def handle_flex_transactions(payload: Mapping[str, Any], config: Mapping[str, Any]) -> dict[str, Any]:
    from bifrost_core.monitor.reader import StatusReader
    from bifrost_core.portfolio.services.transactions_fetch import fetch_cash_transactions_from_flex

    core_cfg = trade_config_for_core(dict(config))
    reader = StatusReader(core_cfg)
    result = fetch_cash_transactions_from_flex(reader, core_cfg, dict(payload))
    return _require_ok(result, label="flex-transactions")


HANDLERS = {
    "flex-trades": handle_flex_trades,
    "flex-transactions": handle_flex_transactions,
}


def dispatch(kind: str, payload: Mapping[str, Any], config: Mapping[str, Any], conn: Any | None = None) -> dict[str, Any]:
    fn = HANDLERS.get(str(kind).strip())
    if fn is None:
        raise ValueError(f"unknown job kind: {kind!r}")
    out = fn(payload, config)
    if conn is not None:
        update_freshness(conn, str(kind).strip(), int(out.get("inserted") or 0))
    return out
