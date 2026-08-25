"""Fetch IB Flex cash transactions and upsert into raw_broker.transactions."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from bifrost_core.monitor.reader import upsert_account_transactions

from bifrost_flex_query.client.flex_client import fetch_cash_transactions
from bifrost_flex_query.orchestration.config_rw import (
    get_flex_config,
    get_flex_default_range_dates,
    open_trade_conn,
    postgres_ready,
)

logger = logging.getLogger(__name__)


def fetch_cash_transactions_from_flex(
    config: dict,
    body: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Fetch Flex cash transactions and upsert (no HTTP)."""
    conn = None
    try:
        if not postgres_ready(config):
            return {
                "ok": False,
                "error": "Postgres config required to write account_transactions.",
                "count": 0,
            }
        conn = open_trade_conn(config)
        entries: List[tuple] = []
        flex_list = get_flex_config(conn, purpose="cash_transactions")
        for a in flex_list:
            tok = (a.get("token") or "").strip()
            qid = (a.get("query_id") or "").strip()
            if tok and qid:
                entries.append((tok, qid))
        if not entries:
            return {
                "ok": False,
                "error": (
                    "No Flex credentials for cash_transactions: set FLEX_HOST_TOKEN / "
                    "FLEX_SECONDARY_TOKEN (K8s Secret bifrost-flex-tokens) and "
                    "purpose=cash_transactions query_id rows "
                    "(Settings → IB Connection → Flex Query IDs, or Ops Console Flex Plugin)."
                ),
                "count": 0,
            }
        payload = body or {}
        from_date = (payload.get("from_date") or "").strip() or None
        to_date = (payload.get("to_date") or "").strip() or None
        if from_date is None and to_date is None:
            from_date, to_date = get_flex_default_range_dates(conn)
        all_rows: List[Dict[str, Any]] = []
        errors: List[str] = []
        for token, query_id in entries:
            try:
                rows = fetch_cash_transactions(token, query_id, from_date=from_date, to_date=to_date)
                all_rows.extend(rows)
            except ValueError as e:
                if (
                    from_date
                    and to_date
                    and ("[1003]" in str(e) or "Statement is not available" in str(e))
                ):
                    logger.warning(
                        "Flex cash date-range rejected for query_id=%s (%s); trying query default",
                        query_id,
                        e,
                    )
                    try:
                        rows = fetch_cash_transactions(token, query_id)
                        all_rows.extend(rows)
                        continue
                    except ValueError as e2:
                        errors.append(f"{e}; fallback query-default failed: {e2}")
                        continue
                errors.append(str(e))
        if errors and not all_rows:
            return {"ok": False, "error": "; ".join(errors), "count": 0}
        if not all_rows:
            return {
                "ok": True,
                "count": 0,
                "message": "No cash transactions in report.",
                "by_account": len(entries),
            }
        n = upsert_account_transactions(config, all_rows)
        msg = f"Upserted {n} transaction(s) from {len(entries)} Flex account(s)."
        if errors:
            msg += " Partial errors: " + "; ".join(errors)
        return {"ok": True, "count": n, "message": msg, "by_account": len(entries)}
    except Exception as e:
        logger.exception("fetch_cash_transactions_from_flex failed: %s", e)
        return {"ok": False, "error": str(e), "count": 0}
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
