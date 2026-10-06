"""TD-117 / TD-116 on real Postgres: the trades run reads Golden Source and reports what it wrote.

Before 0.11.0 the run read "Latest Flex date in DB" through ``bifrost_dev``'s FDW view in the
same transaction as the pre-import read, so it showed the previous run's date (jobs 172, 178,
183). Here the run's Golden Source is a throwaway database: the query rows, the range days and
the stats are read from it, IB is replaced by made-up rows, and core's writer commits them.

Marked ``db`` (``make test-db``). Refuses any server without ``bifrost.throwaway=on``.
Accounts, ids, symbols and prices are made up.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List
from unittest.mock import patch

import psycopg2
import pytest

from bifrost_flex_query.config import core_config
from bifrost_flex_query.orchestration import config_rw
from bifrost_flex_query.orchestration.trades import fetch_flex_trades_and_upsert_executions
from bifrost_flex_query.schema.ddl import ensure_flex_ops_schema

pytestmark = pytest.mark.db

ACCT = "U0000009"


@pytest.fixture
def gs(monkeypatch: pytest.MonkeyPatch) -> Dict[str, Any]:
    if not os.environ.get("PGHOST"):
        pytest.skip("make test-db (a throwaway postgres) runs these")
    conn = psycopg2.connect()
    with conn.cursor() as cur:
        cur.execute("SELECT current_setting('bifrost.throwaway', true)")
        if cur.fetchone()[0] != "on":
            conn.close()
            pytest.fail("not a throwaway database (bifrost.throwaway is not on)")
    from bifrost_core.persistence.postgres.brokerage_ddl import ensure_brokerage_schema

    ensure_brokerage_schema(conn, log=lambda m: None)
    conn.commit()
    ensure_flex_ops_schema(conn)
    with conn.cursor() as cur:
        cur.execute("TRUNCATE raw_broker.executions_raw_flex, raw_broker.commissions, raw_broker.settings_flex")
        cur.execute("DELETE FROM ops_jobs.flex_settings")
        cur.execute(
            "INSERT INTO raw_broker.settings_flex (sort_order, query_label, purpose, query_host_id) "
            "VALUES (0, 'Trades', 'trades', '1000001')"
        )
        cur.execute(
            "INSERT INTO ops_jobs.flex_settings (id, flex_default_range_days, flex_init_range_days) "
            "VALUES (1, 30, 270)"
        )
        # One fill from the previous run.
        cur.execute(
            "INSERT INTO raw_broker.executions_raw_flex (exec_id, account_id, symbol, sec_type, side, quantity, "
            "price, source, trade_date, exec_time) VALUES ('td117.prev', %s, 'ZQY', 'STK', 'BUY', 1, 10, "
            "'flex_trades', DATE '2026-09-30', TIMESTAMPTZ '2026-09-30 14:00+00')",
            (ACCT,),
        )
    conn.commit()
    for name in ("GOLDEN_SOURCE_HOST", "GOLDEN_SOURCE_PORT", "GOLDEN_SOURCE_DATABASE", "GOLDEN_SOURCE_USER",
                 "GOLDEN_SOURCE_PASSWORD", "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER",
                 "POSTGRES_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("FLEX_HOST_TOKEN", "tok-made-up-0009")
    monkeypatch.delenv("FLEX_SECONDARY_TOKEN", raising=False)
    config_rw._token_source_logged = False
    db = {
        "host": os.environ["PGHOST"],
        "port": int(os.environ.get("PGPORT") or 5432),
        "user": os.environ.get("PGUSER") or "bifrost",
        "password": "",
    }
    name = os.environ.get("PGDATABASE") or "bifrost"
    cfg = core_config({"postgres": {**db, "dbname": name}, "golden_source": {**db, "database": name}})
    yield {"conn": conn, "cfg": cfg}
    conn.close()


def _fill(trade_id: str, trade_date: str) -> Dict[str, Any]:
    return {
        "account_id": ACCT,
        "exec_id": f"td117.{trade_id}",
        "trade_id": trade_id,
        "source": "flex_trades",
        "time": 1790000000.0,
        "trade_date": trade_date,
        "symbol": "ZQY",
        "sec_type": "STK",
        "side": "BUY",
        "quantity": 1.0,
        "price": 10.0,
        "contract_key": "ZQY|STK|||",
        "commission": -1.0,
        "currency": "USD",
    }


def _run(cfg: Dict[str, Any], rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    with (
        patch("bifrost_flex_query.orchestration.trades.fetch_trades", return_value=rows) as fetch,
        patch("bifrost_flex_query.orchestration.trades.publish_flex_executions_system_message"),
    ):
        out = fetch_flex_trades_and_upsert_executions(cfg, {"fallback": False})
    out["_fetch_calls"] = fetch.call_args_list
    return out


def test_latest_date_after_is_what_the_run_wrote(gs) -> None:
    out = _run(gs["cfg"], [_fill("8001", "2026-10-01"), _fill("8002", "2026-10-02")])
    assert out["ok"] is True, out
    assert out["count"] == 2
    assert out["last_flex_date_after"] == "2026-10-02"
    # The window came from Golden Source: incremental from the stored 2026-09-30 plus 30 days.
    assert out["range_mode"] == "incremental"
    assert len(out["_fetch_calls"]) == 1


def test_query_rows_come_from_golden_source(gs) -> None:
    out = _run(gs["cfg"], [_fill("8003", "2026-10-01")])
    (call,) = out["_fetch_calls"]
    assert call.args[1] == "1000001"


def test_a_lost_grant_fails_the_run_instead_of_widening_it(gs) -> None:
    """TD-116: an unreadable stats table used to read as "no executions" -> the init window."""
    conn = gs["conn"]
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE raw_broker.executions_raw_flex RENAME TO executions_raw_flex_gone")
    conn.commit()
    try:
        out = _run(gs["cfg"], [_fill("8004", "2026-10-01")])
        assert out["ok"] is False
        assert out["_fetch_calls"] == []
    finally:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE raw_broker.executions_raw_flex_gone RENAME TO executions_raw_flex")
        conn.commit()
