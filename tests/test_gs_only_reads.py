"""TD-116 / TD-117 ratchets: the ingest reads Golden Source only, and a failed read fails.

Before 0.11.0 the query ids, the execution stats and (until seeded) the range days were read
from a Trade env database (``bifrost_dev``) through FDW views back to Golden Source, with
fallbacks that turned a failed read into "nothing there": a failed stats read switched the
trades run to init mode (the 270-day window, extra IB requests against the 1018 throttle).
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from bifrost_flex_query.orchestration import config_rw
from bifrost_flex_query.orchestration.trades import fetch_flex_trades_and_upsert_executions

ROOT = Path(__file__).resolve().parents[1]
TRADE_DB = re.compile(r"\bbifrost_(dev|stg|prod)\b")


def _config_files() -> list[Path]:
    return sorted(
        [*ROOT.glob("k8s/**/*.yaml"), *ROOT.glob("config/*.yaml"), *ROOT.glob("config/*.yaml.example")]
    )


def test_no_config_names_a_trade_database() -> None:
    files = _config_files()
    assert any(p.name == "configmap.yaml" for p in files)
    offenders = [
        f"{p.relative_to(ROOT)}: {line.strip()}"
        for p in files
        for line in p.read_text(encoding="utf-8").splitlines()
        if (TRADE_DB.search(line) or "trade_postgres" in line or "FLEX_TRADE_PG_" in line)
        and not line.lstrip().startswith("#")
    ]
    assert offenders == []


def test_code_opens_no_trade_database() -> None:
    names = ("trade_postgres", "FLEX_TRADE_PG_", "open_trade_conn", "trade_db_conn")
    hits = [
        f"{p.relative_to(ROOT)}: {n}"
        for p in sorted((ROOT / "src").rglob("*.py"))
        for n in names
        if n in p.read_text(encoding="utf-8")
    ]
    assert hits == []


class _BoomCursor:
    def execute(self, *_a: Any, **_k: Any) -> None:
        raise RuntimeError("permission denied for table executions_raw_flex")

    def __enter__(self) -> "_BoomCursor":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None


class _Boom:
    """A connection whose every statement fails (lost grant, dropped table, broken link)."""

    def __init__(self) -> None:
        self.rollbacks = 0

    def cursor(self, **_: Any) -> _BoomCursor:
        return _BoomCursor()

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        return None


def test_stats_read_failure_raises() -> None:
    conn = _Boom()
    with pytest.raises(RuntimeError):
        config_rw.get_flex_executions_stats(conn)
    assert conn.rollbacks == 1


def test_query_rows_read_failure_raises() -> None:
    conn = _Boom()
    with pytest.raises(RuntimeError):
        config_rw.get_flex_config(conn, purpose="trades")
    assert conn.rollbacks == 1


def test_stats_read_on_golden_source_by_trade_date() -> None:
    seen: list[str] = []

    class _Cur:
        def execute(self, sql: str, params: Any = None) -> None:
            seen.append(" ".join(sql.split()))

        def fetchone(self) -> Any:
            return {"count": 3, "accounts": 1, "min_date": date(2026, 9, 1), "max_date": date(2026, 10, 2)}

        def __enter__(self) -> Any:
            return self

        def __exit__(self, *exc: Any) -> None:
            return None

    conn = MagicMock()
    conn.cursor.return_value = _Cur()
    out = config_rw.get_flex_executions_stats(conn)
    assert out == {"count": 3, "accounts": 1, "min_date": date(2026, 9, 1), "max_date": date(2026, 10, 2)}
    assert "FROM raw_broker.executions_raw_flex" in seen[0]
    assert "MAX(trade_date)" in seen[0]
    conn.rollback.assert_called_once()


def test_trades_run_with_unreadable_stats_fails_without_asking_ib() -> None:
    """The old fallback answered count 0 here and the run asked IB for the init window."""
    with (
        patch("bifrost_flex_query.orchestration.trades.open_golden_conn", return_value=_Boom()),
        patch(
            "bifrost_flex_query.orchestration.trades.get_flex_config",
            return_value=[{"token": "t", "query_id": "q1", "role": "host", "query_label": "Trades"}],
        ),
        patch("bifrost_flex_query.orchestration.trades.fetch_trades") as fetch,
        patch("bifrost_flex_query.orchestration.trades.publish_flex_executions_system_message"),
    ):
        out = fetch_flex_trades_and_upsert_executions({"sink": "postgres", "postgres": {"host": "x"}}, {})
    assert out["ok"] is False
    assert "permission denied" in out["error"]
    fetch.assert_not_called()


def test_latest_date_is_read_after_the_write() -> None:
    """TD-117: the stats after the import come from a new read, after the writer committed."""
    order: list[str] = []

    def write(cfg: Any, rows: Any) -> bool:
        order.append("write")
        return True

    def after(cfg: Any) -> dict:
        order.append("stats_after")
        return {"count": 4, "max_date": date(2026, 10, 2)}

    rows = [{"account_id": "U0000009", "trade_date": "2026-10-02", "source": "flex_trades"}]
    with (
        patch("bifrost_flex_query.orchestration.trades.open_golden_conn", return_value=MagicMock()),
        patch(
            "bifrost_flex_query.orchestration.trades.get_flex_config",
            return_value=[{"token": "t", "query_id": "q1", "role": "host", "query_label": "Trades"}],
        ),
        patch(
            "bifrost_flex_query.orchestration.trades.get_flex_executions_stats",
            return_value={"count": 3, "max_date": date(2026, 9, 30)},
        ),
        patch("bifrost_flex_query.orchestration.trades.get_flex_range_days", return_value=(30, 270)),
        patch("bifrost_flex_query.orchestration.trades.fetch_trades", return_value=rows),
        patch("bifrost_flex_query.orchestration.trades.write_account_executions_to_db", side_effect=write),
        patch("bifrost_flex_query.orchestration.trades.read_flex_executions_stats", side_effect=after),
        patch("bifrost_flex_query.orchestration.trades.publish_flex_executions_system_message"),
    ):
        out = fetch_flex_trades_and_upsert_executions({"sink": "postgres", "postgres": {"host": "x"}}, {})
    assert out["ok"] is True
    assert order == ["write", "stats_after"]
    assert out["last_flex_date_after"] == "2026-10-02"
