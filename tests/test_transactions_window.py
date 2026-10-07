"""TD-88: the cash-transactions window catches up from the last stored row, in IB-sized chunks.

Account ids and amounts here are made up.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from bifrost_flex_query.api.metrics import render_metrics
from bifrost_flex_query.client.flex_client import MAX_FLEX_DAYS
from bifrost_flex_query.ops.coverage import coverage_check, gap_months_by_account, read_coverage
from bifrost_flex_query.ops.diagnose import CheckInput, KindInput, diagnose
from bifrost_flex_query.orchestration import transactions as tx
from bifrost_flex_query.orchestration.transactions import (
    CHUNK_MAX_SPAN_DAYS,
    split_window,
    transactions_window,
)

TODAY = date(2026, 10, 6)
YESTERDAY = TODAY - timedelta(days=1)


def test_stored_max_60_days_ago_reaches_back_at_least_60_days() -> None:
    """The ratchet: an outage longer than default_days is caught up, not skipped."""
    last = {"U0000001": TODAY - timedelta(days=60), "U0000002": YESTERDAY}
    start, end, mode, days = transactions_window(last, default_days=30, init_days=270, today=TODAY)
    assert end == YESTERDAY
    assert (TODAY - start).days >= 60
    assert start <= last["U0000001"]
    assert mode == "incremental" and days == (end - start).days


def test_recent_rows_keep_the_default_window() -> None:
    last = {"U0000001": YESTERDAY, "U0000002": YESTERDAY - timedelta(days=2)}
    start, end, mode, _ = transactions_window(last, default_days=30, init_days=270, today=TODAY)
    assert (start, end, mode) == (YESTERDAY - timedelta(days=30), YESTERDAY, "incremental")


def test_nothing_stored_uses_the_init_window() -> None:
    start, end, mode, days = transactions_window({}, default_days=30, init_days=270, today=TODAY)
    assert (end - start).days == 270 and mode == "init" and days == 270


def test_the_oldest_account_sets_the_window() -> None:
    last = {"U0000001": date(2026, 3, 5), "U0000002": date(2026, 3, 4)}
    start, _, _, _ = transactions_window(last, default_days=30, init_days=270, today=TODAY)
    assert start == date(2026, 3, 4)


def test_split_window_respects_the_ib_span() -> None:
    start, end = date(2024, 1, 1), date(2026, 10, 5)
    chunks = split_window(start, end)
    assert chunks[0][0] == start and chunks[-1][1] == end
    for a, b in chunks:
        assert 0 <= (b - a).days <= CHUNK_MAX_SPAN_DAYS < MAX_FLEX_DAYS
    for (_, b), (a2, _) in zip(chunks, chunks[1:]):
        assert a2 == b + timedelta(days=1)


def test_split_window_short_window_is_one_chunk() -> None:
    assert split_window(date(2026, 3, 1), date(2026, 8, 5)) == [(date(2026, 3, 1), date(2026, 8, 5))]


def _run(body: Optional[Dict[str, Any]], last: Dict[str, date]) -> tuple[dict, List[tuple]]:
    calls: List[tuple] = []

    def fake_fetch(token: str, query_id: str, from_date=None, to_date=None) -> List[Dict[str, Any]]:
        calls.append((query_id, from_date, to_date))
        return [{"account_id": "U0000001", "ts": 1.0, "amount": -1.0, "type": "other", "report_date": "2026-03-02"}]

    flex_list = [{"token": "t1", "query_id": "q1"}, {"token": "t2", "query_id": "q2"}]
    with (
        patch.object(tx, "postgres_ready", return_value=True),
        patch.object(tx, "get_flex_config", return_value=flex_list),
        patch.object(tx, "resolve_flex_range_days", return_value=(30, 270)),
        patch.object(tx, "open_golden_conn", return_value=MagicMock()),
        patch.object(tx, "read_last_transaction_dates", return_value=last),
        patch.object(tx, "fetch_cash_transactions", side_effect=fake_fetch),
        patch.object(tx, "upsert_account_transactions", side_effect=lambda cfg, rows: (len(rows), 0)),
    ):
        out = tx.fetch_cash_transactions_from_flex({"sink": "postgres"}, body)
    return out, calls


def test_scheduled_run_requests_from_the_last_stored_day() -> None:
    last_day = date.today() - timedelta(days=120)
    out, calls = _run({"fallback": False}, {"U0000001": last_day})
    want_from = last_day.strftime("%Y%m%d")
    assert {c[1] for c in calls} == {want_from}
    assert out["ok"] and out["range_mode"] == "incremental" and out["range_from"] == want_from
    assert out["range_chunks"] == 1


def test_scheduled_run_after_a_long_outage_is_chunked() -> None:
    last_day = date.today() - timedelta(days=500)
    out, calls = _run({"fallback": False}, {"U0000001": last_day})
    assert out["range_chunks"] == 2 and len(calls) == 4  # 2 chunks x 2 queries
    for _, f, t in calls:
        d = datetime.strptime(t, "%Y%m%d") - datetime.strptime(f, "%Y%m%d")
        assert d.days <= CHUNK_MAX_SPAN_DAYS


def test_explicit_window_is_kept() -> None:
    out, calls = _run({"from_date": "20260301", "to_date": "20260805", "fallback": False}, {"U0000001": date(2026, 10, 1)})
    assert calls == [("q1", "20260301", "20260805"), ("q2", "20260301", "20260805")]
    assert out["range_mode"] == "manual" and out["range_from"] == "20260301" and out["range_to"] == "20260805"


def test_explicit_window_longer_than_ib_allows_is_chunked() -> None:
    out, calls = _run({"from_date": "20250101", "to_date": "20260805", "fallback": False}, {})
    assert out["range_chunks"] == 2 and len(calls) == 4
    assert calls[0][1] == "20250101" and calls[1][2] == "20260805"


# --- coverage ratchet -----------------------------------------------------


class _Cur:
    def __init__(self, rows):
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.sql = sql

    def fetchall(self):
        return self._rows


class _Conn:
    def __init__(self, rows):
        self.rows = rows

    def cursor(self, *a, **k):
        return _Cur(self.rows)

    def rollback(self):
        pass


def _td88_rows():
    rows = []
    for m in (2, 3, 4, 5, 6, 7, 8):
        rows.append({"account_id": "U0000001", "month": date(2026, m, 1), "executions": 10, "gap": m in (4, 5, 6, 7)})
    rows.append({"account_id": "U0000002", "month": date(2026, 4, 1), "executions": 3, "gap": True})
    rows.append({"account_id": "U0000002", "month": date(2026, 8, 1), "executions": 3, "gap": False})
    return rows


def test_read_coverage_lists_gap_months() -> None:
    cov = read_coverage(_Conn(_td88_rows()))
    assert cov["accounts"] == ["U0000001", "U0000002"]
    assert [g["month"] for g in cov["gaps"] if g["account_id"] == "U0000001"] == ["2026-04", "2026-05", "2026-06", "2026-07"]
    assert gap_months_by_account(cov) == {"U0000001": 4, "U0000002": 1}


def test_coverage_gap_turns_the_ops_check_red() -> None:
    """The TD-88 hole would have shown: verdict 'attention' instead of 'ok'."""
    now = datetime(2026, 9, 8, 11, 5, tzinfo=timezone.utc)
    job = {"id": 1, "status": "done", "created_at": now - timedelta(minutes=30), "finished_at": now - timedelta(minutes=29)}
    base = dict(
        now=now,
        tz="America/New_York",
        grace_sec=2700,
        kinds=[KindInput("flex-transactions", job, {"latest_ts": now}, now - timedelta(minutes=35), now + timedelta(days=1), 8)],
        heartbeat={"seen_at": now - timedelta(seconds=10)},
        tokens={"host": True, "secondary": True, "issued_at": "2026-08-24", "age_days": 15},
        data_latest={"executions_raw_flex": now - timedelta(hours=20), "transactions": now - timedelta(hours=20)},
    )
    clean = diagnose(CheckInput(**base, coverage={"accounts": ["U0000001"], "gaps": []}))
    assert clean["verdict"] == "ok"
    assert next(c for c in clean["checks"] if c["id"] == "coverage")["ok"] is True

    holed = diagnose(CheckInput(**base, coverage=read_coverage(_Conn(_td88_rows()))))
    assert holed["verdict"] == "attention"
    check = next(c for c in holed["checks"] if c["id"] == "coverage")
    assert check["ok"] is False and "2026-04" in check["detail"] and len(check["gaps"]) == 5

    unread = diagnose(CheckInput(**base, coverage_error="permission denied"))
    assert unread["verdict"] == "attention"

    not_collected = diagnose(CheckInput(**base))
    assert not any(c["id"] == "coverage" for c in not_collected["checks"])


def test_coverage_check_error_is_not_ok() -> None:
    assert coverage_check(None, "boom")["ok"] is False


def test_parsed_rows_with_nothing_written_fails_the_job() -> None:
    """TD-91: rows > 0 and written == 0 is ok:false, and the handler raises."""
    from bifrost_flex_query.worker.handlers import _require_ok

    def fake_fetch(token: str, query_id: str, from_date=None, to_date=None) -> List[Dict[str, Any]]:
        return [{"account_id": "U0000001", "ts": 1.0, "amount": -1.0, "type": "other", "report_date": "20260302"}]

    with (
        patch.object(tx, "postgres_ready", return_value=True),
        patch.object(tx, "get_flex_config", return_value=[{"token": "t1", "query_id": "q1"}]),
        patch.object(tx, "open_golden_conn", return_value=MagicMock()),
        patch.object(tx, "fetch_cash_transactions", side_effect=fake_fetch),
        patch.object(tx, "upsert_account_transactions", return_value=(0, 1)),
    ):
        out = tx.fetch_cash_transactions_from_flex(
            {"sink": "postgres"},
            {"from_date": "20260301", "to_date": "20260302", "fallback": False},
        )
    assert out["ok"] is False
    assert out["count"] == 0
    assert out["skipped"] == 1
    with pytest.raises(RuntimeError, match="wrote 0"):
        _require_ok(out, label="flex-transactions")


def test_coverage_metric_per_account() -> None:
    snap = {"version": "x", "coverage": read_coverage(_Conn(_td88_rows()))}
    lines = set(render_metrics(snap).splitlines())
    assert 'bifrost_flex_coverage_gap_months{account="U0000001"} 4' in lines
    assert 'bifrost_flex_coverage_gap_months{account="U0000002"} 1' in lines
    covered = {"version": "x", "coverage": {"accounts": ["U0000001"], "gaps": []}}
    assert 'bifrost_flex_coverage_gap_months{account="U0000001"} 0' in render_metrics(covered).splitlines()
    assert "bifrost_flex_coverage_gap_months" not in render_metrics({"version": "x"})
