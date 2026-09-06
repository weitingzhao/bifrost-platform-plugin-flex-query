"""dispatch records the outcome on its own connection (the claim one is not thread-safe)."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

from bifrost_flex_query.worker.handlers import dispatch, record_ingest_outcome


class _FakeConn:
    def __init__(self) -> None:
        self.closed = False

    def cursor(self) -> _FakeConn:
        return self

    def execute(self, *args: Any, **kwargs: Any) -> None:
        _ = args, kwargs

    def fetchone(self) -> Any:
        return None

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True

    def __enter__(self) -> _FakeConn:
        return self

    def __exit__(self, *args: object) -> None:
        return None


def test_dispatch_records_success_on_separate_connection() -> None:
    fake = _FakeConn()

    with (
        patch("psycopg2.connect", return_value=fake),
        patch(
            "bifrost_flex_query.worker.handlers.HANDLERS",
            {"flex-trades": lambda _p, _c: {"inserted": 7, "ok": True, "result": {}}},
        ),
        patch("bifrost_flex_query.worker.handlers.record_freshness") as rec,
    ):
        out = dispatch("flex-trades", {}, {}, conn=object(), job_id=41)

    assert out["inserted"] == 7
    rec.assert_called_once_with(fake, "flex-trades", ok=True, processed_rows=7, new_rows=None, job_id=41)
    assert fake.closed


def test_partial_account_failure_fails_the_job() -> None:
    """Host landed, secondary answered 1003: not done — retry picks the secondary up."""
    from bifrost_flex_query.worker.handlers import _require_ok

    try:
        _require_ok(
            {"ok": True, "count": 46, "errors": ["Flex query 2/2 (secondary 1428413): [1003] Statement is not available."]},
            label="flex-trades",
        )
    except RuntimeError as exc:
        assert "[1003]" in str(exc) and "partial" in str(exc)
    else:
        raise AssertionError("partial errors must raise")
    assert _require_ok({"ok": True, "count": 3, "errors": []}, label="x")["inserted"] == 3


def test_record_ingest_outcome_failure_keeps_latest_ts() -> None:
    fake = _FakeConn()
    with (
        patch("psycopg2.connect", return_value=fake),
        patch("bifrost_flex_query.worker.handlers.record_freshness") as rec,
    ):
        record_ingest_outcome("flex-trades", {}, ok=False, error="not_ready: [1003]", job_id=42)
    rec.assert_called_once_with(fake, "flex-trades", ok=False, error="not_ready: [1003]", job_id=42)
    assert fake.closed
