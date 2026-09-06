"""enqueue_slot unit tests (no postgres)."""

from __future__ import annotations

from typing import Any

from bifrost_flex_query.scheduler.daily import enqueue_slot


class _Cursor:
    def __init__(self, parent: _Conn) -> None:
        self.parent = parent

    def execute(self, query: str, params: Any = None) -> None:
        self.parent.statements.append((query, params))
        self.parent._fetchone = (1,)

    def fetchone(self) -> Any:
        return self.parent._fetchone

    def __enter__(self) -> _Cursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _Conn:
    def __init__(self) -> None:
        self.statements: list[tuple[str, Any]] = []
        self._fetchone: Any = None

    def cursor(self) -> _Cursor:
        return _Cursor(self)

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None


def test_enqueue_slot_flex_trades() -> None:
    conn = _Conn()
    out = enqueue_slot(
        conn,
        "flex-trades",
        scheduler_cfg={"slots": {"flex-trades": {"priority": 5}}},
        payload={"as_of": "2026-08-17"},
    )
    assert out["kind"] == "flex-trades"
    assert out["enqueued"] == 1
    assert out["job_id"] == 1
    # A queued run waits for IB instead of widening the query, and has the morning's attempt budget.
    assert out["payload"] == {"as_of": "2026-08-17", "fallback": False}
    assert out["max_attempts"] == 8
    _sql, params = conn.statements[0]
    assert params[4] == 8


def test_fallback_is_not_identity() -> None:
    """A Console enqueue (no knobs) dedupes against the scheduled one for the same day."""
    from bifrost_flex_query.scheduler.enqueue import payload_hash

    conn = _Conn()
    enqueue_slot(conn, "flex-trades", payload={"as_of": "2026-08-17"})
    enqueue_slot(conn, "flex-trades", payload={"as_of": "2026-08-17", "fallback": True})
    hashes = [params[2] for _sql, params in conn.statements]
    assert hashes[0] == hashes[1] == payload_hash({"as_of": "2026-08-17"})


def test_slot_attempt_budget_from_schedule() -> None:
    conn = _Conn()
    out = enqueue_slot(conn, "flex-transactions", scheduler_cfg={"slots": {"flex-transactions": {"max_attempts": 3}}})
    assert out["max_attempts"] == 3
