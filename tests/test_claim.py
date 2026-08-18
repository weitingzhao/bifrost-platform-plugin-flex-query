"""Unit tests for SKIP LOCKED claim helpers."""

from __future__ import annotations

from typing import Any

from bifrost_flex_query.worker.claim import claim_next, mark_done, mark_failed


class _FakeCursor:
    def __init__(self, fetch_results: list[Any]) -> None:
        self.fetch_results = list(fetch_results)
        self.statements: list[tuple[str, Any]] = []

    def execute(self, query: str, params: Any = None) -> None:
        self.statements.append((query, params))

    def fetchone(self) -> Any:
        if not self.fetch_results:
            return None
        return self.fetch_results.pop(0)

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _FakeConn:
    def __init__(self, fetch_results: list[Any] | None = None) -> None:
        self.cur = _FakeCursor(fetch_results or [])
        self.committed = False

    def cursor(self) -> _FakeCursor:
        return self.cur

    def commit(self) -> None:
        self.committed = True


def test_claim_next_empty() -> None:
    conn = _FakeConn([])
    assert claim_next(conn) is None
    assert "FOR UPDATE SKIP LOCKED" in conn.cur.statements[0][0]


def test_claim_next_marks_running() -> None:
    conn = _FakeConn([(7, "flex-trades", {"as_of": "2026-08-17"}, 0, 3)])
    job = claim_next(conn)
    assert job is not None
    assert job["id"] == 7
    assert job["kind"] == "flex-trades"
    assert job["attempts"] == 1
    assert conn.committed
    assert any("status = 'running'" in s[0] for s in conn.cur.statements)


def test_mark_done() -> None:
    conn = _FakeConn()
    mark_done(conn, 7, {"inserted": 3})
    assert conn.committed
    assert "status = 'done'" in conn.cur.statements[0][0]


def test_mark_failed_retries_then_fails() -> None:
    conn = _FakeConn()
    mark_failed(conn, 7, error="boom", attempts=1, max_attempts=3)
    assert "'pending'" in conn.cur.statements[0][0] or conn.cur.statements[0][1][0] == "pending"
    conn2 = _FakeConn()
    mark_failed(conn2, 7, error="boom", attempts=3, max_attempts=3)
    assert conn2.cur.statements[0][1][0] == "failed"
