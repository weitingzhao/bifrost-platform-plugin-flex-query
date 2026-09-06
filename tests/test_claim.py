from __future__ import annotations

from typing import Any

from bifrost_flex_query.worker.claim import (
    claim_next,
    defer_pending,
    mark_done,
    mark_failed,
    mark_retry,
    reclaim_stale_running,
)


class _FakeCursor:
    def __init__(self, fetch_results: list[Any]) -> None:
        self.fetch_results = list(fetch_results)
        self.statements: list[tuple[str, Any]] = []
        self._returning: list[Any] = []

    def execute(self, query: str, params: Any = None) -> None:
        self.statements.append((query, params))
        if "RETURNING id" in query and params is not None:
            # Simulate reclaim returning one stale id when tests set fetch_results.
            self._returning = list(self.fetch_results)

    def fetchone(self) -> Any:
        if not self.fetch_results:
            return None
        return self.fetch_results.pop(0)

    def fetchall(self) -> list[Any]:
        out = list(self._returning)
        self._returning = []
        return out

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _FakeConn:
    def __init__(self, fetch_results: list[Any] | None = None) -> None:
        self.cur = _FakeCursor(fetch_results or [])
        self.committed = False
        self.rolled_back = False

    def cursor(self) -> _FakeCursor:
        return self.cur

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True


def test_claim_next_empty_ends_the_transaction() -> None:
    conn = _FakeConn([])
    assert claim_next(conn) is None
    sql = conn.cur.statements[0][0]
    assert "FOR UPDATE SKIP LOCKED" in sql
    assert "not_before IS NULL OR not_before <= clock_timestamp()" in sql
    # An idle poll must not leave a transaction open: now() would freeze and vacuum would wait.
    assert conn.rolled_back and not conn.committed


def test_claim_next_marks_running() -> None:
    conn = _FakeConn([(7, "flex-trades", {"as_of": "2026-08-17"}, 0, 3)])
    job = claim_next(conn)
    assert job is not None
    assert job["id"] == 7
    assert job["kind"] == "flex-trades"
    assert job["attempts"] == 1
    assert conn.committed
    running = [s[0] for s in conn.cur.statements if "status = 'running'" in s[0]]
    assert running and "started_at = clock_timestamp()" in running[0]


def test_mark_done() -> None:
    conn = _FakeConn()
    mark_done(conn, 7, {"inserted": 3})
    assert conn.committed
    assert "status = 'done'" in conn.cur.statements[0][0]


def test_mark_failed_is_final() -> None:
    conn = _FakeConn()
    mark_failed(conn, 7, error="[1012] Token has expired.", category="config")
    sql, params = conn.cur.statements[0]
    assert "status = 'failed'" in sql
    assert params[0] == "config" and params[-1] == 7
    assert conn.committed


def test_mark_retry_defers_the_job() -> None:
    conn = _FakeConn()
    mark_retry(conn, 7, error="[1003] Statement is not available.", category="not_ready", delay_sec=1800)
    sql, params = conn.cur.statements[0]
    assert "status = 'pending'" in sql
    assert "not_before = clock_timestamp() + make_interval" in sql
    assert params[0] == 1800 and params[1] == "not_ready"
    assert conn.committed


def test_defer_pending_pushes_the_rest_of_the_queue() -> None:
    conn = _FakeConn([(8,), (9,)])
    ids = defer_pending(conn, delay_sec=1800, exclude_id=7)
    assert ids == [8, 9]
    sql, params = conn.cur.statements[0]
    assert "GREATEST" in sql and "status = 'pending'" in sql
    assert params == (1800, 7, 7)


def test_reclaim_stale_running_requeues_while_attempts_remain() -> None:
    conn = _FakeConn([(23,)])
    ids = reclaim_stale_running(conn, stale_after_sec=3600, requeue_delay_sec=600)
    assert ids == [23]
    assert conn.committed
    sql, params = conn.cur.statements[0]
    assert "status = 'running'" in sql
    assert "CASE WHEN attempts < max_attempts THEN 'pending' ELSE 'failed' END" in sql
    assert params == (600, params[1], 3600)
    assert "stale running reclaimed" in params[1]
