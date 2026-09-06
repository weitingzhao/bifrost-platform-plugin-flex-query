"""record_freshness: success moves latest_ts, failure only records the attempt."""

from __future__ import annotations

from typing import Any

from bifrost_flex_query.schema.ddl import record_freshness, update_freshness


class _Cur:
    def __init__(self, parent: "_Conn") -> None:
        self.parent = parent

    def execute(self, query: str, params: Any = None) -> None:
        self.parent.statements.append((query, params))

    def __enter__(self) -> "_Cur":
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _Conn:
    def __init__(self) -> None:
        self.statements: list[tuple[str, Any]] = []
        self.committed = False

    def cursor(self) -> _Cur:
        return _Cur(self)

    def commit(self) -> None:
        self.committed = True


def test_success_updates_latest_ts_and_counts() -> None:
    conn = _Conn()
    record_freshness(conn, "flex-trades", ok=True, processed_rows=50, new_rows=2, job_id=47)
    sql, params = conn.statements[0]
    assert "latest_ts = EXCLUDED.latest_ts" in sql and "last_ok = true" in sql
    assert params == ("flex-trades", 50, 50, 2, 47)
    assert conn.committed


def test_failure_leaves_latest_ts_alone() -> None:
    conn = _Conn()
    record_freshness(conn, "flex-trades", ok=False, error="not_ready: [1003]", job_id=48)
    sql, params = conn.statements[0]
    assert "latest_ts = EXCLUDED" not in sql and "last_ok = false" in sql
    assert params == ("flex-trades", "not_ready: [1003]", 48)


def test_legacy_wrapper_is_a_success() -> None:
    conn = _Conn()
    update_freshness(conn, "flex-transactions", 11)
    sql, params = conn.statements[0]
    assert "last_ok = true" in sql and params[1] == 11
