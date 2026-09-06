"""A manual run leaves the same trail as a queued one."""

from __future__ import annotations

from typing import Any

from bifrost_flex_query.api.manual_ops import _audit_finish
from bifrost_flex_query.scheduler.enqueue import insert_manual_job


class _Cur:
    def __init__(self, parent: "_Conn") -> None:
        self.parent = parent

    def execute(self, query: str, params: Any = None) -> None:
        self.parent.statements.append((query, params))

    def fetchone(self) -> Any:
        return {"id": 77}

    def __enter__(self) -> "_Cur":
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _Conn:
    def __init__(self) -> None:
        self.statements: list[tuple[str, Any]] = []
        self.commits = 0

    def cursor(self) -> _Cur:
        return _Cur(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        return None


def test_insert_manual_job_is_running_and_never_dedupes() -> None:
    conn = _Conn()
    assert insert_manual_job(conn, kind="flex-trades", payload={"source": "trigger"}) == 77
    sql, params = conn.statements[0]
    assert "'running'" in sql and "payload_hash" in sql and "NULL" in sql
    assert params[0] == "flex-trades" and '"manual": true' in params[1]


def test_audit_finish_done_then_freshness() -> None:
    conn = _Conn()
    _audit_finish(conn, "flex-trades", 77, {"ok": True, "count": 12, "message": "Upserted 12", "errors": []})
    sqls = [s for s, _ in conn.statements]
    assert any("status = 'done'" in s for s in sqls)
    assert any("flex_ingest_freshness" in s and "last_ok = true" in s for s in sqls)


def test_audit_finish_partial_is_a_failure_with_a_category() -> None:
    conn = _Conn()
    _audit_finish(
        conn,
        "flex-transactions",
        78,
        {"ok": True, "count": 11, "errors": ["Flex request failed: [1018] Too many requests"]},
    )
    failed = [p for s, p in conn.statements if "status = 'failed'" in s]
    assert failed and failed[0][0] == "throttled"
    fresh = [(s, p) for s, p in conn.statements if "flex_ingest_freshness" in s]
    assert fresh and "last_ok = false" in fresh[0][0]
    assert "[1018]" in fresh[0][1][1]
