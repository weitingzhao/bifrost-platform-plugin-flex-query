"""DDL unit tests."""

from __future__ import annotations

from typing import Any

from bifrost_flex_query.schema.ddl import ensure_flex_ops_schema, update_freshness


class _FakeCursor:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.fetch_result: list[tuple[int]] = [(0,)]

    def execute(self, query: str, params: Any = None) -> None:
        _ = params
        self.statements.append(query)

    def fetchone(self) -> tuple[int] | None:
        if not self.fetch_result:
            return None
        return self.fetch_result[0]

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _FakeConn:
    def __init__(self) -> None:
        self.cur = _FakeCursor()
        self.committed = False

    def cursor(self) -> _FakeCursor:
        return self.cur

    def commit(self) -> None:
        self.committed = True


def test_ensure_flex_ops_schema_skips_when_tables_present() -> None:
    conn = _FakeConn()
    conn.cur.fetch_result = [(2,)]
    ensure_flex_ops_schema(conn)
    assert conn.committed
    blob = "\n".join(conn.cur.statements)
    assert "information_schema.tables" in blob
    assert "CREATE TABLE" not in blob
    assert "flex_ops" not in blob


def test_ensure_flex_ops_schema() -> None:
    conn = _FakeConn()
    conn.cur.fetch_result = [(0,)]
    ensure_flex_ops_schema(conn)
    assert conn.committed
    blob = "\n".join(conn.cur.statements)
    assert "CREATE SCHEMA IF NOT EXISTS ops_jobs" in blob
    assert "flex_ops" not in blob
    assert "ops_jobs.job_flex_ingest" in blob
    assert "ops_jobs.flex_ingest_freshness" in blob
    assert "job_flex_ingest_kind_hash_active" in blob
    assert "job_flex_ingest_claim" in blob


def test_update_freshness() -> None:
    conn = _FakeConn()
    update_freshness(conn, "flex-trades", 12)
    assert conn.committed
    blob = "\n".join(conn.cur.statements)
    assert "ops_jobs.flex_ingest_freshness" in blob
    assert "ON CONFLICT (dimension)" in blob
