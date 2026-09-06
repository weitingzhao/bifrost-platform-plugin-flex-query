"""run-now clears a deferred job's wait, but not through an IB cooldown."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from fastapi import HTTPException

from bifrost_flex_query.api.ingest import run_job_now


class _Cur:
    def __init__(self, parent: "_Conn") -> None:
        self.parent = parent

    def execute(self, query: str, params: Any = None) -> None:
        self.parent.statements.append((query, params))

    def fetchone(self) -> Any:
        return self.parent.row

    def __enter__(self) -> "_Cur":
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _Conn:
    def __init__(self, row: Any) -> None:
        self.row = row
        self.statements: list[tuple[str, Any]] = []
        self.committed = False

    def cursor(self) -> _Cur:
        return _Cur(self)

    def commit(self) -> None:
        self.committed = True


def test_run_now_clears_not_before() -> None:
    conn = _Conn({"id": 90, "kind": "flex-trades", "status": "pending", "error_category": "not_ready",
                  "not_before": datetime.now(timezone.utc) + timedelta(minutes=20)})
    out = run_job_now(90, force=False, conn=conn)
    assert out["ok"] and out["kind"] == "flex-trades" and out["was_not_before"]
    assert any("not_before = NULL" in q for q, _ in conn.statements) and conn.committed


def test_run_now_refuses_during_ib_cooldown_unless_forced() -> None:
    row = {"id": 91, "kind": "flex-trades", "status": "pending", "error_category": "throttled",
           "not_before": datetime.now(timezone.utc) + timedelta(minutes=20)}
    with pytest.raises(HTTPException) as exc:
        run_job_now(91, force=False, conn=_Conn(row))
    assert exc.value.status_code == 409 and "[1018]" not in exc.value.detail and "throttled" in exc.value.detail
    assert run_job_now(91, force=True, conn=_Conn(row))["ok"]


def test_run_now_only_for_pending_jobs() -> None:
    with pytest.raises(HTTPException) as exc:
        run_job_now(92, force=False, conn=_Conn({"id": 92, "kind": "x", "status": "done", "error_category": None, "not_before": None}))
    assert exc.value.status_code == 409
    with pytest.raises(HTTPException) as exc2:
        run_job_now(93, force=False, conn=_Conn(None))
    assert exc2.value.status_code == 404
