"""The worker's catch-up: a planned slot with no job after the grace period is enqueued once."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from bifrost_flex_query.worker.catchup import catchup_missed_slots, missed_slots

CFG = {
    "timezone": "America/New_York",
    "slots": {
        "flex-trades": {"cron": "30 6 * * 1-6", "priority": 5, "max_attempts": 8},
        "flex-transactions": {"cron": "30 6 * * 1-6", "priority": 4, "max_attempts": 8},
    },
}
PLANNED = datetime(2026, 9, 8, 10, 30, tzinfo=timezone.utc)  # Tue 06:30 EDT


def test_nothing_missed_inside_the_grace_period() -> None:
    now = PLANNED.replace(minute=59)  # 29 min after the slot
    assert missed_slots(CFG, now=now, grace_sec=2700, jobs_since={}) == []


def test_missed_when_no_job_since_the_slot() -> None:
    now = datetime(2026, 9, 8, 11, 20, tzinfo=timezone.utc)
    out = missed_slots(CFG, now=now, grace_sec=2700, jobs_since={"flex-trades": datetime(2026, 9, 7, 10, 31, tzinfo=timezone.utc)})
    assert [m["kind"] for m in out] == ["flex-trades", "flex-transactions"]
    assert out[0]["planned_at"] == PLANNED
    assert out[0]["as_of"] == "2026-09-08"


def test_a_job_created_after_the_slot_means_not_missed() -> None:
    now = datetime(2026, 9, 8, 11, 20, tzinfo=timezone.utc)
    since = {"flex-trades": PLANNED, "flex-transactions": datetime(2026, 9, 8, 10, 30, 5, tzinfo=timezone.utc)}
    assert missed_slots(CFG, now=now, grace_sec=2700, jobs_since=since) == []


def test_sunday_has_no_slot_to_miss_and_old_slots_are_ignored() -> None:
    sunday = datetime(2026, 9, 6, 18, 0, tzinfo=timezone.utc)
    # Saturday 06:30 EDT was 31.5h ago — older than a day, not a catch-up candidate.
    assert missed_slots(CFG, now=sunday, grace_sec=2700, jobs_since={}) == []


class _Cur:
    def __init__(self, parent: "_Conn") -> None:
        self.parent = parent

    def execute(self, query: str, params: Any = None) -> None:
        self.parent.statements.append((query, params))
        self.parent._last = query

    def fetchall(self) -> list[Any]:
        return [("flex-trades", datetime(2026, 9, 7, 10, 31, tzinfo=timezone.utc))]

    def fetchone(self) -> Any:
        self.parent.ids += 1
        return (self.parent.ids,)

    def __enter__(self) -> "_Cur":
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _Conn:
    def __init__(self) -> None:
        self.statements: list[tuple[str, Any]] = []
        self.ids = 100
        self._last = ""

    def cursor(self) -> _Cur:
        return _Cur(self)

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None


def test_catchup_enqueues_with_the_slot_day_and_the_catchup_flag() -> None:
    conn = _Conn()
    now = datetime(2026, 9, 8, 11, 20, tzinfo=timezone.utc)
    done = catchup_missed_slots(conn, CFG, now=now, grace_sec=2700)
    assert [d["kind"] for d in done] == ["flex-trades", "flex-transactions"]
    inserts = [p for q, p in conn.statements if "INSERT INTO ops_jobs.job_flex_ingest" in q]
    assert len(inserts) == 2
    assert '"as_of": "2026-09-08"' in inserts[0][1] and '"catchup": true' in inserts[0][1]
    # identity hash ignores the catchup flag: a late Dagster enqueue for the same day dedupes.
    from bifrost_flex_query.scheduler.enqueue import payload_hash

    assert inserts[0][2] == payload_hash({"as_of": "2026-09-08"})
