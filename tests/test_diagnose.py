"""The self-check's verdicts, one scenario per thing an operator can see."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from bifrost_flex_query.ops.diagnose import CheckInput, KindInput, diagnose, fmt_local

NOW = datetime(2026, 9, 8, 11, 5, tzinfo=timezone.utc)  # Tue 07:05 EDT
PLANNED = datetime(2026, 9, 8, 10, 30, tzinfo=timezone.utc)
NEXT = datetime(2026, 9, 9, 10, 30, tzinfo=timezone.utc)
HB_OK = {"seen_at": NOW - timedelta(seconds=20), "pod": "w-1", "version": "0.6.1", "jobs_done": 3}
TOKENS = {"host": True, "secondary": True, "issued_at": "2026-08-24", "age_days": 15}
DATA = {"executions_raw_flex": NOW - timedelta(hours=20), "transactions": NOW - timedelta(hours=20)}


def _inp(job, fresh=None, **over) -> CheckInput:
    base = dict(
        now=NOW,
        tz="America/New_York",
        grace_sec=2700,
        kinds=[KindInput("flex-trades", job, fresh, PLANNED, NEXT, 8)],
        heartbeat=HB_OK,
        tokens=TOKENS,
        data_latest=DATA,
    )
    base.update(over)
    return CheckInput(**base)


def test_waiting_names_the_retry_time_and_offers_run_now() -> None:
    job = {
        "id": 90, "status": "pending", "attempts": 1, "max_attempts": 8, "error_category": "not_ready",
        "not_before": NOW + timedelta(minutes=27), "created_at": PLANNED,
        "result": {"error": "Flex query 1/2 (host): [1003] Statement is not available."},
    }
    out = diagnose(_inp(job, {"latest_ts": NOW - timedelta(days=1)}))
    k = out["kinds"][0]
    assert out["verdict"] == "waiting" and k["verdict"] == "waiting"
    assert "07:32 EDT" in k["headline"] and "attempt 1/8" in k["headline"]
    assert k["actions"][0]["id"] == "run_now" and k["actions"][0]["enabled"]
    assert k["actions"][0]["path"] == "/flex/ingest/jobs/90/run-now"
    assert "Run now" in out["next_step"]


def test_throttled_disables_run_now_with_the_cooldown_time() -> None:
    until = NOW + timedelta(minutes=25)
    job = {"id": 91, "status": "pending", "attempts": 2, "max_attempts": 8, "error_category": "throttled",
           "not_before": until, "created_at": PLANNED, "result": {"error": "[1018] Too many requests"}}
    out = diagnose(_inp(job, cooldown_until=until))
    k = out["kinds"][0]
    assert k["verdict"] == "throttled"
    assert not k["actions"][0]["enabled"] and "[1018]" in k["actions"][0]["reason"]
    assert out["next_step"].startswith("Do nothing until")
    assert any(c["id"] == "cooldown" and not c["ok"] for c in out["checks"])


def test_config_failure_tells_the_operator_what_to_fix() -> None:
    job = {"id": 92, "status": "failed", "attempts": 1, "max_attempts": 8, "error_category": "config",
           "finished_at": NOW - timedelta(minutes=3), "created_at": PLANNED,
           "result": {"error": "Flex request failed: [1012] Token has expired."}}
    out = diagnose(_inp(job))
    k = out["kinds"][0]
    assert out["verdict"] == "failed"
    assert "configuration" in k["headline"]
    assert "sync-flex-tokens" in k["actions"][0]["reason"]
    assert "nothing automatic will help" in out["next_step"]


def test_missed_plan_after_grace_offers_enqueue_and_mentions_catchup() -> None:
    old = {"id": 80, "status": "done", "attempts": 1, "max_attempts": 8, "created_at": PLANNED - timedelta(days=1),
           "finished_at": PLANNED - timedelta(days=1) + timedelta(minutes=1)}
    late = NOW + timedelta(minutes=40)  # 75 min after the slot
    out = diagnose(_inp(old, now=late))
    k = out["kinds"][0]
    assert k["verdict"] == "missed"
    assert k["actions"][0]["id"] == "enqueue" and k["actions"][0]["body"] == {"slot": "flex-trades"}
    assert "catch-up" in k["detail"]
    # Inside the grace period the same state is still 'ok' — nothing to press yet.
    assert diagnose(_inp(old))["kinds"][0]["verdict"] == "ok"


def test_ok_reports_rows_and_next_plan() -> None:
    job = {"id": 93, "status": "done", "attempts": 1, "max_attempts": 8, "created_at": PLANNED + timedelta(seconds=3),
           "finished_at": PLANNED + timedelta(seconds=40)}
    out = diagnose(_inp(job, {"latest_ts": PLANNED + timedelta(seconds=40), "processed_rows": 50, "new_rows": 2}))
    k = out["kinds"][0]
    assert out["verdict"] == "ok"
    assert "50 rows (2 new)" in k["headline"] and "Wed 06:30 EDT" in k["headline"]
    assert out["next_step"] == "Nothing to do; next planned run Wed 06:30 EDT."


def test_silent_worker_turns_ok_into_attention() -> None:
    job = {"id": 93, "status": "done", "attempts": 1, "max_attempts": 8, "created_at": PLANNED + timedelta(seconds=3)}
    out = diagnose(_inp(job, heartbeat={"seen_at": NOW - timedelta(minutes=12), "pod": "w-0"}))
    assert out["verdict"] == "attention"
    worker = next(c for c in out["checks"] if c["id"] == "worker")
    assert not worker["ok"] and "silent for 12 min" in worker["detail"]


def test_fmt_local_reads_in_the_owner_clock() -> None:
    assert fmt_local(PLANNED, "America/New_York") == "Tue 06:30 EDT"
    assert fmt_local(None, "America/New_York") == "—"
