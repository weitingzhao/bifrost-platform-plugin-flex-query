"""The self-check's rules, as a pure function over what the API can read.

One verdict per kind, ordered by how much a human needs to do about it:

    failed    the last attempt is final — a config error or the attempt budget is gone
    missed    the planned slot passed and nothing was queued
    throttled IB refused the token; the queue is cooling down until a known time
    waiting   IB has no statement yet; the worker retries at a known time
    running   a job is in flight
    queued    a job is claimable now
    ok        the last run completed and the plan has not been missed
    idle      never ran

Each verdict carries the one sentence a reader needs and the actions that make
sense right now (with the reason when one is disabled), so the Console can show
buttons rather than a hint to go look somewhere else.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping
from zoneinfo import ZoneInfo

VERDICT_ORDER = ("failed", "missed", "throttled", "waiting", "running", "queued", "ok", "idle", "unknown")
HEARTBEAT_STALE_SEC = 180


@dataclass
class KindInput:
    kind: str
    job: Mapping[str, Any] | None
    freshness: Mapping[str, Any] | None
    planned_last: datetime | None
    planned_next: datetime | None
    max_attempts: int


@dataclass
class CheckInput:
    now: datetime
    tz: str | None
    grace_sec: int
    kinds: list[KindInput]
    heartbeat: Mapping[str, Any] | None
    tokens: Mapping[str, Any]
    """{host: bool, secondary: bool, issued_at: str|None, age_days: int|None}"""
    data_latest: Mapping[str, datetime | None]
    cooldown_until: datetime | None = None
    catchup_enabled: bool = True
    worker_poll_sec: float = 5.0


@dataclass
class Action:
    id: str
    label: str
    method: str
    path: str
    body: Mapping[str, Any] | None = None
    enabled: bool = True
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "label": self.label,
            "method": self.method,
            "path": self.path,
            "body": dict(self.body) if self.body else None,
            "enabled": self.enabled,
            "reason": self.reason,
        }


@dataclass
class KindVerdict:
    kind: str
    verdict: str
    headline: str
    detail: str | None = None
    next_at: datetime | None = None
    job: Mapping[str, Any] | None = None
    last_success_at: datetime | None = None
    actions: list[Action] = field(default_factory=list)


def _iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fmt_local(dt: datetime | None, tz: str | None) -> str:
    """'Mon 06:30 EDT' — the clock the Owner reads."""
    if dt is None:
        return "—"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    zone = ZoneInfo(tz) if tz else timezone.utc
    local = dt.astimezone(zone)
    label = local.tzname() or (tz or "UTC")
    return f"{local.strftime('%a %H:%M')} {label}"


def _aware(dt: Any) -> datetime | None:
    if not isinstance(dt, datetime):
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _job_error(job: Mapping[str, Any] | None) -> str:
    if not job:
        return ""
    res = job.get("result")
    if isinstance(res, Mapping) and res.get("error"):
        return str(res["error"])
    return ""


def _short(msg: str, n: int = 220) -> str:
    msg = " ".join(msg.split())
    return msg if len(msg) <= n else msg[: n - 1] + "…"


def _enqueue_action(kind: str, label: str, *, cooldown_until: datetime | None, now: datetime, tz: str | None) -> Action:
    a = Action("enqueue", label, "POST", "/flex/ingest/enqueue", {"slot": kind})
    if cooldown_until is not None and cooldown_until > now:
        a.enabled = False
        a.reason = f"IB throttle cooldown until {fmt_local(cooldown_until, tz)}"
    return a


def _missed_plan(k: KindInput, inp: CheckInput) -> bool:
    planned_last = _aware(k.planned_last)
    if planned_last is None:
        return False
    age = inp.now - planned_last
    if age < timedelta(seconds=inp.grace_sec) or age > timedelta(hours=24):
        return False
    if k.job is None:
        return True
    created = _aware(k.job.get("created_at"))
    return created is None or created < planned_last


def _missed_verdict(k: KindInput, inp: CheckInput, *, tail: str = "") -> KindVerdict:
    return KindVerdict(
        k.kind,
        "missed",
        f"Planned {fmt_local(_aware(k.planned_last), inp.tz)} came and went with no job{tail}.",
        (
            "The worker's catch-up enqueues it on its own after the grace period; press Enqueue to do it now."
            if inp.catchup_enabled
            else "Catch-up is disabled; enqueue it by hand."
        ),
        actions=[_enqueue_action(k.kind, "Enqueue today's slot", cooldown_until=inp.cooldown_until, now=inp.now, tz=inp.tz)],
    )


def assess_kind(k: KindInput, inp: CheckInput) -> KindVerdict:
    now, tz = inp.now, inp.tz
    job = k.job
    fresh = k.freshness or {}
    last_success = _aware(fresh.get("latest_ts"))

    def with_job(v: KindVerdict) -> KindVerdict:
        v.job = job
        v.last_success_at = last_success
        return v

    if job is None:
        if _missed_plan(k, inp):
            return with_job(_missed_verdict(k, inp))
        return with_job(
            KindVerdict(
                k.kind,
                "idle",
                "Never ran.",
                f"Next planned {fmt_local(k.planned_next, tz)}.",
                next_at=k.planned_next,
                actions=[_enqueue_action(k.kind, "Enqueue now", cooldown_until=inp.cooldown_until, now=now, tz=tz)],
            )
        )

    status = str(job.get("status") or "")
    attempts = int(job.get("attempts") or 0)
    max_attempts = int(job.get("max_attempts") or k.max_attempts)
    category = str(job.get("error_category") or "")
    err = _job_error(job)
    not_before = _aware(job.get("not_before"))

    if status == "running":
        started = _aware(job.get("started_at"))
        return with_job(
            KindVerdict(
                k.kind,
                "running",
                f"Running since {fmt_local(started, tz)} (attempt {attempts}/{max_attempts}).",
                "An IB fetch takes seconds to a few minutes; a run older than two hours is reclaimed automatically.",
            )
        )

    if status == "pending":
        if not_before is not None and not_before > now:
            run_now = Action("run_now", "Run now", "POST", f"/flex/ingest/jobs/{job.get('id')}/run-now")
            if category == "throttled":
                run_now.enabled = False
                run_now.reason = f"IB refused the token ([1018]); a request before {fmt_local(not_before, tz)} fails again"
                return with_job(
                    KindVerdict(
                        k.kind,
                        "throttled",
                        f"IB throttled the token; cooling down until {fmt_local(not_before, tz)} (attempt {attempts}/{max_attempts}).",
                        _short(err) or None,
                        next_at=not_before,
                        actions=[run_now],
                    )
                )
            why = "IB has not generated the statement yet" if category == "not_ready" else (category or "retry scheduled")
            return with_job(
                KindVerdict(
                    k.kind,
                    "waiting",
                    f"{why}; the worker retries at {fmt_local(not_before, tz)} (attempt {attempts}/{max_attempts}).",
                    _short(err) or None,
                    next_at=not_before,
                    actions=[run_now],
                )
            )
        return with_job(
            KindVerdict(k.kind, "queued", f"Queued; the worker claims it within {int(inp.worker_poll_sec)}s.")
        )

    if status == "failed":
        if category == "config":
            return with_job(
                KindVerdict(
                    k.kind,
                    "failed",
                    "Failed on configuration — retrying cannot fix this.",
                    _short(err) or None,
                    actions=[
                        Action(
                            "fix_config",
                            "Fix config, then enqueue",
                            "POST",
                            "/flex/ingest/enqueue",
                            {"slot": k.kind},
                            enabled=False,
                            reason=(
                                "Expired/invalid token: generate a new one in IB Account Management and run "
                                "make sync-flex-tokens; bad query id: Config tab"
                            ),
                        )
                    ],
                )
            )
        finished = _aware(job.get("finished_at"))
        return with_job(
            KindVerdict(
                k.kind,
                "failed",
                f"Failed for good at {fmt_local(finished, tz)} ({category or 'error'}, {attempts}/{max_attempts} attempts).",
                _short(err) or None,
                actions=[_enqueue_action(k.kind, "Enqueue again", cooldown_until=inp.cooldown_until, now=now, tz=tz)],
            )
        )

    # done
    if _missed_plan(k, inp):
        return with_job(
            _missed_verdict(k, inp, tail=f"; last run was {fmt_local(_aware(job.get('created_at')), tz)}")
        )
    finished = _aware(job.get("finished_at"))
    processed = fresh.get("processed_rows")
    new_rows = fresh.get("new_rows")
    rows = ""
    if processed is not None:
        rows = f", {int(processed)} rows"
        if new_rows is not None:
            rows += f" ({int(new_rows)} new)"
    return with_job(
        KindVerdict(
            k.kind,
            "ok",
            f"Completed {fmt_local(finished, tz)}{rows}; next planned {fmt_local(k.planned_next, tz)}.",
            next_at=k.planned_next,
        )
    )


def _worst(verdicts: list[str]) -> str:
    for v in VERDICT_ORDER:
        if v in verdicts:
            return v
    return "unknown"


def _checks(inp: CheckInput) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    t = inp.tokens
    host, sec = bool(t.get("host")), bool(t.get("secondary"))
    age = t.get("age_days")
    if not host and not sec:
        out.append({"id": "tokens", "ok": False, "detail": "No Flex token in Secret bifrost-flex-tokens — run make sync-flex-tokens."})
    else:
        issued = f", issued {t.get('issued_at')} ({age}d ago)" if t.get("issued_at") else ", issue date unknown"
        warn = age is not None and int(age) > 335
        sides = "+".join(s for s, on in (("host", host), ("secondary", sec)) if on)
        out.append(
            {
                "id": "tokens",
                "ok": not warn,
                "detail": f"{sides} from Secret{issued}" + (" — IB expires tokens after a year, rotate soon." if warn else ""),
            }
        )
    hb = inp.heartbeat or {}
    seen = _aware(hb.get("seen_at"))
    if seen is None:
        out.append({"id": "worker", "ok": False, "detail": "Worker has never reported in — is flex-query-worker running on 0.6.1+?"})
    else:
        silent = (inp.now - seen).total_seconds()
        ok = silent <= HEARTBEAT_STALE_SEC
        out.append(
            {
                "id": "worker",
                "ok": ok,
                "detail": (
                    f"worker alive, seen {int(silent)}s ago (pod {hb.get('pod') or '?'}, v{hb.get('version') or '?'}, "
                    f"done {hb.get('jobs_done') or 0} / retried {hb.get('jobs_retried') or 0} / failed {hb.get('jobs_failed') or 0}, "
                    f"db reconnects {hb.get('db_reconnects') or 0}, catch-ups {hb.get('catchups') or 0})"
                    if ok
                    else f"worker silent for {int(silent // 60)} min (last pod {hb.get('pod') or '?'}) — check flex-query-worker."
                ),
            }
        )
    if inp.cooldown_until is not None and inp.cooldown_until > inp.now:
        out.append({"id": "cooldown", "ok": False, "detail": f"IB throttle cooldown until {fmt_local(inp.cooldown_until, inp.tz)} — no request before then."})
    else:
        out.append({"id": "cooldown", "ok": True, "detail": "No IB throttle cooldown."})
    nxt = [k.planned_next for k in inp.kinds if k.planned_next is not None]
    out.append(
        {
            "id": "schedule",
            "ok": True,
            "detail": f"Trigger: Dagster research_flex_morning_schedule; next planned {fmt_local(min(nxt), inp.tz) if nxt else '—'}"
            + (f"; worker catch-up {inp.grace_sec // 60} min after a missed slot" if inp.catchup_enabled else "; catch-up disabled"),
        }
    )
    parts = []
    stale = False
    for table, dt in inp.data_latest.items():
        d = _aware(dt)
        if d is None:
            parts.append(f"{table}: none")
            stale = True
            continue
        parts.append(f"{table}: {fmt_local(d, inp.tz)}")
        if table == "executions_raw_flex" and (inp.now - d).total_seconds() > 7 * 86400:
            stale = True
    out.append({"id": "data", "ok": not stale, "detail": "; ".join(parts) or "—"})
    return out


def _next_step(overall: str, verdicts: list[KindVerdict], inp: CheckInput) -> str:
    tz = inp.tz
    first = {v.verdict: v for v in reversed(verdicts)}
    if overall == "failed":
        v = first["failed"]
        if v.job and str(v.job.get("error_category") or "") == "config":
            return f"Fix the configuration for {v.kind} (see its action), then enqueue; nothing automatic will help."
        return f"{v.kind} exhausted its attempts; enqueue it again (the next planned run will also try)."
    if overall == "missed":
        v = first["missed"]
        return f"Enqueue {v.kind} now, or wait for the worker's catch-up." if inp.catchup_enabled else f"Enqueue {v.kind} now."
    if overall == "throttled":
        v = first["throttled"]
        return f"Do nothing until {fmt_local(v.next_at, tz)}; the worker retries by itself and every request before then fails."
    if overall == "waiting":
        v = first["waiting"]
        return f"The worker retries {v.kind} at {fmt_local(v.next_at, tz)}; press Run now to skip the wait."
    if overall in ("running", "queued"):
        return "A job is in flight; refresh in a minute."
    nxt = [x.next_at for x in verdicts if x.next_at]
    if nxt:
        return f"Nothing to do; next planned run {fmt_local(min(nxt), tz)}."
    return "Nothing to do."


def diagnose(inp: CheckInput) -> dict[str, Any]:
    verdicts = [assess_kind(k, inp) for k in inp.kinds]
    checks = _checks(inp)
    overall = _worst([v.verdict for v in verdicts])
    if overall in ("ok", "idle") and any(not c["ok"] for c in checks):
        overall = "attention"
    return {
        "generated_at": _iso(inp.now),
        "timezone": inp.tz or "UTC",
        "verdict": overall,
        "next_step": _next_step(overall, verdicts, inp),
        "kinds": [
            {
                "kind": v.kind,
                "verdict": v.verdict,
                "headline": v.headline,
                "detail": v.detail,
                "next_at": _iso(v.next_at),
                "next_in_seconds": None if v.next_at is None else max(0, int((v.next_at - inp.now).total_seconds())),
                "last_success_at": _iso(v.last_success_at),
                "job": None
                if v.job is None
                else {
                    "id": v.job.get("id"),
                    "status": v.job.get("status"),
                    "attempts": v.job.get("attempts"),
                    "max_attempts": v.job.get("max_attempts"),
                    "error_category": v.job.get("error_category"),
                    "not_before": _iso(_aware(v.job.get("not_before"))),
                    "created_at": _iso(_aware(v.job.get("created_at"))),
                    "finished_at": _iso(_aware(v.job.get("finished_at"))),
                    "error": _short(_job_error(v.job), 500) or None,
                    "manual": bool(v.job.get("payload", {}).get("manual")) if isinstance(v.job.get("payload"), Mapping) else False,
                },
                "actions": [a.to_dict() for a in v.actions],
            }
            for v in verdicts
        ],
        "checks": checks,
    }
