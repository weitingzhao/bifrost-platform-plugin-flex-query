"""Prometheus exposition for the ingest.

What an alert needs to know without reading the queue by hand: when each kind
last succeeded, how its latest job ended, whether a retry is pending, how old
the newest row of data is, and how old the Flex token is.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from fastapi import APIRouter, Depends
from fastapi.responses import PlainTextResponse

from bifrost_flex_query import __version__
from bifrost_flex_query.api.deps import db_conn
from bifrost_flex_query.orchestration.config_rw import flex_tokens_issued_at, resolve_flex_tokens
from bifrost_flex_query.scheduler.cronutil import next_fires, previous_fire
from bifrost_flex_query.scheduler.daily import SLOT_KIND, load_schedule, schedule_timezone
from bifrost_flex_query.schema.ddl import ensure_flex_ops_schema

router = APIRouter(tags=["metrics"])

JOB_STATUSES = ("pending", "running", "done", "failed")
_DATA_TABLES = (
    ("executions_raw_flex", "raw_broker.executions_raw_flex", "exec_time"),
    ("transactions", "raw_broker.transactions", "ts"),
)


def _ts(dt: datetime | None) -> float | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def _label(v: Any) -> str:
    return str(v).replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")


def _line(name: str, value: float | int, labels: Mapping[str, Any] | None = None) -> str:
    if labels:
        body = ",".join(f'{k}="{_label(v)}"' for k, v in labels.items())
        return f"{name}{{{body}}} {value}"
    return f"{name} {value}"


def render_metrics(snap: Mapping[str, Any]) -> str:
    """Text exposition from a snapshot dict (see ``collect_metrics``)."""
    out: list[str] = []

    def head(name: str, kind: str, help_: str) -> None:
        out.append(f"# HELP {name} {help_}")
        out.append(f"# TYPE {name} {kind}")

    head("bifrost_flex_plugin_info", "gauge", "Flex Query plugin version.")
    out.append(_line("bifrost_flex_plugin_info", 1, {"version": snap.get("version", "")}))

    head("bifrost_flex_ingest_last_success_timestamp_seconds", "gauge", "When this kind last completed a run.")
    head("bifrost_flex_ingest_last_attempt_ok", "gauge", "1 if the most recent attempt of this kind succeeded.")
    head("bifrost_flex_ingest_last_processed_rows", "gauge", "Rows upserted by the last successful run.")
    head("bifrost_flex_ingest_last_new_rows", "gauge", "Rows the last successful run added that did not exist before.")
    for r in snap.get("freshness", []):
        kind = r["dimension"]
        ts = _ts(r.get("latest_ts"))
        if ts is not None:
            out.append(_line("bifrost_flex_ingest_last_success_timestamp_seconds", ts, {"kind": kind}))
        if r.get("last_ok") is not None:
            out.append(_line("bifrost_flex_ingest_last_attempt_ok", 1 if r["last_ok"] else 0, {"kind": kind}))
        if r.get("processed_rows") is not None:
            out.append(_line("bifrost_flex_ingest_last_processed_rows", int(r["processed_rows"]), {"kind": kind}))
        if r.get("new_rows") is not None:
            out.append(_line("bifrost_flex_ingest_last_new_rows", int(r["new_rows"]), {"kind": kind}))

    head("bifrost_flex_ingest_last_job_status", "gauge", "1 for the status of the newest job of this kind.")
    head("bifrost_flex_ingest_last_job_category", "gauge", "1 for the error category of the newest job of this kind.")
    head("bifrost_flex_ingest_next_retry_timestamp_seconds", "gauge", "When the newest job of this kind may run again (pending with not_before).")
    for r in snap.get("last_jobs", []):
        kind = r["kind"]
        for st in JOB_STATUSES:
            out.append(_line("bifrost_flex_ingest_last_job_status", 1 if r.get("status") == st else 0, {"kind": kind, "status": st}))
        if r.get("error_category"):
            out.append(_line("bifrost_flex_ingest_last_job_category", 1, {"kind": kind, "category": r["error_category"]}))
        nb = _ts(r.get("not_before"))
        if r.get("status") == "pending" and nb is not None:
            out.append(_line("bifrost_flex_ingest_next_retry_timestamp_seconds", nb, {"kind": kind}))

    head("bifrost_flex_ingest_jobs", "gauge", "Jobs in the queue by status.")
    counts = dict(snap.get("counts") or {})
    for st in JOB_STATUSES:
        out.append(_line("bifrost_flex_ingest_jobs", int(counts.get(st, 0)), {"status": st}))

    head("bifrost_flex_planned_last_timestamp_seconds", "gauge", "Most recent planned fire of the slot.")
    head("bifrost_flex_planned_next_timestamp_seconds", "gauge", "Next planned fire of the slot.")
    for r in snap.get("planned", []):
        if r.get("last") is not None:
            out.append(_line("bifrost_flex_planned_last_timestamp_seconds", _ts(r["last"]), {"slot": r["slot"]}))
        if r.get("next") is not None:
            out.append(_line("bifrost_flex_planned_next_timestamp_seconds", _ts(r["next"]), {"slot": r["slot"]}))

    head("bifrost_flex_data_latest_timestamp_seconds", "gauge", "Newest row timestamp in the landed data.")
    for r in snap.get("data", []):
        ts = _ts(r.get("latest"))
        if ts is not None:
            out.append(_line("bifrost_flex_data_latest_timestamp_seconds", ts, {"table": r["table"]}))

    head("bifrost_flex_token_configured", "gauge", "1 when a Flex token is present for the side.")
    for side, present in (snap.get("tokens") or {}).items():
        out.append(_line("bifrost_flex_token_configured", 1 if present else 0, {"side": side}))
    age = snap.get("token_age_seconds")
    if age is not None:
        head("bifrost_flex_token_age_seconds", "gauge", "Seconds since the Flex tokens were issued (IB expires them after a year).")
        out.append(_line("bifrost_flex_token_age_seconds", int(age)))
    return "\n".join(out) + "\n"


def collect_metrics(conn: Any, *, now: datetime | None = None) -> dict[str, Any]:
    """Everything ``render_metrics`` needs, read in a handful of small queries."""
    now = now or datetime.now(timezone.utc)
    snap: dict[str, Any] = {"version": __version__}

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT dimension, latest_ts, last_ok, processed_rows, new_rows
            FROM ops_jobs.flex_ingest_freshness
            ORDER BY dimension
            """
        )
        snap["freshness"] = [dict(r) for r in cur.fetchall() or []]
        cur.execute(
            """
            SELECT DISTINCT ON (kind) kind, status, error_category, not_before
            FROM ops_jobs.job_flex_ingest
            ORDER BY kind, id DESC
            """
        )
        snap["last_jobs"] = [dict(r) for r in cur.fetchall() or []]
        cur.execute("SELECT status, count(*)::int AS n FROM ops_jobs.job_flex_ingest GROUP BY status")
        snap["counts"] = {str(r["status"]): int(r["n"]) for r in cur.fetchall() or []}
    conn.rollback()

    data: list[dict[str, Any]] = []
    for name, fq, col in _DATA_TABLES:
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT max({col}) AS ts FROM {fq}")
                row = cur.fetchone()
            data.append({"table": name, "latest": row["ts"] if row else None})
        except Exception:
            conn.rollback()
    snap["data"] = data

    schedule = load_schedule()
    scheduler_cfg = dict(schedule.get("scheduler") or {})
    tz = schedule_timezone(scheduler_cfg)
    slots = dict(scheduler_cfg.get("slots") or {})
    planned: list[dict[str, Any]] = []
    for slot in SLOT_KIND:
        cron = str(dict(slots.get(slot) or {}).get("cron") or "")
        if not cron:
            continue
        upcoming = next_fires(cron, after=now, count=1, tz=tz)
        planned.append(
            {
                "slot": slot,
                "last": previous_fire(cron, before=now, tz=tz),
                "next": upcoming[0] if upcoming else None,
            }
        )
    snap["planned"] = planned

    host_tok, sec_tok, _, _ = resolve_flex_tokens()
    snap["tokens"] = {"host": bool(host_tok), "secondary": bool(sec_tok)}
    issued_at, age_days = flex_tokens_issued_at(now)
    snap["token_issued_at"] = issued_at
    snap["token_age_seconds"] = None if age_days is None else age_days * 86400
    return snap


@router.get("/metrics", response_class=PlainTextResponse)
def metrics(conn: Any = Depends(db_conn)) -> str:
    ensure_flex_ops_schema(conn)
    return render_metrics(collect_metrics(conn))

