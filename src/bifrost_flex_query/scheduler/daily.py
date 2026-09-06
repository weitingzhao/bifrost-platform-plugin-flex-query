"""Slot → job: what a scheduled Flex ingest looks like when it lands in the queue.

The trigger itself lives in bifrost-research (Dagster ``research_flex_morning``);
this module is the shared vocabulary: slot names, kinds, per-slot priority and
attempt budget, and the CLI that Dagster's HTTP call and the old CronJob both
funnel into.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import yaml

from bifrost_flex_query.config import load_config, postgres_connect_kwargs
from bifrost_flex_query.scheduler.enqueue import insert_job
from bifrost_flex_query.schema.ddl import ensure_flex_ops_schema

logger = logging.getLogger(__name__)

SLOT_NAMES = ("flex-trades", "flex-transactions")
SLOT_KIND = {
    "flex-trades": "flex-trades",
    "flex-transactions": "flex-transactions",
}
# Eight attempts half an hour apart cover a 06:30 ET first try until IB's
# statement is generated, without a single burst of requests.
DEFAULT_SLOT_MAX_ATTEMPTS = 8
# Knobs that shape how a job runs but not which job it is (dedupe ignores them).
_EXECUTION_KEYS = ("fallback", "catchup")
# A planned slot with no job this long after it fired is one the scheduler missed.
DEFAULT_CATCHUP_GRACE_SEC = 2700


def default_schedule_path() -> Path | None:
    env = (os.environ.get("SCHEDULE_CONFIG") or "").strip()
    if env:
        p = Path(env)
        if p.is_file():
            return p
    here = Path(__file__).resolve().parents[3]
    for candidate in (Path("/config/schedule.yaml"), here / "config" / "schedule.yaml"):
        if candidate.is_file():
            return candidate
    return None


def load_schedule(path: str | Path | None = None) -> dict[str, Any]:
    resolved: Path | None
    if path is not None:
        resolved = Path(path)
    else:
        resolved = default_schedule_path()
    if resolved is None or not resolved.is_file():
        return {"scheduler": {}}
    with resolved.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return raw if isinstance(raw, dict) else {"scheduler": {}}


def schedule_timezone(scheduler_cfg: Mapping[str, Any] | None) -> str | None:
    """The zone the slot crons are written in (None = UTC)."""
    tz = str((scheduler_cfg or {}).get("timezone") or "").strip()
    return tz or None


def catchup_grace_sec(scheduler_cfg: Mapping[str, Any] | None) -> int:
    raw = (scheduler_cfg or {}).get("catchup_grace_sec")
    try:
        return max(300, int(raw)) if raw not in (None, "") else DEFAULT_CATCHUP_GRACE_SEC
    except (TypeError, ValueError):
        return DEFAULT_CATCHUP_GRACE_SEC


def enqueue_slot(
    conn: Any,
    slot: str,
    *,
    scheduler_cfg: Mapping[str, Any] | None = None,
    payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    slot_key = str(slot).strip().lower()
    if slot_key not in SLOT_NAMES:
        raise ValueError(f"unknown slot: {slot!r} (expected one of {SLOT_NAMES})")
    cfg = dict(scheduler_cfg or {})
    slots = dict(cfg.get("slots") or {})
    scfg = dict(slots.get(slot_key) or {})
    priority = int(scfg.get("priority") or 0)
    max_attempts = int(scfg.get("max_attempts") or DEFAULT_SLOT_MAX_ATTEMPTS)
    kind = SLOT_KIND[slot_key]
    body = dict(payload or {})
    if "as_of" not in body:
        body["as_of"] = datetime.now(timezone.utc).date().isoformat()
    identity = {k: v for k, v in body.items() if k not in _EXECUTION_KEYS}
    # A queued run waits for IB rather than widening the query; a reader who
    # wants the old fallback chain says so in the payload.
    body.setdefault("fallback", False)
    job_id = insert_job(
        conn,
        kind=kind,
        payload=body,
        priority=priority,
        max_attempts=max_attempts,
        hash_payload=identity,
    )
    return {
        "slot": slot_key,
        "kind": kind,
        "enqueued": 0 if job_id is None else 1,
        "deduped": 1 if job_id is None else 0,
        "job_id": job_id,
        "payload": body,
        "max_attempts": max_attempts,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Enqueue Flex Query ingest jobs")
    parser.add_argument("--slot", required=True, choices=SLOT_NAMES)
    parser.add_argument("--config", default=None)
    parser.add_argument("--schedule", default=None)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")

    cfg = load_config(args.config)
    schedule = load_schedule(args.schedule)
    scheduler_cfg = dict(schedule.get("scheduler") or {})

    import time

    import psycopg2

    kw = postgres_connect_kwargs(cfg)
    conn = None
    last_err: Exception | None = None
    for attempt in range(1, 6):
        try:
            conn = psycopg2.connect(**{**kw, "connect_timeout": 10})
            break
        except psycopg2.OperationalError as exc:
            last_err = exc
            logger.warning("postgres connect attempt %s/5 failed: %s", attempt, exc)
            time.sleep(min(2 * attempt, 8))
    if conn is None:
        raise last_err if last_err is not None else RuntimeError("postgres connect failed")
    try:
        ensure_flex_ops_schema(conn)
        # Fail-closed: never enqueue when Flex credentials are missing (K8s Job
        # Complete must not mask an empty Secret).
        from bifrost_flex_query.orchestration.config_rw import resolve_flex_tokens

        host_tok, sec_tok, host_src, sec_src = resolve_flex_tokens()
        if not host_tok and not sec_tok:
            logger.error(
                "flex tokens missing (host=%s secondary=%s) — refuse enqueue",
                host_src,
                sec_src,
            )
            return 1
        result = enqueue_slot(conn, args.slot, scheduler_cfg=scheduler_cfg)
    finally:
        conn.close()
    logger.info("slot=%s enqueued=%s deduped=%s", result.get("slot"), result.get("enqueued"), result.get("deduped"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
