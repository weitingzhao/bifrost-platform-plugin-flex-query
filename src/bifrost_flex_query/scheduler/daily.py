"""CronJob-driven enqueue into ops_jobs.job_flex_ingest."""

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
    kind = SLOT_KIND[slot_key]
    body = dict(payload or {})
    if "as_of" not in body:
        body["as_of"] = datetime.now(timezone.utc).date().isoformat()
    job_id = insert_job(conn, kind=kind, payload=body, priority=priority)
    return {
        "slot": slot_key,
        "kind": kind,
        "enqueued": 0 if job_id is None else 1,
        "deduped": 1 if job_id is None else 0,
        "job_id": job_id,
        "payload": body,
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
