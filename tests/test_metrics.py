"""/metrics exposition: the series an alert reads, rendered from a snapshot."""

from __future__ import annotations

from datetime import datetime, timezone

from bifrost_flex_query.api.metrics import render_metrics

T_SUCCESS = datetime(2026, 9, 5, 14, 36, tzinfo=timezone.utc)
T_RETRY = datetime(2026, 9, 6, 11, 0, tzinfo=timezone.utc)
T_NEXT = datetime(2026, 9, 7, 10, 30, tzinfo=timezone.utc)
T_DATA = datetime(2026, 9, 4, tzinfo=timezone.utc)


def _snap() -> dict:
    t = T_SUCCESS
    return {
        "version": "0.6.0",
        "freshness": [
            {"dimension": "flex-trades", "latest_ts": t, "last_ok": False, "processed_rows": 50, "new_rows": 2},
            {"dimension": "flex-transactions", "latest_ts": None, "last_ok": None, "processed_rows": None, "new_rows": None},
        ],
        "last_jobs": [
            {"kind": "flex-trades", "status": "pending", "error_category": "not_ready", "not_before": T_RETRY},
            {"kind": "flex-transactions", "status": "failed", "error_category": "throttled", "not_before": None},
        ],
        "counts": {"pending": 1, "failed": 21, "done": 23},
        "planned": [{"slot": "flex-trades", "last": t, "next": T_NEXT}],
        "data": [{"table": "executions_raw_flex", "latest": T_DATA}, {"table": "transactions", "latest": None}],
        "tokens": {"host": True, "secondary": False},
        "token_age_seconds": 13 * 86400,
    }


def test_render_metrics_series() -> None:
    text = render_metrics(_snap())
    lines = set(text.splitlines())
    assert 'bifrost_flex_plugin_info{version="0.6.0"} 1' in lines
    assert f'bifrost_flex_ingest_last_success_timestamp_seconds{{kind="flex-trades"}} {T_SUCCESS.timestamp()}' in lines
    assert 'bifrost_flex_ingest_last_attempt_ok{kind="flex-trades"} 0' in lines
    assert 'bifrost_flex_ingest_last_new_rows{kind="flex-trades"} 2' in lines
    # An unknown outcome emits nothing rather than a misleading 0/1.
    assert not any(line.startswith('bifrost_flex_ingest_last_attempt_ok{kind="flex-transactions"}') for line in lines)
    assert 'bifrost_flex_ingest_last_job_status{kind="flex-trades",status="pending"} 1' in lines
    assert 'bifrost_flex_ingest_last_job_status{kind="flex-trades",status="failed"} 0' in lines
    assert 'bifrost_flex_ingest_last_job_status{kind="flex-transactions",status="failed"} 1' in lines
    assert 'bifrost_flex_ingest_last_job_category{kind="flex-transactions",category="throttled"} 1' in lines
    assert f'bifrost_flex_ingest_next_retry_timestamp_seconds{{kind="flex-trades"}} {T_RETRY.timestamp()}' in lines
    assert 'bifrost_flex_ingest_jobs{status="running"} 0' in lines
    assert 'bifrost_flex_ingest_jobs{status="failed"} 21' in lines
    assert f'bifrost_flex_planned_next_timestamp_seconds{{slot="flex-trades"}} {T_NEXT.timestamp()}' in lines
    assert f'bifrost_flex_data_latest_timestamp_seconds{{table="executions_raw_flex"}} {T_DATA.timestamp()}' in lines
    assert 'bifrost_flex_token_configured{side="secondary"} 0' in lines
    assert "bifrost_flex_token_age_seconds 1123200" in lines
    assert text.endswith("\n")


def test_render_metrics_without_token_age() -> None:
    snap = _snap()
    snap["token_age_seconds"] = None
    assert "bifrost_flex_token_age_seconds" not in render_metrics(snap)
