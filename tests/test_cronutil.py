"""Cron helper tests."""

from __future__ import annotations

from datetime import datetime, timezone

from bifrost_flex_query.scheduler.cronutil import next_fires, parse_cron, previous_fire


def test_parse_weekday_cron() -> None:
    minutes, hours, dows = parse_cron("30 22 * * 1-5")
    assert minutes == {30}
    assert hours == {22}
    assert dows == {1, 2, 3, 4, 5}


def test_next_fires_weekday() -> None:
    # Friday 22:00 UTC → next is Friday 22:30
    after = datetime(2026, 8, 14, 22, 0, tzinfo=timezone.utc)
    fires = next_fires("30 22 * * 1-5", after=after, count=2)
    assert fires[0].hour == 22 and fires[0].minute == 30
    assert fires[0].date().isoformat() == "2026-08-14"


def test_previous_fire() -> None:
    before = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)  # Monday
    prev = previous_fire("30 22 * * 1-5", before=before)
    assert prev is not None
    assert prev.hour == 22 and prev.minute == 30


def test_timezone_crons_plan_in_local_time() -> None:
    """06:30 America/New_York on 2026-09-08 (EDT) is 10:30 UTC — the instant Dagster fires."""
    after = datetime(2026, 9, 7, 12, 0, tzinfo=timezone.utc)  # Monday noon UTC
    fires = next_fires("30 6 * * 1-6", after=after, count=2, tz="America/New_York")
    assert fires[0] == datetime(2026, 9, 8, 10, 30, tzinfo=timezone.utc)
    assert fires[1] == datetime(2026, 9, 9, 10, 30, tzinfo=timezone.utc)
    # Saturday fires, Sunday does not.
    sat = next_fires("30 6 * * 1-6", after=datetime(2026, 9, 11, 12, 0, tzinfo=timezone.utc), count=2, tz="America/New_York")
    assert [f.date().isoformat() for f in sat] == ["2026-09-12", "2026-09-14"]
    prev = previous_fire("30 6 * * 1-6", before=datetime(2026, 9, 8, 11, 0, tzinfo=timezone.utc), tz="America/New_York")
    assert prev == datetime(2026, 9, 8, 10, 30, tzinfo=timezone.utc)
