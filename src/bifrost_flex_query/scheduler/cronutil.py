"""Minimal 5-field cron helpers for schedule plan / adherence UI.

Crons are matched in ``tz`` (an IANA name) and returned as UTC instants, so a
schedule written the way Dagster writes it — ``30 6 * * 1-6`` in
America/New_York — plans the same fire times the scheduler will actually use.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo


def _parse_part(part: str, minimum: int, maximum: int) -> set[int]:
    p = part.strip()
    if p.startswith("*/"):
        step = int(p[2:])
        if step <= 0:
            raise ValueError(f"invalid step in cron field: {part!r}")
        return {v for v in range(minimum, maximum + 1) if v % step == 0}
    if "-" in p:
        a, b = p.split("-", 1)
        return set(range(int(a), int(b) + 1))
    return {int(p)}


def _parse_field(field: str, minimum: int, maximum: int) -> set[int] | None:
    f = field.strip()
    if f == "*":
        return None
    out: set[int] = set()
    for part in f.split(","):
        out |= _parse_part(part, minimum, maximum)
    return out


def parse_cron(expr: str) -> tuple[set[int] | None, set[int] | None, set[int] | None]:
    parts = expr.strip().split()
    if len(parts) != 5:
        raise ValueError(f"expected 5-field cron, got {expr!r}")
    minute_s, hour_s, _dom, _month, dow_s = parts
    minutes = _parse_field(minute_s, 0, 59)
    hours = _parse_field(hour_s, 0, 23)
    dows = _parse_field(dow_s, 0, 6)
    return minutes, hours, dows


def _matches(dt: datetime, minutes: set[int] | None, hours: set[int] | None, dows: set[int] | None) -> bool:
    if minutes is not None and dt.minute not in minutes:
        return False
    if hours is not None and dt.hour not in hours:
        return False
    if dows is not None:
        cron_dow = 0 if dt.weekday() == 6 else dt.weekday() + 1
        if cron_dow not in dows:
            return False
    return True


def _zone(tz: str | None) -> ZoneInfo | timezone:
    return ZoneInfo(tz) if tz else timezone.utc


def iter_cron_fires(expr: str, *, start: datetime, end: datetime, tz: str | None = None) -> list[datetime]:
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    minutes, hours, dows = parse_cron(expr)
    zone = _zone(tz)
    cur = start.replace(second=0, microsecond=0)
    if cur < start:
        cur += timedelta(minutes=1)
    out: list[datetime] = []
    while cur < end:
        if _matches(cur.astimezone(zone), minutes, hours, dows):
            out.append(cur.astimezone(timezone.utc))
        cur += timedelta(minutes=1)
    return out


def next_fires(
    expr: str, *, after: datetime, count: int = 3, horizon_days: int = 14, tz: str | None = None
) -> list[datetime]:
    start = after + timedelta(minutes=1)
    end = after + timedelta(days=horizon_days)
    return iter_cron_fires(expr, start=start, end=end, tz=tz)[: max(0, int(count))]


def previous_fire(
    expr: str, *, before: datetime, lookback_days: int = 14, tz: str | None = None
) -> datetime | None:
    start = before - timedelta(days=lookback_days)
    fires = iter_cron_fires(expr, start=start, end=before, tz=tz)
    return fires[-1] if fires else None


def iso_z(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
