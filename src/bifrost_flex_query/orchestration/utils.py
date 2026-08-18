"""Pure helpers for Flex execution row processing."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


def rows_span(rows: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    """Min/max calendar dates from execution-like rows. Returns (iso_min, iso_max)."""
    min_d: Optional[date] = None
    max_d: Optional[date] = None
    for r in rows:
        d: Optional[date] = None
        t_val = r.get("time")
        if isinstance(t_val, (int, float)):
            try:
                d = datetime.fromtimestamp(float(t_val), tz=timezone.utc).date()
            except Exception:
                d = None
        elif isinstance(t_val, datetime):
            try:
                d = t_val.date()
            except Exception:
                d = None
        if d is None:
            td = r.get("trade_date") or r.get("report_date")
            if isinstance(td, date):
                d = td
        if d is None:
            continue
        if min_d is None or d < min_d:
            min_d = d
        if max_d is None or d > max_d:
            max_d = d
    return (
        min_d.isoformat() if min_d is not None else None,
        max_d.isoformat() if max_d is not None else None,
    )
