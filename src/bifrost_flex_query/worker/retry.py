"""Retry policy for Flex ingest jobs.

IB's Flex Web Service answers a request for a statement that is not generated
yet with "not ready" codes (1003/1004/1019 …). Those are not failures — the
right move is to come back later. Hammering them immediately is what earns the
1018 throttle, and once throttled every token on the account is throttled.
This module turns an error string into a category and a category into a plan:
retry after a delay, cool the whole queue down, or give up.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Iterable


class ErrorCategory(str, Enum):
    NOT_READY = "not_ready"  # statement not generated yet — try again later
    THROTTLED = "throttled"  # 1018 — every request from this token is refused for a while
    CONFIG = "config"  # token / query id / account problem — retrying cannot help
    TRANSIENT = "transient"  # network / database hiccup
    UNKNOWN = "unknown"


# IB Flex Web Service error codes (ErrorCode in the SendRequest response).
NOT_READY_CODES = frozenset({1003, 1004, 1005, 1006, 1007, 1008, 1009, 1019, 1021})
THROTTLED_CODES = frozenset({1018})
CONFIG_CODES = frozenset({1010, 1011, 1012, 1013, 1014, 1015, 1016, 1017, 1020})

_CODE_RE = re.compile(r"\[(\d{4})\]")
_CONFIG_MARKERS = (
    "No Flex credentials",
    "Token has expired",
    "Token is invalid",
    "Query is invalid",
    "unknown job kind",
)
_TRANSIENT_MARKERS = (
    "timed out",
    "Connection refused",
    "Connection reset",
    "connection to server",
    "could not connect",
    "Name or service not known",
    "Temporary failure in name resolution",
    "URLError",
    "HTTP Error 5",
    "OperationalError",
    "InterfaceError",
    "did not return report",
)


def flex_error_codes(message: str) -> set[int]:
    """Every `[NNNN]` IB code quoted in an error string (a job may join several)."""
    return {int(m) for m in _CODE_RE.findall(message or "")}


def classify_flex_error(message: str) -> ErrorCategory:
    """Throttle dominates: a 1018 anywhere means the next request will fail too."""
    msg = message or ""
    codes = flex_error_codes(msg)
    if codes & THROTTLED_CODES:
        return ErrorCategory.THROTTLED
    if codes & CONFIG_CODES or any(m in msg for m in _CONFIG_MARKERS):
        return ErrorCategory.CONFIG
    if codes & NOT_READY_CODES:
        return ErrorCategory.NOT_READY
    if any(m in msg for m in _TRANSIENT_MARKERS):
        return ErrorCategory.TRANSIENT
    return ErrorCategory.UNKNOWN


@dataclass(frozen=True)
class RetryPolicy:
    """Delays in seconds. Defaults fit a 06:30 ET first attempt with ~8 attempts before noon."""

    not_ready_delay_sec: int = 1800
    throttled_delay_sec: int = 1800
    transient_delay_sec: int = 120
    unknown_delay_sec: int = 300

    @classmethod
    def from_config(cls, raw: dict | None) -> "RetryPolicy":
        data = dict(raw or {})
        out: dict[str, int] = {}
        for field in ("not_ready_delay_sec", "throttled_delay_sec", "transient_delay_sec", "unknown_delay_sec"):
            val = data.get(field)
            if val is None or str(val).strip() == "":
                continue
            try:
                out[field] = max(30, int(val))
            except (TypeError, ValueError):
                continue
        return cls(**out)


@dataclass(frozen=True)
class RetryPlan:
    status: str  # 'pending' (retry later) | 'failed' (final)
    category: ErrorCategory
    delay_sec: int = 0
    """Seconds before this job may be claimed again."""
    cooldown_all_sec: int = 0
    """Seconds every other pending job must also wait (token throttle)."""

    @property
    def retry(self) -> bool:
        return self.status == "pending"


def plan_retry(
    category: ErrorCategory,
    *,
    attempts: int,
    max_attempts: int,
    policy: RetryPolicy | None = None,
) -> RetryPlan:
    """What to do with a job whose attempt just failed with `category`."""
    p = policy or RetryPolicy()
    if category is ErrorCategory.CONFIG:
        return RetryPlan("failed", category)
    if int(attempts) >= int(max_attempts):
        return RetryPlan("failed", category)
    if category is ErrorCategory.NOT_READY:
        return RetryPlan("pending", category, delay_sec=p.not_ready_delay_sec)
    if category is ErrorCategory.THROTTLED:
        return RetryPlan(
            "pending",
            category,
            delay_sec=p.throttled_delay_sec,
            cooldown_all_sec=p.throttled_delay_sec,
        )
    if category is ErrorCategory.TRANSIENT:
        return RetryPlan("pending", category, delay_sec=p.transient_delay_sec)
    return RetryPlan("pending", category, delay_sec=p.unknown_delay_sec)


def summarize_categories(messages: Iterable[str]) -> ErrorCategory:
    """The category of a job made of several per-account errors: the worst one wins."""
    order = (
        ErrorCategory.THROTTLED,
        ErrorCategory.CONFIG,
        ErrorCategory.NOT_READY,
        ErrorCategory.TRANSIENT,
        ErrorCategory.UNKNOWN,
    )
    seen = {classify_flex_error(m) for m in messages}
    for cat in order:
        if cat in seen:
            return cat
    return ErrorCategory.UNKNOWN
