"""Retry classification and planning — pinned against the failure strings IB actually returned."""

from __future__ import annotations

from bifrost_flex_query.worker.retry import (
    ErrorCategory,
    RetryPolicy,
    classify_flex_error,
    flex_error_codes,
    plan_retry,
    summarize_categories,
)

# Verbatim from ops_jobs.job_flex_ingest on 2026-09-01 .. 09-05.
NOT_READY = (
    "Flex query 1/2 (host 1428383): Flex request failed: [1004] Statement is incomplete at this time. "
    "Please try again shortly.; Flex query 2/2 (secondary 1428413): Flex request failed: [1003] Statement is not available."
)
THROTTLED_AFTER_NOT_READY = (
    "Flex request failed: [1003] Statement is not available.; fallback query-default failed: "
    "Flex request failed: [1018] Too many requests have been made from this token. Please try again shortly."
)
CONFIG = "No Flex credentials for trades: set FLEX_HOST_TOKEN / FLEX_SECONDARY_TOKEN"


def test_codes_are_extracted_from_joined_messages() -> None:
    assert flex_error_codes(NOT_READY) == {1003, 1004}
    assert flex_error_codes(THROTTLED_AFTER_NOT_READY) == {1003, 1018}
    assert flex_error_codes("boom") == set()


def test_not_ready_is_not_a_failure() -> None:
    assert classify_flex_error(NOT_READY) is ErrorCategory.NOT_READY
    assert classify_flex_error("[1019] Statement generation in progress. Please try again shortly.") is ErrorCategory.NOT_READY


def test_throttle_dominates_whatever_else_is_in_the_message() -> None:
    assert classify_flex_error(THROTTLED_AFTER_NOT_READY) is ErrorCategory.THROTTLED
    assert summarize_categories([NOT_READY, THROTTLED_AFTER_NOT_READY]) is ErrorCategory.THROTTLED


def test_config_errors_never_retry() -> None:
    assert classify_flex_error(CONFIG) is ErrorCategory.CONFIG
    assert classify_flex_error("Flex request failed: [1012] Token has expired.") is ErrorCategory.CONFIG
    plan = plan_retry(ErrorCategory.CONFIG, attempts=1, max_attempts=8)
    assert plan.status == "failed" and not plan.retry


def test_transient_and_unknown() -> None:
    assert classify_flex_error("Flex SendRequest failed: <urlopen error [Errno 110] Connection timed out>") is ErrorCategory.TRANSIENT
    assert classify_flex_error("psycopg2.OperationalError: connection to server at x failed") is ErrorCategory.TRANSIENT
    assert classify_flex_error("Failed to write account_executions.") is ErrorCategory.UNKNOWN


def test_plan_delays_and_cooldown() -> None:
    policy = RetryPolicy(not_ready_delay_sec=1800, throttled_delay_sec=900, transient_delay_sec=60, unknown_delay_sec=300)
    nr = plan_retry(ErrorCategory.NOT_READY, attempts=1, max_attempts=8, policy=policy)
    assert nr.retry and nr.delay_sec == 1800 and nr.cooldown_all_sec == 0
    th = plan_retry(ErrorCategory.THROTTLED, attempts=1, max_attempts=8, policy=policy)
    assert th.retry and th.delay_sec == 900 and th.cooldown_all_sec == 900
    tr = plan_retry(ErrorCategory.TRANSIENT, attempts=1, max_attempts=8, policy=policy)
    assert tr.delay_sec == 60
    un = plan_retry(ErrorCategory.UNKNOWN, attempts=1, max_attempts=8, policy=policy)
    assert un.delay_sec == 300


def test_attempts_exhausted_is_final() -> None:
    plan = plan_retry(ErrorCategory.NOT_READY, attempts=8, max_attempts=8)
    assert plan.status == "failed"
    assert plan.category is ErrorCategory.NOT_READY


def test_policy_from_config_ignores_garbage_and_floors() -> None:
    p = RetryPolicy.from_config({"not_ready_delay_sec": "600", "throttled_delay_sec": "x", "transient_delay_sec": 5})
    assert p.not_ready_delay_sec == 600
    assert p.throttled_delay_sec == RetryPolicy().throttled_delay_sec
    assert p.transient_delay_sec == 30
