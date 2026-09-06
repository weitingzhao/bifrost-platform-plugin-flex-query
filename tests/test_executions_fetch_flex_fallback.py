"""Flex Trades fetch: IB 1003 date-range → query-default / period=5 fallback."""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from bifrost_flex_query.orchestration.trades import (
    _fetch_trades_with_date_fallback,
    _is_flex_statement_unavailable,
    fetch_flex_trades_and_upsert_executions,
)


def test_is_flex_statement_unavailable() -> None:
    assert _is_flex_statement_unavailable(
        ValueError("Flex request failed: [1003] Statement is not available.")
    )
    assert not _is_flex_statement_unavailable(ValueError("Flex request failed: [1015] Token is invalid."))


def test_scheduled_run_does_not_widen_on_1003() -> None:
    """Without fallback a 'statement not available' is raised as-is: the worker defers, IB is not hammered."""
    calls: List[Dict[str, Any]] = []

    def fake_fetch(token: str, query_id: str, from_date=None, to_date=None, period=None):
        calls.append({"from_date": from_date, "period": period})
        raise ValueError("Flex request failed: [1003] Statement is not available.")

    with patch("bifrost_flex_query.orchestration.trades.fetch_trades", side_effect=fake_fetch):
        with pytest.raises(ValueError, match=r"\[1003\]"):
            _fetch_trades_with_date_fallback("tok", "1428383", from_date="20260905", to_date="20260906", allow_fallback=False)
    assert len(calls) == 1


def test_scheduled_run_accepts_an_empty_window() -> None:
    with patch("bifrost_flex_query.orchestration.trades.fetch_trades", return_value=[]) as ft:
        rows, used, kind = _fetch_trades_with_date_fallback("tok", "q", from_date="20260905", to_date="20260906", allow_fallback=False)
    assert rows == [] and used is False and kind is None
    assert ft.call_count == 1


def test_date_range_1003_falls_back_to_query_default() -> None:
    calls: List[Dict[str, Any]] = []

    def fake_fetch(
        token: str,
        query_id: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        period: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        calls.append(
            {"from_date": from_date, "to_date": to_date, "period": period}
        )
        if from_date and to_date:
            raise ValueError("Flex request failed: [1003] Statement is not available.")
        if period is not None:
            return []
        return [{"account_id": "U1", "symbol": "AAPL", "source": "flex_trades"}]

    with patch(
        "bifrost_flex_query.orchestration.trades.fetch_trades",
        side_effect=fake_fetch,
    ):
        rows, used_fallback, kind = _fetch_trades_with_date_fallback(
            "tok",
            "1428383",
            from_date="20260705",
            to_date="20260807",
        )

    assert len(rows) == 1
    assert used_fallback is True
    assert kind == "query_default_after_1003"
    assert calls[0]["from_date"] == "20260705"
    assert calls[1]["from_date"] is None and calls[1]["period"] is None


def test_empty_then_period5() -> None:
    def fake_fetch(
        token: str,
        query_id: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        period: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        if period == 5:
            return [{"account_id": "U1", "symbol": "MSFT", "source": "flex_trades"}]
        return []

    with patch(
        "bifrost_flex_query.orchestration.trades.fetch_trades",
        side_effect=fake_fetch,
    ):
        rows, used_fallback, kind = _fetch_trades_with_date_fallback(
            "tok",
            "1",
            from_date="20260701",
            to_date="20260731",
        )

    assert len(rows) == 1
    assert used_fallback is True
    assert kind == "period5_after_empty_or_1003"


def test_non_1003_date_error_propagates() -> None:
    with patch(
        "bifrost_flex_query.orchestration.trades.fetch_trades",
        side_effect=ValueError("Flex request failed: [1015] Token is invalid."),
    ):
        with pytest.raises(ValueError, match=r"\[1015\]"):
            _fetch_trades_with_date_fallback(
                "tok", "1", from_date="20260701", to_date="20260731"
            )


def test_fetch_flex_service_uses_fallback_on_1003() -> None:
    cfg = {"sink": "postgres", "postgres": {"host": "x"}}

    def fake_fetch(
        token: str,
        query_id: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        period: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        if from_date and to_date:
            raise ValueError("Flex request failed: [1003] Statement is not available.")
        return [
            {
                "account_id": "U1",
                "time": 1.0,
                "symbol": "AAPL",
                "sec_type": "STK",
                "side": "BUY",
                "quantity": 1.0,
                "price": 1.0,
                "source": "flex_trades",
            }
        ]

    with (
        patch(
            "bifrost_flex_query.orchestration.trades.open_trade_conn",
            return_value=MagicMock(),
        ),
        patch(
            "bifrost_flex_query.orchestration.trades.get_flex_config",
            return_value=[
                {"token": "t", "query_id": "1428383", "role": "host", "query_label": "Trades"}
            ],
        ),
        patch(
            "bifrost_flex_query.orchestration.trades.get_flex_executions_stats",
            return_value={"count": 10, "max_date": None},
        ),
        patch(
            "bifrost_flex_query.orchestration.trades.get_flex_range_days",
            return_value=(30, 270),
        ),
        patch(
            "bifrost_flex_query.orchestration.trades.fetch_trades",
            side_effect=fake_fetch,
        ),
        patch(
            "bifrost_flex_query.orchestration.trades.write_account_executions_to_db",
            return_value=True,
        ),
        patch(
            "bifrost_flex_query.orchestration.trades.rows_span",
            return_value=("2026-08-01", "2026-08-04"),
        ),
    ):
        out = fetch_flex_trades_and_upsert_executions(cfg, {})

    assert out["ok"] is True
    assert out["count"] == 1
    assert out["per_query"][0]["used_fallback"] is True
    assert out["per_query"][0]["fallback_kind"] == "query_default_after_1003"
