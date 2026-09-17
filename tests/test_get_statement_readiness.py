"""GetStatement: an unready report is polled again, never parsed as an empty window.

2026-09-16: a first trigger for a new window returned "No trades in Flex report."
within a second and the identical retry upserted 328 rows. GetStatement answers
1019 "Statement generation in progress" with Status Warn, and only Fail was
treated as not ready. All XML below is invented.
"""

from __future__ import annotations

from typing import List
from unittest.mock import MagicMock, patch

import pytest

from bifrost_flex_query.client import flex_client
from bifrost_flex_query.client.flex_client import fetch_trades, get_statement
from bifrost_flex_query.orchestration.trades import fetch_flex_trades_and_upsert_executions
from bifrost_flex_query.worker.retry import ErrorCategory, classify_flex_error

SEND_REQUEST_OK = """<?xml version="1.0" encoding="UTF-8"?>
<FlexStatementResponse timestamp="16 September, 2026 06:31 AM EDT">
<Status>Success</Status>
<ReferenceCode>9990001112</ReferenceCode>
<Url>https://example.invalid/GetStatement</Url>
</FlexStatementResponse>"""

GENERATION_IN_PROGRESS = """<?xml version="1.0" encoding="UTF-8"?>
<FlexStatementResponse timestamp="16 September, 2026 06:31 AM EDT">
<Status>Warn</Status>
<ErrorCode>1019</ErrorCode>
<ErrorMessage>Statement generation in progress. Please try again shortly.</ErrorMessage>
</FlexStatementResponse>"""

INCOMPLETE = """<?xml version="1.0" encoding="UTF-8"?>
<FlexStatementResponse timestamp="16 September, 2026 06:31 AM EDT">
<Status>Fail</Status>
<ErrorCode>1004</ErrorCode>
<ErrorMessage>Statement is incomplete at this time. Please try again shortly.</ErrorMessage>
</FlexStatementResponse>"""

NO_STATEMENT = """<?xml version="1.0" encoding="UTF-8"?>
<FlexQueryResponse queryName="Invented Trades" type="AF">
<FlexStatements count="0">
</FlexStatements>
</FlexQueryResponse>"""

HTML_PAGE = "<html><body>Service temporarily unavailable</body></html>"

NOT_XML = "Service temporarily unavailable"

READY_WITH_TRADES = """<?xml version="1.0" encoding="UTF-8"?>
<FlexQueryResponse queryName="Invented Trades" type="AF">
<FlexStatements count="1">
<FlexStatement accountId="U0000001" fromDate="20260301" toDate="20260915" period="" whenGenerated="20260916;063140">
<Trades>
<Trade accountId="U0000001" symbol="XYZ" assetCategory="STK" dateTime="20260310;101500" tradeDate="20260310"
  buySell="BUY" quantity="10" tradePrice="50.00" tradeID="1001" ibExecID="0000aaaa.0001" currency="USD"/>
<Trade accountId="U0000001" symbol="XYZ" assetCategory="STK" dateTime="20260311;141500" tradeDate="20260311"
  buySell="SELL" quantity="-10" tradePrice="51.00" tradeID="1002" ibExecID="0000aaaa.0002" currency="USD"/>
</Trades>
</FlexStatement>
</FlexStatements>
</FlexQueryResponse>"""

EMPTY_WINDOW = """<?xml version="1.0" encoding="UTF-8"?>
<FlexQueryResponse queryName="Invented Trades" type="AF">
<FlexStatements count="1">
<FlexStatement accountId="U0000001" fromDate="20260801" toDate="20260802" period="" whenGenerated="20260916;063140">
<Trades />
</FlexStatement>
</FlexStatements>
</FlexQueryResponse>"""

EMPTY_WINDOW_NO_SECTION = """<?xml version="1.0" encoding="UTF-8"?>
<FlexQueryResponse queryName="Invented Trades" type="AF">
<FlexStatements count="1">
<FlexStatement accountId="U0000001" fromDate="20260801" toDate="20260802" period="" whenGenerated="20260916;063140">
</FlexStatement>
</FlexStatements>
</FlexQueryResponse>"""


class _Resp:
    def __init__(self, body: str) -> None:
        self._body = body.encode("utf-8")

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "_Resp":
        return self

    def __exit__(self, *exc: object) -> bool:
        return False


class FakeFlex:
    """Answers SendRequest with a reference code and GetStatement from a script."""

    def __init__(self, statements: List[str]) -> None:
        self.statements = list(statements)
        self.send_requests = 0
        self.get_statements = 0

    def __call__(self, req, timeout=None) -> _Resp:
        if "SendRequest" in req.full_url:
            self.send_requests += 1
            return _Resp(SEND_REQUEST_OK)
        self.get_statements += 1
        # The last scripted answer repeats: IB keeps saying the same thing.
        body = self.statements.pop(0) if len(self.statements) > 1 else self.statements[0]
        return _Resp(body)


@pytest.fixture
def no_sleep():
    with patch("bifrost_flex_query.client.flex_client.time.sleep") as sleep:
        yield sleep


@pytest.mark.parametrize(
    "unready",
    [GENERATION_IN_PROGRESS, INCOMPLETE, NO_STATEMENT, HTML_PAGE, NOT_XML],
    ids=["warn-1019", "fail-1004", "no-flexstatement", "html", "not-xml"],
)
def test_unready_answer_is_polled_until_the_report_arrives(unready: str, no_sleep: MagicMock) -> None:
    fake = FakeFlex([unready, READY_WITH_TRADES])
    with patch("bifrost_flex_query.client.flex_client.urlopen", side_effect=fake):
        body = get_statement("tok", "9990001112")
    assert body == READY_WITH_TRADES
    assert fake.get_statements == 2
    assert no_sleep.call_count == 1


def test_fetch_trades_waits_for_generation_instead_of_returning_empty(no_sleep: MagicMock) -> None:
    """The 2026-09-16 shape: Warn 1019 first, the full report on the next poll."""
    fake = FakeFlex([GENERATION_IN_PROGRESS, GENERATION_IN_PROGRESS, READY_WITH_TRADES])
    with patch("bifrost_flex_query.client.flex_client.urlopen", side_effect=fake):
        rows = fetch_trades("tok", "q1", from_date="20260301", to_date="20260915")
    assert [r["trade_id"] for r in rows] == ["1001", "1002"]
    assert fake.send_requests == 1  # polling reuses the reference code; no new SendRequest
    assert fake.get_statements == 3


def test_never_ready_raises_within_the_poll_budget_as_not_ready(no_sleep: MagicMock) -> None:
    fake = FakeFlex([GENERATION_IN_PROGRESS])
    with patch("bifrost_flex_query.client.flex_client.urlopen", side_effect=fake):
        with pytest.raises(ValueError, match=r"\[1019\]") as exc:
            fetch_trades("tok", "q1", from_date="20260301", to_date="20260915")
    assert fake.send_requests == 1
    assert fake.get_statements == flex_client.MAX_GET_STATEMENT_POLLS
    # The worker defers a not-ready job; it does not record an empty success.
    assert classify_flex_error(str(exc.value)) is ErrorCategory.NOT_READY


@pytest.mark.parametrize("empty", [EMPTY_WINDOW, EMPTY_WINDOW_NO_SECTION], ids=["empty-trades", "no-trades-section"])
def test_generated_statement_with_no_trades_is_an_empty_window(empty: str, no_sleep: MagicMock) -> None:
    fake = FakeFlex([empty])
    with patch("bifrost_flex_query.client.flex_client.urlopen", side_effect=fake):
        rows = fetch_trades("tok", "q1", from_date="20260801", to_date="20260802")
    assert rows == []
    assert fake.get_statements == 1
    assert no_sleep.call_count == 0


def _run_ingest(fake: FakeFlex, body: dict) -> dict:
    cfg = {"sink": "postgres", "postgres": {"host": "x"}}
    with (
        patch("bifrost_flex_query.client.flex_client.urlopen", side_effect=fake),
        patch("bifrost_flex_query.orchestration.trades.open_trade_conn", return_value=MagicMock()),
        patch(
            "bifrost_flex_query.orchestration.trades.get_flex_config",
            return_value=[{"token": "t", "query_id": "q1", "role": "host", "query_label": "Trades"}],
        ),
        patch("bifrost_flex_query.orchestration.trades.get_flex_executions_stats", return_value={"count": 0}),
        patch("bifrost_flex_query.orchestration.trades.write_account_executions_to_db", return_value=True),
        patch("bifrost_flex_query.orchestration.trades.publish_flex_executions_system_message"),
    ):
        return fetch_flex_trades_and_upsert_executions(cfg, body)


def test_ingest_of_a_genuinely_empty_window_is_ok_with_zero(no_sleep: MagicMock) -> None:
    fake = FakeFlex([EMPTY_WINDOW])
    out = _run_ingest(fake, {"from_date": "20260801", "to_date": "20260802", "fallback": False})
    assert out["ok"] is True
    assert out["count"] == 0
    assert out["message"] == "No trades in Flex report."
    assert fake.send_requests == 1 and fake.get_statements == 1


def test_ingest_of_an_unready_report_is_not_an_empty_success(no_sleep: MagicMock) -> None:
    fake = FakeFlex([GENERATION_IN_PROGRESS])
    out = _run_ingest(fake, {"from_date": "20260301", "to_date": "20260915", "fallback": False})
    assert out["ok"] is False
    assert out["count"] == 0
    assert "[1019]" in out["error"]
    assert fake.send_requests == 1  # no widened fallback request
