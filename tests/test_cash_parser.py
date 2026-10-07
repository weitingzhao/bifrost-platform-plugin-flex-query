"""parse_cash_transactions_xml (TD-115): the cash ledger's parser had no test.

Fixtures are made up (accounts, ids, symbols, amounts), in both shapes IB sends: every field as
an attribute (the usual Activity Flex template) and as child elements.
"""

from __future__ import annotations

from datetime import datetime, timezone

from bifrost_flex_query.client.flex_client import parse_cash_transactions_xml

ATTRIBUTE_STYLE = """<FlexQueryResponse queryName="Cash" type="AF">
<FlexStatements count="1">
<FlexStatement accountId="U0000009" fromDate="20260901" toDate="20260930">
<CashTransactions>
<CashTransaction currency="USD" fxRateToBase="1" assetCategory="STK" symbol="ZQY" conid="123456"
  securityID="US0000000009" securityIDType="ISIN" listingExchange="NASDAQ"
  description="ZQY(US0000000009) CASH DIVIDEND USD 0.10 PER SHARE (Ordinary Dividend)"
  dateTime="20260921;202000" settleDate="20260921" amount="12.34" type="Dividends" code=""
  transactionID="990001" reportDate="20260921" availableForTradingDate="20260921" />
<CashTransaction currency="USD" fxRateToBase="1" assetCategory="STK" symbol="ZQY" conid="123456"
  description="ZQY(US0000000009) CASH DIVIDEND - US TAX" dateTime="20260921;202000"
  amount="-1,234.50" type="Withholding Tax" transactionID="990002" reportDate="20260921" />
<CashTransaction currency="USD" description="ELECTRONIC FUND TRANSFER" dateTime="20260915"
  amount="5000" type="Deposits/Withdrawals" transactionID="990003" reportDate="20260915" />
<CashTransaction currency="USD" description="DISBURSEMENT" dateTime="20260916"
  amount="-200" type="Deposits/Withdrawals" transactionID="990004" reportDate="20260916" />
</CashTransactions>
</FlexStatement>
</FlexStatements>
</FlexQueryResponse>"""

ELEMENT_STYLE = """<FlexQueryResponse><FlexStatements><FlexStatement>
<CashTransactions>
<CashTransaction>
  <accountId>U0000008</accountId><currency>USD</currency><amount>-3.21</amount>
  <type>Broker Interest Paid</type><description>USD DEBIT INT FOR AUG-2026</description>
  <dateTime>2026-09-03</dateTime><transactionID>880001</transactionID><reportDate>20260903</reportDate>
</CashTransaction>
</CashTransactions>
</FlexStatement></FlexStatements></FlexQueryResponse>"""


def _utc(y: int, m: int, d: int) -> float:
    return datetime(y, m, d, tzinfo=timezone.utc).timestamp()


def test_attribute_style_rows() -> None:
    rows = parse_cash_transactions_xml(ATTRIBUTE_STYLE)
    assert [r["amount"] for r in rows] == [12.34, -1234.5, 5000.0, -200.0]
    div = rows[0]
    # The report-level accountId fills rows that carry none.
    assert div["account_id"] == "U0000009"
    # Date-only UTC on purpose: ts is part of raw_broker.transactions' UNIQUE key.
    assert div["ts"] == _utc(2026, 9, 21)
    assert div["type"] == "dividend"
    assert div["flex_type"] == "Dividends"
    assert div["currency"] == "USD"
    assert div["symbol"] == "ZQY" and div["conid"] == 123456
    assert div["security_id"] == "US0000000009" and div["security_id_type"] == "ISIN"
    assert div["listing_exchange"] == "NASDAQ"
    assert div["report_date"] == "20260921"
    assert div["available_for_trading_date"] == "20260921"
    assert div["fx_rate_to_base"] == 1.0
    # Every attribute is kept, the full dateTime and the id included.
    assert div["raw_extra"]["dateTime"] == "20260921;202000"
    assert div["raw_extra"]["transactionID"] == "990001"


def test_types_by_flex_type_and_sign() -> None:
    rows = parse_cash_transactions_xml(ATTRIBUTE_STYLE)
    assert [r["type"] for r in rows] == ["dividend", "other", "deposit", "withdrawal"]


def test_element_style_row() -> None:
    (row,) = parse_cash_transactions_xml(ELEMENT_STYLE)
    assert row["account_id"] == "U0000008"
    assert row["amount"] == -3.21
    assert row["ts"] == _utc(2026, 9, 3)
    assert row["flex_transaction_id"] == "880001"
    assert row["report_date"] == "20260903"
    assert row["description"] == "USD DEBIT INT FOR AUG-2026"


def test_unparseable_xml_is_no_rows() -> None:
    assert parse_cash_transactions_xml("<FlexQueryResponse><broken") == []


def test_row_without_account_and_amount_is_dropped() -> None:
    xml = "<FlexQueryResponse><CashTransaction type='Other Fees' dateTime='20260901' /></FlexQueryResponse>"
    assert parse_cash_transactions_xml(xml) == []


def test_attribute_transaction_id_is_kept() -> None:
    """TD-103: transactionID is an attribute on CashTransaction, not only a child element."""
    rows = parse_cash_transactions_xml(ATTRIBUTE_STYLE)
    assert [r.get("flex_transaction_id") for r in rows] == ["990001", "990002", "990003", "990004"]
