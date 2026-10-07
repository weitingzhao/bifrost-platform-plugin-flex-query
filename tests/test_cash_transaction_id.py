"""TD-103: attribute-only CashTransaction rows keep IB's transactionID.

Made-up account and amounts. Two same-day same-amount rows must stay two dicts.
A row with no date is skipped (it used to be inserted at now() on every run).
"""

from __future__ import annotations

from bifrost_flex_query.client.flex_client import parse_cash_transactions_xml

_XML = """<?xml version="1.0" encoding="UTF-8"?>
<FlexQueryResponse queryName="Cash" type="AF">
<FlexStatements count="1">
<FlexStatement accountId="U00011111" fromDate="20261001" toDate="20261001">
<CashTransactions>
<CashTransaction accountId="U00011111" dateTime="20261001;120000" settleDate="20261001"
  amount="-1.23" type="Withholding Tax" code="WHD" currency="USD"
  description="fee A" transactionID="900001" reportDate="20261001"/>
<CashTransaction accountId="U00011111" dateTime="20261001;153000" settleDate="20261001"
  amount="-1.23" type="Withholding Tax" code="WHD" currency="USD"
  description="fee B" transactionID="900002" reportDate="20261001"/>
<CashTransaction accountId="U00011111" amount="-1.23" type="Withholding Tax"
  transactionID="900003" reportDate="20261001"/>
</CashTransactions>
</FlexStatement>
</FlexStatements>
</FlexQueryResponse>"""


def test_attribute_transaction_id_is_kept_and_dateless_rows_are_skipped() -> None:
    rows = parse_cash_transactions_xml(_XML)
    assert len(rows) == 2
    ids = [r["flex_transaction_id"] for r in rows]
    assert ids == ["900001", "900002"]
    assert rows[0]["amount"] == rows[1]["amount"] == -1.23
    assert rows[0]["description"] != rows[1]["description"]
    assert all(r["account_id"] == "U00011111" for r in rows)
