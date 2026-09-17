"""IB's dateTime → exec_time: the time is kept on the local clock, a bare date is local midnight."""

from __future__ import annotations

from datetime import datetime, timezone

from bifrost_flex_query.client.flex_client import parse_flex_datetime, parse_trades_xml

TRADES_XML = """<?xml version="1.0" encoding="UTF-8"?>
<FlexQueryResponse queryName="Trades" type="AF">
<FlexStatements count="1">
<FlexStatement accountId="U111" fromDate="20260904" toDate="20260904" period="LastBusinessDay">
<Trades>
<Trade accountId="U111" symbol="NVDA" assetCategory="STK" dateTime="20260904;093012" tradeDate="20260904"
  buySell="BUY" quantity="100" tradePrice="180.25" tradeID="123" ibExecID="0000e1a7.66d9" currency="USD"/>
<Trade accountId="U111" symbol="AAPL" assetCategory="STK" dateTime="20260904" tradeDate="20260904"
  buySell="SELL" quantity="-10" tradePrice="230.10" tradeID="124" ibExecID="0000e1a7.66da" currency="USD"/>
</Trades>
</FlexStatement>
</FlexStatements>
</FlexQueryResponse>"""


def test_timed_value_is_read_on_the_local_clock() -> None:
    # 20:20 in New York (EDT, UTC−4) is 00:20 UTC the next day. Reading it as
    # UTC stored every Flex fill four hours early.
    dt, date_only = parse_flex_datetime("20260904;202000")
    assert dt == datetime(2026, 9, 5, 0, 20, 0, tzinfo=timezone.utc)
    assert date_only is False


def test_timed_value_follows_winter_time() -> None:
    # The 09:30 open in January is EST (UTC−5): 14:30 UTC, not 13:30.
    dt, _ = parse_flex_datetime("20260115;093000")
    assert dt == datetime(2026, 1, 15, 14, 30, 0, tzinfo=timezone.utc)


def test_a_regular_session_fill_lands_inside_the_utc_session() -> None:
    # The symptom that exposed the bug: no stored fill fell in 13:30–20:00 UTC.
    for raw in ("20260317;093001", "20260317;125508", "20260317;155959"):
        dt, _ = parse_flex_datetime(raw)
        assert dt is not None
        minutes = dt.hour * 60 + dt.minute
        assert 13 * 60 + 30 <= minutes < 20 * 60, raw


def test_bare_date_is_local_midnight_not_utc_midnight() -> None:
    dt, date_only = parse_flex_datetime("20260904")
    assert date_only is True
    # 2026-09-04 00:00 America/New_York (EDT) = 04:00 UTC — the same calendar day in both zones.
    assert dt == datetime(2026, 9, 4, 4, 0, tzinfo=timezone.utc)
    assert parse_flex_datetime("2026-09-04")[0] == dt
    assert parse_flex_datetime("")[0] is None


def test_trades_xml_rows_carry_the_time_ib_sent() -> None:
    rows = parse_trades_xml(TRADES_XML)
    by_symbol = {r["symbol"]: r for r in rows}
    timed = datetime.fromtimestamp(by_symbol["NVDA"]["time"], tz=timezone.utc)
    # 09:30:12 at the open in New York = 13:30:12 UTC.
    assert timed == datetime(2026, 9, 4, 13, 30, 12, tzinfo=timezone.utc)
    dated = datetime.fromtimestamp(by_symbol["AAPL"]["time"], tz=timezone.utc)
    assert dated == datetime(2026, 9, 4, 4, 0, tzinfo=timezone.utc)
