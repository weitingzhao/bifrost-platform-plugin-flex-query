"""IB Flex HTTPS + XML client."""

from bifrost_flex_query.client.flex_client import (
    fetch_cash_transactions,
    fetch_trades,
    parse_cash_transactions_xml,
    parse_trades_xml,
)

__all__ = [
    "fetch_cash_transactions",
    "fetch_trades",
    "parse_cash_transactions_xml",
    "parse_trades_xml",
]
