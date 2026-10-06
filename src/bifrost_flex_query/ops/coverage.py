"""Cash-ledger coverage: months where an account traded but no cash row landed.

Fees, interest and dividends post as cash transactions month after month, so a
month with Flex executions and no cash rows means the cash-transactions ingest
missed it (TD-88: 2026-03..07 went missing for five months while every run
reported ok). The current month is left out: its cash rows may not have posted yet.
"""

from __future__ import annotations

from typing import Any, Mapping

COVERAGE_SQL = """
WITH e AS (
    SELECT account_id, date_trunc('month', trade_date)::date AS month, count(*)::int AS executions
    FROM raw_broker.executions_raw_flex
    WHERE account_id IS NOT NULL AND trade_date IS NOT NULL
      AND trade_date < date_trunc('month', (now() AT TIME ZONE 'UTC'))::date
    GROUP BY 1, 2
), t AS (
    SELECT DISTINCT account_id, date_trunc('month', ts AT TIME ZONE 'UTC')::date AS month
    FROM raw_broker.transactions
)
SELECT e.account_id, e.month, e.executions, (t.month IS NULL) AS gap
FROM e LEFT JOIN t USING (account_id, month)
ORDER BY e.month, e.account_id
"""


def _row(r: Any) -> tuple[Any, Any, Any, Any]:
    if isinstance(r, Mapping):
        return r["account_id"], r["month"], r["executions"], r["gap"]
    return r[0], r[1], r[2], r[3]


def read_coverage(conn: Any) -> dict[str, Any]:
    """{accounts: [...], gaps: [{account_id, month: 'YYYY-MM', executions}]}.

    ``accounts`` lists every account with Flex executions in a closed month, so a
    metric can report 0 for an account that is covered rather than nothing.
    """
    with conn.cursor() as cur:
        cur.execute(COVERAGE_SQL)
        rows = cur.fetchall() or []
    conn.rollback()
    accounts: list[str] = []
    gaps: list[dict[str, Any]] = []
    for r in rows:
        acct, month, n, gap = _row(r)
        acct = str(acct)
        if acct not in accounts:
            accounts.append(acct)
        if gap:
            label = month.strftime("%Y-%m") if hasattr(month, "strftime") else str(month)[:7]
            gaps.append({"account_id": acct, "month": label, "executions": int(n or 0)})
    return {"accounts": sorted(accounts), "gaps": gaps}


def gap_months_by_account(coverage: Mapping[str, Any]) -> dict[str, int]:
    out = {str(a): 0 for a in coverage.get("accounts") or []}
    for g in coverage.get("gaps") or []:
        out[str(g["account_id"])] = out.get(str(g["account_id"]), 0) + 1
    return out


def coverage_check(coverage: Mapping[str, Any] | None, error: str | None = None) -> dict[str, Any]:
    """The ``coverage`` entry of /flex/ops/check."""
    if error:
        return {"id": "coverage", "ok": False, "detail": f"Cash-ledger coverage unreadable: {error}"}
    cov = coverage or {}
    gaps = list(cov.get("gaps") or [])
    if not gaps:
        n = len(cov.get("accounts") or [])
        return {
            "id": "coverage",
            "ok": True,
            "detail": f"Cash transactions present in every closed month with executions ({n} account(s)).",
        }
    by_acct: dict[str, list[str]] = {}
    for g in gaps:
        by_acct.setdefault(str(g["account_id"]), []).append(str(g["month"]))
    parts = [f"{a}: {', '.join(ms)}" for a, ms in sorted(by_acct.items())]
    return {
        "id": "coverage",
        "ok": False,
        "detail": (
            f"{len(gaps)} account-month(s) have executions but no cash transactions — "
            + "; ".join(parts)
            + ". Backfill with a flex-transactions job whose payload sets from_date/to_date."
        ),
        "gaps": gaps,
    }
