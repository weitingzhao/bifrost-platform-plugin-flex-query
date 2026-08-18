"""Read-only peek of recent brokerage ingest rows."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query

from bifrost_flex_query.api.deps import db_conn

router = APIRouter(prefix="/flex/coverage", tags=["coverage"])

TableName = Literal["executions_raw_flex", "transactions"]

_PEEK: dict[str, tuple[str, str, tuple[str, ...]]] = {
    "executions_raw_flex": (
        "brokerage.executions_raw_flex",
        "exec_time",
        (
            "exec_time",
            "account_id",
            "contract_key",
            "side",
            "quantity",
            "price",
            "exec_id",
            "source",
        ),
    ),
    "transactions": (
        "brokerage.transactions",
        "ts",
        ("ts", "account_id", "amount", "type", "description", "currency", "symbol"),
    ),
}


def _cell(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (bytes, memoryview)):
        return bytes(value).decode("utf-8", errors="replace")
    return value


@router.get("/raw-peek")
def raw_peek(
    table: TableName = Query(...),
    limit: int = Query(default=20, ge=1, le=100),
    conn: Any = Depends(db_conn),
) -> dict[str, Any]:
    spec = _PEEK.get(table)
    if spec is None:
        raise HTTPException(status_code=422, detail="unsupported table")
    fq, order_col, columns = spec
    cols_sql = ", ".join(columns)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {cols_sql} FROM {fq} ORDER BY {order_col} DESC NULLS LAST LIMIT %s",
                (limit,),
            )
            fetched = cur.fetchall() or []
    except Exception as exc:
        conn.rollback()
        raise HTTPException(status_code=503, detail=f"raw peek failed: {exc}") from exc

    rows: list[list[Any]] = []
    for rec in fetched:
        if isinstance(rec, dict):
            rows.append([_cell(rec.get(c)) for c in columns])
        else:
            rows.append([_cell(v) for v in rec])
    return {
        "table": table,
        "row_count": len(rows),
        "columns": list(columns),
        "rows": rows,
    }
