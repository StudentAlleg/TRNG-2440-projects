"""Part E -- turn Gold rows into sales-summary events.

Kept free of Spark so the event shape can be unit-tested against plain dicts.
"""

from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal
from typing import Any

EVENT_TYPE = "MONTHLY_CATEGORY_SALES_READY"


def build_event(row: dict[str, Any]) -> dict[str, Any]:
    """Map one monthly_category_sales row to the required event payload."""
    sales_month = str(row["order_month"])
    category = str(row["category"])
    return {
        "event_id": f"SALES-{sales_month}-{category}",
        "event_type": EVENT_TYPE,
        "sales_month": sales_month,
        "category": category,
        "order_count": int(row["order_count"]),
        "total_quantity": int(row["total_quantity"]),
        "total_revenue": _as_float(row["total_revenue"]),
    }


def build_events(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [build_event(r) for r in rows]


def _as_float(value: Any) -> float:
    # Spark hands back Decimal for decimal columns; JSON needs a number.
    if isinstance(value, Decimal):
        return float(round(value, 2))
    return round(float(value), 2)
