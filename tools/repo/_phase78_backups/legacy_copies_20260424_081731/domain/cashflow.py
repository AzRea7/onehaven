# onehaven_decision_engine/backend/app/domain/cashflow.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import calendar


def month_bounds(yyyy_mm: str) -> tuple[date, date]:
    y, m = [int(x) for x in yyyy_mm.split("-")]
    last_day = calendar.monthrange(y, m)[1]
    return date(y, m, 1), date(y, m, last_day)


@dataclass(frozen=True)
class CashRollup:
    month: str
    expected_rent: float
    collected_rent: float
    expenses_by_category: dict[str, float]
    total_expenses: float
    net_cashflow: float


def expected_rent_for_month(leases: list[Any], yyyy_mm: str) -> float:
    start, end = month_bounds(yyyy_mm)
    total = 0.0
    for l in leases:
        # naive: if lease overlaps month, count monthly_rent
        ls = getattr(l, "start_date", None)
        le = getattr(l, "end_date", None)
        mr = float(getattr(l, "monthly_rent", 0.0) or 0.0)
        if ls is None:
            continue
        if le is None:
            # open ended
            if ls <= end:
                total += mr
        else:
            if ls <= end and le >= start:
                total += mr
    return float(total)


def rollup_transactions(txns: list[Any]) -> tuple[float, dict[str, float]]:
    collected = 0.0
    expenses: dict[str, float] = {}
    for t in txns:
        amt = float(getattr(t, "amount", 0.0) or 0.0)
        ttype = (getattr(t, "txn_type", "") or "").lower()
        cat = (getattr(t, "category", "uncategorized") or "uncategorized").lower()

        if ttype in {"income", "rent"}:
            collected += max(0.0, amt)
        else:
            expenses[cat] = float(expenses.get(cat, 0.0) + abs(amt))
    return float(collected), expenses


def cashflow_rollup(
    *,
    leases: list[Any],
    transactions: list[Any],
    month: str,
) -> CashRollup:
    expected = expected_rent_for_month(leases, month)
    collected, exp_by_cat = rollup_transactions(transactions)
    total_exp = float(sum(exp_by_cat.values()))
    net = float(collected - total_exp)
    return CashRollup(
        month=str(month),
        expected_rent=float(expected),
        collected_rent=float(collected),
        expenses_by_category=exp_by_cat,
        total_expenses=float(total_exp),
        net_cashflow=float(net),
    )