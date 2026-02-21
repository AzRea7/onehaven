# onehaven_decision_engine/backend/tests/test_cash_rollup_math.py
from __future__ import annotations

from datetime import date

from app.domain.cashflow import cashflow_rollup
from dataclasses import dataclass


@dataclass
class L:
    start_date: date
    end_date: date
    monthly_rent: float


@dataclass
class T:
    posted_date: date
    txn_type: str
    category: str
    amount: float


def test_cash_rollup_math():
    leases = [L(start_date=date(2026, 1, 1), end_date=date(2026, 12, 31), monthly_rent=1500.0)]
    txns = [
        T(posted_date=date(2026, 2, 5), txn_type="income", category="rent", amount=1500.0),
        T(posted_date=date(2026, 2, 10), txn_type="expense", category="repairs", amount=-200.0),
        T(posted_date=date(2026, 2, 12), txn_type="expense", category="utilities", amount=-100.0),
    ]
    r = cashflow_rollup(leases=leases, transactions=txns, month="2026-02")
    assert r.expected_rent == 1500.0
    assert r.collected_rent == 1500.0
    assert r.total_expenses == 300.0
    assert r.net_cashflow == 1200.0