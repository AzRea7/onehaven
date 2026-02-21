# onehaven_decision_engine/backend/app/domain/valuation_cadence.py
from __future__ import annotations

from datetime import date, timedelta


def next_valuation_due(as_of: date, cadence: str = "quarterly") -> date:
    c = (cadence or "quarterly").strip().lower()
    if c == "monthly":
        return as_of + timedelta(days=30)
    if c == "semiannual":
        return as_of + timedelta(days=182)
    if c == "annual":
        return as_of + timedelta(days=365)
    # default quarterly
    return as_of + timedelta(days=91)