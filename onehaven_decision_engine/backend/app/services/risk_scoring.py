# onehaven_decision_engine/backend/app/services/risk_scoring.py
from __future__ import annotations

from typing import Optional, Tuple

RISK_VERSION = "v0_stub"


def compute_crime_score(*, lat: float, lng: float) -> Tuple[Optional[float], dict]:
    """
    Stub scoring:
    - Replace with Detroit incident dataset + radius counting.
    - Return (crime_score, debug_meta)
    """
    # Deterministic placeholder: score grows as you move toward downtown-ish (not real)
    score = min(100.0, max(0.0, 50.0))
    return score, {"version": RISK_VERSION, "note": "stub scorer - replace with dataset"}


def compute_offender_count(*, lat: float, lng: float) -> Tuple[Optional[int], dict]:
    """
    Stub:
    - Replace with registry/API + spatial index.
    """
    return 0, {"version": RISK_VERSION, "note": "stub scorer - replace with dataset"}