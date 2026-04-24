# onehaven_decision_engine/backend/app/services/rent_comp_selection.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Property
from app.policy_models import RentComp


@dataclass(frozen=True)
class ScoredComp:
    comp: RentComp
    score: float


def _score(prop: Property, c: RentComp) -> float:
    """
    Deterministic similarity score (higher is better).
    v1 factors:
      - bedrooms exact match heavily weighted
      - city match
      - sqft proximity if present
    """
    score = 0.0
    pb = int(prop.bedrooms or 0)
    cb = int(getattr(c, "bedrooms", 0) or 0)
    if pb and cb:
        score += 10.0 if pb == cb else max(0.0, 10.0 - abs(pb - cb) * 3.0)

    pc = (prop.city or "").strip().lower()
    cc = (getattr(c, "city", None) or "").strip().lower()
    if pc and cc and pc == cc:
        score += 4.0

    psq = float(prop.square_feet or 0.0)
    csq = float(getattr(c, "square_feet", 0.0) or 0.0)
    if psq > 0 and csq > 0:
        diff = abs(psq - csq)
        score += max(0.0, 3.0 - (diff / 500.0))

    return score


def select_best_comps(db: Session, *, org_id: int, prop: Property, limit: int = 5) -> list[RentComp]:
    rows = db.scalars(select(RentComp).where(RentComp.org_id == org_id)).all()
    scored = [ScoredComp(comp=r, score=_score(prop, r)) for r in rows]
    scored.sort(key=lambda x: x.score, reverse=True)
    return [s.comp for s in scored[: max(0, int(limit))]]