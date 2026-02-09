# backend/app/routers/evaluate.py
from __future__ import annotations

import json
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, func
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import Deal, Property, RentAssumption, UnderwritingResult
from ..schemas import BatchEvalOut, SurvivorOut
from ..domain.decision_engine import score_and_decide
from ..domain.underwriting import underwrite

router = APIRouter(prefix="/evaluate", tags=["evaluate"])


def _inventory_proxy(db: Session, snapshot_id: int, city: str, state: str) -> int:
    cnt = db.scalar(
        select(func.count(Deal.id))
        .join(Property, Property.id == Deal.property_id)
        .where(Deal.snapshot_id == snapshot_id)
        .where(Property.city == city)
        .where(Property.state == state)
    )
    return int(cnt or 0)


def _gross_rent_used(d: Deal, p: Property, ra: Optional[RentAssumption]) -> tuple[float, bool]:
    """
    Returns (gross_rent_used, was_estimated).
    Priority:
      1) approved_rent_ceiling
      2) rent_reasonableness_comp
      3) section8_fmr
      4) market_rent_estimate
      5) fallback estimate from 1.3% rule
    """
    candidates = []
    if ra:
        for v in [ra.approved_rent_ceiling, ra.rent_reasonableness_comp, ra.section8_fmr, ra.market_rent_estimate]:
            if v is not None and v > 0:
                candidates.append(float(v))

    if candidates:
        return min(candidates), False

    # Fallback: 1.3% rule rent estimate
    if d.asking_price and d.asking_price > 0:
        return float(d.asking_price) * 0.013, True

    return 0.0, True


@router.post("/snapshot/{snapshot_id}", response_model=BatchEvalOut)
def evaluate_snapshot(snapshot_id: int, db: Session = Depends(get_db)):
    deals = db.scalars(select(Deal).where(Deal.snapshot_id == snapshot_id)).all()

    pass_count = 0
    review_count = 0
    reject_count = 0

    for d in deals:
        p = db.scalar(select(Property).where(Property.id == d.property_id))
        if not p:
            continue

        ra = db.scalar(select(RentAssumption).where(RentAssumption.property_id == p.id))

        # Ensure inventory proxy exists even if not filled yet
        inv = None
        if ra and ra.inventory_count is not None:
            inv = ra.inventory_count
        else:
            inv = _inventory_proxy(db, snapshot_id, p.city, p.state)
            if ra is None:
                ra = RentAssumption(property_id=p.id)
                db.add(ra)
            if ra.inventory_count is None:
                ra.inventory_count = inv
                db.commit()

        gross_rent, estimated = _gross_rent_used(d, p, ra)

        # Underwrite (math layer)
        uw = underwrite(
            asking_price=float(d.asking_price),
            down_payment_pct=float(d.down_payment_pct or 0.20),
            interest_rate=float(d.interest_rate or 0.07),
            term_years=int(d.term_years or 30),
            gross_rent=float(gross_rent),
            rehab_estimate=float(d.rehab_estimate or 0.0),
        )

        # Decision engine (rules/scoring)
        decision, score, reasons = score_and_decide(
            property=p,
            deal=d,
            rent_assumption=ra,
            underwriting=uw,
        )

        # If rent was estimated, never allow PASS (force REVIEW)
        if estimated and decision == "PASS":
            decision = "REVIEW"
            reasons.append("Rent was estimated from 1.3% rule; verify with comps/FMR before PASS")

        # Store result (upsert)
        existing = db.scalar(select(UnderwritingResult).where(UnderwritingResult.deal_id == d.id))
        if not existing:
            existing = UnderwritingResult(deal_id=d.id)
            db.add(existing)

        existing.decision = decision
        existing.score = int(score)
        existing.reasons_json = json.dumps(reasons)

        existing.gross_rent_used = float(gross_rent)
        existing.mortgage_payment = float(uw.mortgage_payment)
        existing.operating_expenses = float(uw.operating_expenses)
        existing.noi = float(uw.noi)
        existing.cash_flow = float(uw.cash_flow)
        existing.dscr = float(uw.dscr)
        existing.cash_on_cash = float(uw.cash_on_cash)
        existing.break_even_rent = float(uw.break_even_rent)
        existing.min_rent_for_target_roi = float(uw.min_rent_for_target_roi)

        db.commit()

        if decision == "PASS":
            pass_count += 1
        elif decision == "REVIEW":
            review_count += 1
        else:
            reject_count += 1

    return BatchEvalOut(
        snapshot_id=snapshot_id,
        total_deals=len(deals),
        pass_count=pass_count,
        review_count=review_count,
        reject_count=reject_count,
    )
