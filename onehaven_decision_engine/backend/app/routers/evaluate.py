# onehaven_decision_engine/backend/app/routers/evaluate.py
from __future__ import annotations

import json
from typing import Optional, List

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import Deal, Property, RentAssumption, UnderwritingResult
from ..schemas import BatchEvalOut, UnderwritingResultOut
from ..domain.decision_engine import score_and_decide
from ..domain.underwriting import underwrite

router = APIRouter(prefix="/evaluate", tags=["evaluate"])


def _inventory_proxy(db: Session, snapshot_id: int, city: str, state: str) -> int:
    # Simple proxy: count of deals in same city/state for this snapshot
    # (acts like “inventory density” until you plug real MLS/market counts)
    q = (
        select(Deal)
        .join(Property, Property.id == Deal.property_id)
        .where(Deal.snapshot_id == snapshot_id)
        .where(Property.city == city)
        .where(Property.state == state)
    )
    return len(db.scalars(q).all())


def _gross_rent_used(d: Deal, p: Property, ra: Optional[RentAssumption]) -> tuple[float, bool]:
    """
    Picks the best available rent for underwriting.
    Priority:
      1) market_rent_estimate
      2) section8_fmr
      3) 1.3% rule estimate (price * 0.013)
    Returns: (rent, estimated_flag)
    """
    if ra is not None:
        if ra.market_rent_estimate is not None and ra.market_rent_estimate > 0:
            return float(ra.market_rent_estimate), False
        if ra.section8_fmr is not None and ra.section8_fmr > 0:
            return float(ra.section8_fmr), False

    # fallback estimate
    asking = float(d.asking_price or 0.0)
    est = asking * 0.013
    return float(est), True


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


# ---- Compatibility aliases for your curl workflow ----

@router.post("/run", response_model=BatchEvalOut)
def evaluate_run(snapshot_id: int = Query(...), db: Session = Depends(get_db)):
    # allows: POST /evaluate/run?snapshot_id=4
    return evaluate_snapshot(snapshot_id=snapshot_id, db=db)


@router.get("/results", response_model=List[UnderwritingResultOut])
def evaluate_results(
    decision: Optional[str] = Query(None, description="PASS|REVIEW|REJECT"),
    snapshot_id: Optional[int] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
):
    q = select(UnderwritingResult).join(Deal, Deal.id == UnderwritingResult.deal_id)
    if snapshot_id is not None:
        q = q.where(Deal.snapshot_id == snapshot_id)
    if decision is not None:
        q = q.where(UnderwritingResult.decision == decision)

    rows = db.scalars(q.limit(limit)).all()
    return [UnderwritingResultOut.model_validate(r) for r in rows]
