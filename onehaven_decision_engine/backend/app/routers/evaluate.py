from __future__ import annotations

import json
from typing import Optional, List, Tuple

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import Deal, Property, RentAssumption, UnderwritingResult
from ..schemas import BatchEvalOut, UnderwritingResultOut
from ..domain.decision_engine import score_and_decide
from ..domain.underwriting import underwrite

router = APIRouter(prefix="/evaluate", tags=["evaluate"])


def _rent_used_for_underwriting(
    *,
    strategy: str,
    d: Deal,
    ra: Optional[RentAssumption],
) -> Tuple[float, bool, list[str]]:
    """
    Picks rent to use for underwriting + returns whether it was estimated.

    Goals:
      - Strategy-aware rent selection.
      - Section 8: cap by approved_rent_ceiling (safe) when available.
      - Market: use market_rent_estimate only (no cap).
      - If missing data, fall back to 1.3% heuristic => mark estimated=True.
    Returns: (rent_used, estimated_flag, notes)
    """
    notes: list[str] = []
    strategy = (strategy or "section8").strip().lower()

    market = None
    ceiling = None

    if ra is not None:
        try:
            if ra.market_rent_estimate is not None and float(ra.market_rent_estimate) > 0:
                market = float(ra.market_rent_estimate)
        except Exception:
            market = None

        # Prefer approved_rent_ceiling. If not present, compute a conservative fallback.
        try:
            if ra.approved_rent_ceiling is not None and float(ra.approved_rent_ceiling) > 0:
                ceiling = float(ra.approved_rent_ceiling)
            else:
                candidates: list[float] = []
                if ra.section8_fmr is not None and float(ra.section8_fmr) > 0:
                    candidates.append(float(ra.section8_fmr))
                if ra.rent_reasonableness_comp is not None and float(ra.rent_reasonableness_comp) > 0:
                    candidates.append(float(ra.rent_reasonableness_comp))
                ceiling = min(candidates) if candidates else None
        except Exception:
            ceiling = None

    # Strategy selection
    if strategy == "market":
        if market is not None:
            return market, False, notes
        # fallback
        asking = float(d.asking_price or 0.0)
        est = asking * 0.013
        notes.append("Market strategy missing market rent; fell back to 1.3% heuristic")
        return float(est), True, notes

    # section8 default
    if market is None and ceiling is None:
        asking = float(d.asking_price or 0.0)
        est = asking * 0.013
        notes.append("Section 8 strategy missing market rent and ceiling; fell back to 1.3% heuristic")
        return float(est), True, notes

    if market is None:
        notes.append("Section 8 strategy missing market rent; using ceiling only")
        return float(ceiling), False, notes

    if ceiling is None:
        notes.append("Section 8 strategy missing ceiling; using market rent only")
        return float(market), False, notes

    if market > ceiling:
        notes.append("Section 8 cap applied: market rent > approved ceiling")
    return float(min(market, ceiling)), False, notes


@router.post("/snapshot/{snapshot_id}", response_model=BatchEvalOut)
def evaluate_snapshot(
    snapshot_id: int,
    strategy: str = Query("section8", description="section8|market"),
    db: Session = Depends(get_db),
):
    deals = db.scalars(select(Deal).where(Deal.snapshot_id == snapshot_id)).all()

    pass_count = 0
    review_count = 0
    reject_count = 0

    for d in deals:
        p = db.scalar(select(Property).where(Property.id == d.property_id))
        if not p:
            continue

        ra = db.scalar(select(RentAssumption).where(RentAssumption.property_id == p.id))

        gross_rent, estimated, rent_notes = _rent_used_for_underwriting(strategy=strategy, d=d, ra=ra)

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
            strategy=strategy,
        )

        # Add rent selection notes (explainable)
        for n in rent_notes:
            reasons.append(n)

        # If rent was estimated, never allow PASS (force REVIEW)
        if estimated and decision == "PASS":
            decision = "REVIEW"
            reasons.append("Rent was estimated from 1.3% heuristic; verify with comps/FMR/ceiling before PASS")

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
def evaluate_run(
    snapshot_id: int = Query(...),
    strategy: str = Query("section8", description="section8|market"),
    db: Session = Depends(get_db),
):
    # allows: POST /evaluate/run?snapshot_id=4&strategy=section8
    return evaluate_snapshot(snapshot_id=snapshot_id, strategy=strategy, db=db)


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
