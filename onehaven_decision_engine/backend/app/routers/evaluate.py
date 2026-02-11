# backend/app/routers/evaluate.py
from __future__ import annotations

import json
from typing import Optional, List, Tuple

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import Deal, Property, RentAssumption, UnderwritingResult, JurisdictionRule
from ..schemas import BatchEvalOut, UnderwritingResultOut
from ..domain.decision_engine import score_and_decide
from ..domain.underwriting import underwrite

# NOTE: adjust this import if your function lives elsewhere
from ..domain.jurisdiction_scoring import compute_friction

router = APIRouter(prefix="/evaluate", tags=["evaluate"])


def _compute_ceiling_from_inputs(
    *,
    ra: Optional[RentAssumption],
    payment_standard_pct: float,
) -> Optional[float]:
    """
    Operating Truth:
      rent_ceiling = min(FMR * payment_standard_pct, rent_reasonableness_comp)

    If a manual approved_rent_ceiling override exists, we treat that as the ceiling.
    """
    if ra is None:
        return None

    # 1) Manual override wins (this is your persisted "approved ceiling")
    try:
        if ra.approved_rent_ceiling is not None and float(ra.approved_rent_ceiling) > 0:
            return float(ra.approved_rent_ceiling)
    except Exception:
        pass

    # 2) Otherwise compute from FMR + comps median
    fmr: Optional[float] = None
    comp: Optional[float] = None

    try:
        if ra.section8_fmr is not None and float(ra.section8_fmr) > 0:
            fmr = float(ra.section8_fmr)
    except Exception:
        fmr = None

    try:
        if ra.rent_reasonableness_comp is not None and float(ra.rent_reasonableness_comp) > 0:
            comp = float(ra.rent_reasonableness_comp)
    except Exception:
        comp = None

    if fmr is None and comp is None:
        return None

    candidates: list[float] = []
    if fmr is not None:
        candidates.append(float(fmr) * float(payment_standard_pct))
    if comp is not None:
        candidates.append(float(comp))

    return min(candidates) if candidates else None


def _rent_used_for_underwriting(
    *,
    strategy: str,
    d: Deal,
    ra: Optional[RentAssumption],
    payment_standard_pct: float,
) -> Tuple[float, bool, list[str], Optional[float]]:
    """
    Returns:
      (gross_rent_used, was_estimated, notes, computed_ceiling)

    computed_ceiling is included so we can persist it (when appropriate) and explain reasons.
    """
    notes: list[str] = []
    strategy = (strategy or "section8").strip().lower()

    market: Optional[float] = None
    if ra is not None:
        try:
            if ra.market_rent_estimate is not None and float(ra.market_rent_estimate) > 0:
                market = float(ra.market_rent_estimate)
        except Exception:
            market = None

    ceiling = _compute_ceiling_from_inputs(ra=ra, payment_standard_pct=float(payment_standard_pct))

    # --- MARKET strategy ---
    if strategy == "market":
        if market is not None:
            return market, False, notes, ceiling

        asking = float(d.asking_price or 0.0)
        est = asking * 0.013
        notes.append("Market strategy missing market rent; fell back to 1.3% heuristic")
        return float(est), True, notes, ceiling

    # --- SECTION 8 default ---
    if market is None and ceiling is None:
        asking = float(d.asking_price or 0.0)
        est = asking * 0.013
        notes.append("Section 8 strategy missing market rent and ceiling; fell back to 1.3% heuristic")
        return float(est), True, notes, ceiling

    if market is None:
        notes.append("Section 8 strategy missing market rent; using ceiling only")
        return float(ceiling), False, notes, ceiling  # type: ignore[arg-type]

    if ceiling is None:
        notes.append("Section 8 strategy missing ceiling; using market rent only")
        return float(market), False, notes, ceiling

    if market > ceiling:
        notes.append("Section 8 cap applied: market rent > approved ceiling")
    return float(min(market, ceiling)), False, notes, ceiling


@router.post("/snapshot/{snapshot_id}", response_model=BatchEvalOut)
def evaluate_snapshot(
    snapshot_id: int,
    # ✅ optional override for all deals in snapshot
    strategy: Optional[str] = Query(default=None, description="section8|market (optional override)"),
    # ✅ Operating Truth knob (payment standard percent)
    payment_standard_pct: float = Query(default=1.0, ge=0.5, le=1.5),
    db: Session = Depends(get_db),
):
    deals = db.scalars(select(Deal).where(Deal.snapshot_id == snapshot_id)).all()

    pass_count = 0
    review_count = 0
    reject_count = 0
    errors: list[str] = []

    for d in deals:
        try:
            p = db.scalar(select(Property).where(Property.id == d.property_id))
            if not p:
                errors.append(f"deal_id={d.id}: missing property_id={d.property_id}")
                continue

            ra = db.scalar(select(RentAssumption).where(RentAssumption.property_id == p.id))

            deal_strategy = (strategy or getattr(d, "strategy", None) or "section8").strip().lower()
            if deal_strategy not in {"section8", "market"}:
                deal_strategy = "section8"

            gross_rent, estimated, rent_notes, computed_ceiling = _rent_used_for_underwriting(
                strategy=deal_strategy,
                d=d,
                ra=ra,
                payment_standard_pct=float(payment_standard_pct),
            )

            # Persist rent outputs back to RentAssumption (Phase 1 heart)
            if ra is not None:
                ra.rent_used = float(gross_rent)

                # Only set approved_rent_ceiling automatically if:
                # - no manual override exists (or invalid)
                # - computed_ceiling exists
                try:
                    override_ok = ra.approved_rent_ceiling is not None and float(ra.approved_rent_ceiling) > 0
                except Exception:
                    override_ok = False

                if (not override_ok) and computed_ceiling is not None:
                    ra.approved_rent_ceiling = float(computed_ceiling)

                db.add(ra)
                db.commit()

            purchase_price = float(d.estimated_purchase_price or d.asking_price or 0.0)
            if purchase_price <= 0:
                errors.append(f"deal_id={d.id}: invalid purchase_price={purchase_price}")
                continue

            uw = underwrite(
                purchase_price=purchase_price,
                gross_rent=float(gross_rent),
                interest_rate=float(d.interest_rate),
                term_years=int(d.term_years),
                down_payment_pct=float(d.down_payment_pct),
                rehab_estimate=float(d.rehab_estimate or 0.0),
            )

            decision, score, reasons = score_and_decide(
                property=p,
                deal=d,
                rent_assumption=ra,
                underwriting=uw,
                strategy=deal_strategy,
            )

            # Always include rent reasoning notes
            reasons.extend(rent_notes)

            # If rent was heuristic-estimated, never PASS automatically
            if estimated and decision == "PASS":
                decision = "REVIEW"
                reasons.append("Rent was estimated from 1.3% heuristic; verify with comps/FMR/ceiling before PASS")

            # ---- Phase 2: Jurisdiction friction multiplier ----
            jr = db.scalar(
                select(JurisdictionRule).where(
                    JurisdictionRule.city == p.city,
                    JurisdictionRule.state == p.state,
                )
            )
            friction = compute_friction(jr)

            if getattr(friction, "reasons", None):
                reasons.extend([f"[Jurisdiction] {r}" for r in friction.reasons])

            # Apply multiplier to score (clamp 0..100)
            try:
                mult = float(getattr(friction, "multiplier", 1.0))
            except Exception:
                mult = 1.0

            score = int(round(float(score) * mult))
            score = max(0, min(100, score))

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

        except Exception as e:
            db.rollback()
            errors.append(f"deal_id={getattr(d, 'id', '?')}: {type(e).__name__}: {e}")

    return BatchEvalOut(
        snapshot_id=snapshot_id,
        total_deals=len(deals),
        pass_count=pass_count,
        review_count=review_count,
        reject_count=reject_count,
        errors=errors,
    )


@router.post("/run", response_model=BatchEvalOut)
def evaluate_run(
    snapshot_id: int = Query(...),
    strategy: Optional[str] = Query(default=None, description="section8|market (optional override)"),
    payment_standard_pct: float = Query(default=1.0, ge=0.5, le=1.5),
    db: Session = Depends(get_db),
):
    return evaluate_snapshot(
        snapshot_id=snapshot_id,
        strategy=strategy,
        payment_standard_pct=payment_standard_pct,
        db=db,
    )


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
