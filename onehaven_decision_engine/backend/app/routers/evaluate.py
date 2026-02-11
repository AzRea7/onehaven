# backend/app/routers/evaluate.py
from __future__ import annotations

import json
from typing import Optional, List, Tuple

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, desc
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import Deal, Property, RentAssumption, UnderwritingResult, JurisdictionRule
from ..schemas import BatchEvalOut, UnderwritingResultOut
from ..domain.decision_engine import score_and_decide
from ..domain.underwriting import underwrite
from ..domain.jurisdiction_scoring import compute_friction
from ..config import settings

router = APIRouter(prefix="/evaluate", tags=["evaluate"])


def _ceiling_and_reason(
    ra: Optional[RentAssumption],
    payment_standard_pct: float,
) -> tuple[Optional[float], str, Optional[float]]:
    """
    Returns: (ceiling, cap_reason, fmr_adjusted)
      cap_reason: override | fmr | comps | none
    """
    if ra is None:
        return None, "none", None

    # manual override wins
    try:
        if ra.approved_rent_ceiling is not None and float(ra.approved_rent_ceiling) > 0:
            return float(ra.approved_rent_ceiling), "override", None
    except Exception:
        pass

    fmr_adjusted: Optional[float] = None
    candidates: list[tuple[str, float]] = []

    try:
        if ra.section8_fmr is not None and float(ra.section8_fmr) > 0:
            fmr_adjusted = float(ra.section8_fmr) * float(payment_standard_pct)
            candidates.append(("fmr", float(fmr_adjusted)))
    except Exception:
        fmr_adjusted = None

    try:
        if ra.rent_reasonableness_comp is not None and float(ra.rent_reasonableness_comp) > 0:
            candidates.append(("comps", float(ra.rent_reasonableness_comp)))
    except Exception:
        pass

    if not candidates:
        return None, "none", fmr_adjusted

    cap_reason, ceiling = min(candidates, key=lambda x: x[1])
    return float(ceiling), cap_reason, fmr_adjusted


def _rent_used_for_underwriting(
    *,
    strategy: str,
    d: Deal,
    ra: Optional[RentAssumption],
    payment_standard_pct: float,
) -> Tuple[float, bool, list[str], Optional[float], str, Optional[float]]:
    """
    Returns:
      gross_rent_used,
      was_estimated,
      notes,
      ceiling,
      cap_reason,
      fmr_adjusted
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

    ceiling, cap_reason, fmr_adjusted = _ceiling_and_reason(ra, payment_standard_pct=float(payment_standard_pct))

    if strategy == "market":
        if market is not None:
            return market, False, notes, ceiling, "none", fmr_adjusted
        asking = float(d.asking_price or 0.0)
        est = asking * float(settings.rent_rule_min_pct)
        notes.append("Market strategy missing market rent; fell back to rent_rule_min_pct heuristic")
        return float(est), True, notes, ceiling, "none", fmr_adjusted

    # section8 default
    if market is None and ceiling is None:
        asking = float(d.asking_price or 0.0)
        est = asking * float(settings.rent_rule_min_pct)
        notes.append("Section 8 missing market rent and ceiling; fell back to rent_rule_min_pct heuristic")
        return float(est), True, notes, ceiling, "none", fmr_adjusted

    if market is None:
        notes.append("Section 8 missing market rent; using ceiling only")
        return float(ceiling), False, notes, ceiling, cap_reason, fmr_adjusted  # type: ignore[arg-type]

    if ceiling is None:
        notes.append("Section 8 missing ceiling; using market only")
        return float(market), False, notes, ceiling, "none", fmr_adjusted

    if market > ceiling:
        notes.append("Section 8 cap applied: market > ceiling")
    return float(min(market, ceiling)), False, notes, ceiling, cap_reason, fmr_adjusted


@router.post("/snapshot/{snapshot_id}", response_model=BatchEvalOut)
def evaluate_snapshot(
    snapshot_id: int,
    strategy: Optional[str] = Query(default=None, description="section8|market (optional override)"),
    payment_standard_pct: float | None = Query(default=None, description="Override. Default from config."),
    db: Session = Depends(get_db),
):
    ps = float(payment_standard_pct) if payment_standard_pct is not None else float(settings.payment_standard_pct)

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

            gross_rent, estimated, rent_notes, computed_ceiling, cap_reason, fmr_adjusted = _rent_used_for_underwriting(
                strategy=deal_strategy,
                d=d,
                ra=ra,
                payment_standard_pct=ps,
            )

            # Persist rent_used always (underwriting consumes this)
            if ra is not None:
                ra.rent_used = float(gross_rent)

                # Only auto-fill approved_rent_ceiling if no valid manual override exists
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

            reasons.extend(rent_notes)

            if estimated and decision == "PASS":
                decision = "REVIEW"
                reasons.append("Rent was heuristic-estimated; verify comps/FMR/ceiling before PASS")

            # Phase 2: jurisdiction friction (persist exact friction used)
            jr = db.scalar(
                select(JurisdictionRule).where(
                    JurisdictionRule.city == p.city,
                    JurisdictionRule.state == p.state,
                )
            )
            friction = compute_friction(jr)

            fr_reasons = getattr(friction, "reasons", []) or []
            fr_mult = float(getattr(friction, "multiplier", 1.0) or 1.0)

            if fr_reasons:
                reasons.extend([f"[Jurisdiction] {r}" for r in fr_reasons])

            # Apply multiplier to score (clamp)
            score = int(round(float(score) * fr_mult))
            score = max(0, min(100, score))

            existing = db.scalar(select(UnderwritingResult).where(UnderwritingResult.deal_id == d.id))
            if not existing:
                existing = UnderwritingResult(
                    deal_id=d.id,
                    decision="REVIEW",
                    gross_rent_used=0.0,
                    mortgage_payment=0.0,
                    operating_expenses=0.0,
                    noi=0.0,
                    cash_flow=0.0,
                    dscr=0.0,
                    cash_on_cash=0.0,
                    break_even_rent=0.0,
                    min_rent_for_target_roi=0.0,
                )
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

            # ✅ Phase 0 reproducibility
            existing.decision_version = str(settings.decision_version)
            existing.payment_standard_pct_used = float(ps)

            # ✅ Phase 2 persist friction used
            existing.jurisdiction_multiplier = float(fr_mult)
            existing.jurisdiction_reasons_json = json.dumps(fr_reasons)

            # ✅ Phase 3 rent explain winners
            existing.rent_cap_reason = cap_reason
            existing.fmr_adjusted = float(fmr_adjusted) if fmr_adjusted is not None else None

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
    payment_standard_pct: float | None = Query(default=None),
    db: Session = Depends(get_db),
):
    return evaluate_snapshot(snapshot_id=snapshot_id, strategy=strategy, payment_standard_pct=payment_standard_pct, db=db)


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

    rows = db.scalars(q.order_by(desc(UnderwritingResult.id)).limit(limit)).all()
    return [UnderwritingResultOut.model_validate(r) for r in rows]
