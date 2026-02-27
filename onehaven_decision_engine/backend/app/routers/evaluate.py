# backend/app/routers/evaluate.py
from __future__ import annotations

import json
from typing import Optional, Tuple, List

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, desc
from sqlalchemy.orm import Session

from ..auth import get_principal
from ..db import get_db
from ..models import Deal, Property, RentAssumption, UnderwritingResult, JurisdictionRule, RentExplainRun
from ..schemas import BatchEvalOut, UnderwritingResultOut
from ..domain.decision_engine import score_and_decide
from ..domain.underwriting import underwrite
from ..domain.jurisdiction_scoring import compute_friction
from ..domain.events import emit_workflow_event
from ..config import settings

from .rent import explain_rent

router = APIRouter(prefix="/evaluate", tags=["evaluate"])


# --- Policy knobs ----------------------------------------------------
# “Strong” underwriting: can survive friction/cap risk and becomes REVIEW instead of REJECT.
# You can later move these into config.py if you want.
_STRONG_DSCR = 1.30
_STRONG_CASHFLOW = 400.0

# Money tolerance when comparing ceiling values
_MONEY_TOL = 2.0


def _to_pos_float(v: object) -> Optional[float]:
    try:
        if v is None:
            return None
        f = float(v)
        return f if f > 0 else None
    except Exception:
        return None


def _norm_strategy(strategy: Optional[str]) -> str:
    s = (strategy or "section8").strip().lower()
    return s if s in {"section8", "market"} else "section8"


def _almost_equal(a: Optional[float], b: Optional[float], tol: float = _MONEY_TOL) -> bool:
    if a is None or b is None:
        return False
    return abs(float(a) - float(b)) <= float(tol)


def _compute_fmr_adjusted(ra: Optional[RentAssumption], payment_standard_pct: float) -> Optional[float]:
    if ra is None:
        return None
    fmr = _to_pos_float(getattr(ra, "section8_fmr", None))
    if fmr is None:
        return None
    return float(fmr) * float(payment_standard_pct)


def _ceiling_and_reason(
    ra: Optional[RentAssumption],
    payment_standard_pct: float,
) -> tuple[Optional[float], str, Optional[float]]:
    """
    Returns (ceiling, cap_reason, fmr_adjusted)

    cap_reason is one of:
      - "override"  (true manual override)
      - "fmr"       (payment standard cap)
      - "comps"     (rent reasonableness cap)
      - "none"
    """
    if ra is None:
        return None, "none", None

    fmr_adjusted = _compute_fmr_adjusted(ra, payment_standard_pct)

    candidates: list[tuple[str, float]] = []
    if fmr_adjusted is not None:
        candidates.append(("fmr", float(fmr_adjusted)))

    comps = _to_pos_float(getattr(ra, "rent_reasonableness_comp", None))
    if comps is not None:
        candidates.append(("comps", float(comps)))

    computed_ceiling: Optional[float] = None
    computed_reason: str = "none"
    if candidates:
        computed_reason, computed_ceiling = min(candidates, key=lambda x: x[1])

    approved = _to_pos_float(getattr(ra, "approved_rent_ceiling", None))
    if approved is None:
        return computed_ceiling, computed_reason, fmr_adjusted

    # If approved matches computed within tolerance, treat as computed (NOT manual override)
    if computed_ceiling is not None and _almost_equal(approved, computed_ceiling):
        return float(computed_ceiling), computed_reason, fmr_adjusted

    # Otherwise it’s a true override
    return float(approved), "override", fmr_adjusted


def _rent_used_for_underwriting(
    *,
    strategy: str,
    d: Deal,
    ra: Optional[RentAssumption],
    payment_standard_pct: float,
) -> Tuple[float, bool, list[str], Optional[float], str, Optional[float]]:
    notes: list[str] = []
    strategy = _norm_strategy(strategy)

    market = _to_pos_float(getattr(ra, "market_rent_estimate", None)) if ra is not None else None
    computed_ceiling, cap_reason, fmr_adjusted = _ceiling_and_reason(
        ra, payment_standard_pct=float(payment_standard_pct)
    )

    if strategy == "market":
        if market is not None:
            return float(market), False, notes, computed_ceiling, "none", fmr_adjusted

        asking = float(getattr(d, "asking_price", 0.0) or 0.0)
        est = asking * float(settings.rent_rule_min_pct)
        notes.append("Market strategy: missing market_rent_estimate; fell back to rent_rule_min_pct heuristic.")
        return float(est), True, notes, computed_ceiling, "none", fmr_adjusted

    # section8
    if market is None and computed_ceiling is None:
        asking = float(getattr(d, "asking_price", 0.0) or 0.0)
        est = asking * float(settings.rent_rule_min_pct)
        notes.append("Section 8: missing market_rent_estimate and ceiling; fell back to rent_rule_min_pct heuristic.")
        return float(est), True, notes, computed_ceiling, "none", fmr_adjusted

    if market is None:
        notes.append("Section 8: missing market_rent_estimate; using ceiling only.")
        return float(computed_ceiling), False, notes, computed_ceiling, cap_reason, fmr_adjusted  # type: ignore[arg-type]

    if computed_ceiling is None:
        notes.append("Section 8: missing ceiling; using market_rent_estimate only.")
        return float(market), False, notes, computed_ceiling, "none", fmr_adjusted

    if float(market) > float(computed_ceiling):
        notes.append("Section 8 cap applied: market_rent_estimate > ceiling.")

    return float(min(float(market), float(computed_ceiling))), False, notes, computed_ceiling, cap_reason, fmr_adjusted


def _is_strong_underwriting(*, dscr: float, cash_flow: float) -> bool:
    return float(dscr) >= float(_STRONG_DSCR) and float(cash_flow) >= float(_STRONG_CASHFLOW)


def _has_jurisdiction_signals(reasons: list[str], fr_reasons: list[str], fr_mult: float) -> bool:
    if fr_reasons:
        return True
    if abs(float(fr_mult) - 1.0) > 1e-9:
        return True
    # Detect if your scoring already injected jurisdiction notes
    for r in reasons:
        if "[Jurisdiction]" in r:
            return True
    return False


def _has_section8_cap_signal(reasons: list[str]) -> bool:
    needles = [
        "Section 8 cap applied",
        "capped",
        "ceiling",
        "approved_rent_ceiling",
    ]
    joined = " | ".join(reasons).lower()
    return any(n.lower() in joined for n in needles)


def _convert_reject_to_review_when_strong(
    *,
    decision: str,
    reasons: list[str],
    fr_reasons: list[str],
    fr_mult: float,
    dscr: float,
    cash_flow: float,
) -> str:
    """
    Product rule:
      If underwriting is strong, and the “badness” is operational/compliance/cap related,
      downgrade REJECT -> REVIEW instead of losing the deal.

    We intentionally do NOT convert when:
      - decision is already PASS/REVIEW
      - underwriting is not strong
    """
    if decision != "REJECT":
        return decision

    if not _is_strong_underwriting(dscr=dscr, cash_flow=cash_flow):
        return decision

    jurisdiction_signal = _has_jurisdiction_signals(reasons=reasons, fr_reasons=fr_reasons, fr_mult=fr_mult)
    cap_signal = _has_section8_cap_signal(reasons=reasons)

    if jurisdiction_signal or cap_signal:
        reasons.append(
            f"Converted REJECT→REVIEW: strong underwriting (DSCR {float(dscr):.2f}, cash_flow ${float(cash_flow):.0f}) "
            f"but friction/cap signals present (jurisdiction={jurisdiction_signal}, cap={cap_signal}, x{float(fr_mult):.2f})."
        )
        return "REVIEW"

    return decision


@router.post("/snapshot/{snapshot_id}", response_model=BatchEvalOut)
def evaluate_snapshot(
    snapshot_id: int,
    strategy: Optional[str] = Query(default=None, description="section8|market (optional override)"),
    payment_standard_pct: float | None = Query(default=None, description="Override. Default from config."),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    ps_default = getattr(settings, "default_payment_standard_pct", None)
    if ps_default is None:
        ps_default = getattr(settings, "payment_standard_pct", 1.0)
    ps = float(payment_standard_pct) if payment_standard_pct is not None else float(ps_default)

    deals = db.scalars(
        select(Deal)
        .where(Deal.snapshot_id == snapshot_id)
        .where(Deal.org_id == principal.org_id)
        .order_by(desc(Deal.id))
    ).all()

    pass_count = 0
    review_count = 0
    reject_count = 0
    errors: list[str] = []

    for d in deals:
        try:
            p = db.scalar(
                select(Property)
                .where(Property.id == d.property_id)
                .where(Property.org_id == principal.org_id)
            )
            if not p:
                errors.append(f"deal_id={d.id}: missing/unauthorized property_id={d.property_id}")
                db.rollback()
                continue

            deal_strategy = _norm_strategy(strategy or getattr(d, "strategy", None) or "section8")

            # Force explain artifact creation (RentExplainRun) + persist rent_used/ceiling
            _ = explain_rent(
                property_id=int(p.id),
                strategy=deal_strategy,
                payment_standard_pct=float(ps),
                persist=True,
                db=db,
                p=principal,
            )

            # Re-fetch RA after explain (explain may create/update it)
            ra = db.scalar(
                select(RentAssumption)
                .where(RentAssumption.property_id == p.id)
                .where(RentAssumption.org_id == principal.org_id)
            )

            rent_explain_run_id = db.scalar(
                select(RentExplainRun.id)
                .where(
                    RentExplainRun.org_id == principal.org_id,
                    RentExplainRun.property_id == int(p.id),
                    RentExplainRun.strategy == deal_strategy,
                )
                .order_by(desc(RentExplainRun.id))
                .limit(1)
            )

            gross_rent, estimated, rent_notes, computed_ceiling, cap_reason, fmr_adjusted = _rent_used_for_underwriting(
                strategy=deal_strategy,
                d=d,
                ra=ra,
                payment_standard_pct=ps,
            )

            if ra is not None:
                ra.rent_used = float(gross_rent)
                # Only auto-fill approved ceiling if empty
                manual = _to_pos_float(getattr(ra, "approved_rent_ceiling", None))
                if manual is None and computed_ceiling is not None:
                    ra.approved_rent_ceiling = float(computed_ceiling)
                db.add(ra)

            purchase_price = float(
                getattr(d, "estimated_purchase_price", None) or getattr(d, "asking_price", 0.0) or 0.0
            )
            if purchase_price <= 0:
                errors.append(f"deal_id={d.id}: invalid purchase_price={purchase_price}")
                db.rollback()
                continue

            uw = underwrite(
                purchase_price=purchase_price,
                gross_rent=float(gross_rent),
                interest_rate=float(d.interest_rate),
                term_years=int(d.term_years),
                down_payment_pct=float(d.down_payment_pct),
                rehab_estimate=float(getattr(d, "rehab_estimate", 0.0) or 0.0),
            )

            decision, score, reasons = score_and_decide(
                property=p,
                deal=d,
                rent_assumption=ra,
                underwriting=uw,
                strategy=deal_strategy,
            )

            reasons.extend(rent_notes)

            # If rent was heuristic-estimated, we never allow an autopass
            if estimated and decision == "PASS":
                decision = "REVIEW"
                reasons.append("Rent was heuristic-estimated; verify comps/FMR/ceiling before PASS.")

            jr = db.scalar(
                select(JurisdictionRule).where(
                    JurisdictionRule.org_id == principal.org_id,
                    JurisdictionRule.city == p.city,
                    JurisdictionRule.state == p.state,
                )
            )
            if jr is None:
                jr = db.scalar(
                    select(JurisdictionRule).where(
                        JurisdictionRule.org_id.is_(None),
                        JurisdictionRule.city == p.city,
                        JurisdictionRule.state == p.state,
                    )
                )

            friction = compute_friction(jr)
            fr_reasons = list(getattr(friction, "reasons", []) or [])
            fr_mult = float(getattr(friction, "multiplier", 1.0) or 1.0)

            if fr_reasons:
                reasons.extend([f"[Jurisdiction] {r}" for r in fr_reasons])

            # If we literally have no jurisdiction row, we still block PASS → REVIEW
            if jr is None and decision == "PASS":
                decision = "REVIEW"
                reasons.append("No jurisdiction row → cannot PASS without compliance friction data.")

            # Apply friction to score
            score = int(round(float(score) * fr_mult))
            score = max(0, min(100, score))

            # ✅ Key product rule: strong UW => friction/cap should produce REVIEW not REJECT
            decision = _convert_reject_to_review_when_strong(
                decision=decision,
                reasons=reasons,
                fr_reasons=fr_reasons,
                fr_mult=fr_mult,
                dscr=float(uw.dscr),
                cash_flow=float(uw.cash_flow),
            )

            existing = db.scalar(
                select(UnderwritingResult)
                .where(UnderwritingResult.deal_id == d.id)
                .where(UnderwritingResult.org_id == principal.org_id)
            )
            if not existing:
                existing = UnderwritingResult(
                    org_id=principal.org_id,
                    deal_id=d.id,
                    decision="REVIEW",
                    score=0,
                    reasons_json="[]",
                    gross_rent_used=0.0,
                    mortgage_payment=0.0,
                    operating_expenses=0.0,
                    noi=0.0,
                    cash_flow=0.0,
                    dscr=0.0,
                    cash_on_cash=0.0,
                    break_even_rent=0.0,
                    min_rent_for_target_roi=0.0,
                    decision_version=str(settings.decision_version),
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

            existing.decision_version = str(settings.decision_version)
            existing.payment_standard_pct_used = float(ps)

            existing.jurisdiction_multiplier = float(fr_mult)
            existing.jurisdiction_reasons_json = json.dumps(fr_reasons)

            existing.rent_cap_reason = str(cap_reason or "none")
            existing.fmr_adjusted = float(fmr_adjusted) if fmr_adjusted is not None else None

            if rent_explain_run_id is not None and hasattr(existing, "rent_explain_run_id"):
                setattr(existing, "rent_explain_run_id", int(rent_explain_run_id))

            emit_workflow_event(
                db,
                org_id=principal.org_id,
                actor_user_id=principal.user_id,
                event_type="deal_evaluated",
                property_id=int(d.property_id),
                payload={
                    "deal_id": int(d.id),
                    "property_id": int(d.property_id),
                    "decision": decision,
                    "score": int(score),
                    "rent_explain_run_id": rent_explain_run_id,
                    "jurisdiction_multiplier": float(fr_mult),
                },
            )

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

    emit_workflow_event(
        db,
        org_id=principal.org_id,
        actor_user_id=principal.user_id,
        event_type="snapshot_evaluated",
        payload={
            "snapshot_id": snapshot_id,
            "total": len(deals),
            "pass": pass_count,
            "review": review_count,
            "reject": reject_count,
        },
    )
    db.commit()

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
    principal=Depends(get_principal),
):
    return evaluate_snapshot(
        snapshot_id=snapshot_id,
        strategy=strategy,
        payment_standard_pct=payment_standard_pct,
        db=db,
        principal=principal,
    )


@router.get("/results", response_model=List[UnderwritingResultOut])
def evaluate_results(
    decision: Optional[str] = Query(None, description="PASS|REVIEW|REJECT"),
    snapshot_id: Optional[int] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    q = (
        select(UnderwritingResult, Deal, Property)
        .join(Deal, Deal.id == UnderwritingResult.deal_id)
        .join(Property, Property.id == Deal.property_id)
        .where(UnderwritingResult.org_id == principal.org_id)
        .where(Deal.org_id == principal.org_id)
        .where(Property.org_id == principal.org_id)
    )

    if snapshot_id is not None:
        q = q.where(Deal.snapshot_id == snapshot_id)
    if decision is not None:
        q = q.where(UnderwritingResult.decision == decision)

    rows = db.execute(q.order_by(desc(UnderwritingResult.id)).limit(limit)).all()

    out: list[UnderwritingResultOut] = []
    for r, d, p in rows:
        payload = {
            "id": r.id,
            "deal_id": r.deal_id,
            "org_id": r.org_id,
            "decision": r.decision,
            "score": r.score,
            "dscr": r.dscr,
            "cash_flow": r.cash_flow,
            "gross_rent_used": r.gross_rent_used,
            "mortgage_payment": r.mortgage_payment,
            "operating_expenses": r.operating_expenses,
            "noi": r.noi,
            "cash_on_cash": r.cash_on_cash,
            "break_even_rent": r.break_even_rent,
            "min_rent_for_target_roi": r.min_rent_for_target_roi,
            "decision_version": r.decision_version,
            "payment_standard_pct_used": r.payment_standard_pct_used,
            "jurisdiction_multiplier": r.jurisdiction_multiplier,
            "jurisdiction_reasons_json": r.jurisdiction_reasons_json,
            "rent_cap_reason": r.rent_cap_reason,
            "fmr_adjusted": r.fmr_adjusted,
            "reasons_json": r.reasons_json,
            "bedrooms": getattr(p, "bedrooms", None),
            "bathrooms": getattr(p, "bathrooms", None),
        }
        if hasattr(r, "rent_explain_run_id"):
            payload["rent_explain_run_id"] = getattr(r, "rent_explain_run_id")
        out.append(UnderwritingResultOut.model_validate(payload))

    return out
