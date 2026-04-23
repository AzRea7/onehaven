# backend/app/routers/evaluate.py
from __future__ import annotations

import json
from typing import Optional, Tuple, List, Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from ..auth import get_principal
from app.config import settings
from app.db import get_db
from app.domain.decision_engine import score_and_decide
from app.domain.events import emit_workflow_event
from app.domain.jurisdiction_scoring import compute_friction
from app.domain.underwriting import underwrite
from app.models import Deal, Property, RentAssumption, UnderwritingResult, JurisdictionRule
from app.schemas import UnderwritingResultOut

from .rent import explain_rent

router = APIRouter(prefix="/evaluate", tags=["evaluate"])


_STRONG_DSCR = 1.30
_STRONG_CASHFLOW = 400.0
_MONEY_TOL = 2.0


class EvaluatePropertiesIn(BaseModel):
    property_ids: list[int] = Field(default_factory=list)
    strategy: Optional[str] = None
    payment_standard_pct: float | None = None
    explain_rent_first: bool = True


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


def _normalize_decision(decision: Optional[str]) -> str:
    value = str(decision or "").strip().upper()
    if value in {"GOOD", "PASS"}:
        return "GOOD"
    if value in {"REJECT", "FAIL"}:
        return "REJECT"
    if value in {"REVIEW", "UNKNOWN", ""}:
        return "REVIEW"
    return "REVIEW"


def _decision_filter_values(decision: str) -> list[str]:
    normalized = _normalize_decision(decision)
    equivalents = {
        "GOOD": ["GOOD", "PASS"],
        "REVIEW": ["REVIEW", "UNKNOWN"],
        "REJECT": ["REJECT", "FAIL"],
    }
    return equivalents.get(normalized, [normalized])


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

    if computed_ceiling is not None and _almost_equal(approved, computed_ceiling):
        return float(computed_ceiling), computed_reason, fmr_adjusted

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
        ra,
        payment_standard_pct=float(payment_standard_pct),
    )

    if strategy == "market":
        if market is not None:
            return float(market), False, notes, computed_ceiling, "none", fmr_adjusted

        asking = float(getattr(d, "asking_price", 0.0) or 0.0)
        est = asking * float(settings.rent_rule_min_pct)
        notes.append("Market strategy: missing market_rent_estimate; fell back to rent_rule_min_pct heuristic.")
        return float(est), True, notes, computed_ceiling, "none", fmr_adjusted

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


def _result_to_schema_payload(r: UnderwritingResult, p: Optional[Property] = None) -> dict[str, Any]:
    return {
        "id": r.id,
        "deal_id": r.deal_id,
        "org_id": r.org_id,
        "decision": _normalize_decision(r.decision),
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
        "bedrooms": getattr(p, "bedrooms", None) if p is not None else None,
        "bathrooms": getattr(p, "bathrooms", None) if p is not None else None,
        "rent_explain_run_id": getattr(r, "rent_explain_run_id", None),
    }


def evaluate_property_core(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    strategy: Optional[str] = None,
    payment_standard_pct: float | None = None,
    actor_user_id: Optional[int] = None,
    emit_events: bool = True,
    commit: bool = True,
) -> dict[str, Any]:
    prop = db.scalar(
        select(Property).where(
            Property.id == int(property_id),
            Property.org_id == int(org_id),
        )
    )
    if prop is None:
        return {"ok": False, "detail": "Property not found"}

    deal = db.scalar(
        select(Deal)
        .where(Deal.property_id == int(property_id), Deal.org_id == int(org_id))
        .order_by(desc(Deal.id))
    )
    if deal is None:
        return {"ok": False, "detail": "Deal not found for property"}

    ra = db.scalar(
        select(RentAssumption)
        .where(RentAssumption.property_id == int(property_id), RentAssumption.org_id == int(org_id))
        .order_by(desc(RentAssumption.id))
    )

    ps_default = getattr(settings, "default_payment_standard_pct", None)
    if ps_default is None:
        ps_default = getattr(settings, "payment_standard_pct", 1.0)
    pct = float(payment_standard_pct) if payment_standard_pct is not None else float(ps_default)

    deal_strategy = _norm_strategy(strategy or getattr(deal, "strategy", None) or "section8")

    rent_used, fallback_used, notes, computed_ceiling, cap_reason, fmr_adjusted = _rent_used_for_underwriting(
        strategy=deal_strategy,
        d=deal,
        ra=ra,
        payment_standard_pct=pct,
    )

    if ra is not None:
        ra.rent_used = float(rent_used)
        approved = _to_pos_float(getattr(ra, "approved_rent_ceiling", None))
        if approved is None and computed_ceiling is not None:
            ra.approved_rent_ceiling = float(computed_ceiling)
        db.add(ra)

    purchase_price = float(
        getattr(deal, "estimated_purchase_price", None) or getattr(deal, "asking_price", 0.0) or 0.0
    )
    if purchase_price <= 0:
        return {"ok": False, "detail": "invalid_purchase_price"}

    uw = underwrite(
        purchase_price=purchase_price,
        rehab_estimate=float(getattr(deal, "rehab_estimate", 0.0) or 0.0),
        down_payment_pct=float(getattr(deal, "down_payment_pct", 0.20) or 0.20),
        interest_rate=float(getattr(deal, "interest_rate", 0.075) or 0.075),
        term_years=int(getattr(deal, "term_years", 30) or 30),
        gross_rent=float(rent_used or 0.0),
    )

    raw_decision, score, reasons = score_and_decide(
        property=prop,
        deal=deal,
        rent_assumption=ra,
        underwriting=uw,
        strategy=deal_strategy,
    )
    reasons.extend(notes)

    decision = _normalize_decision(raw_decision)

    if fallback_used and decision == "GOOD":
        decision = "REVIEW"
        reasons.append("Rent was heuristic-estimated; verify comps/FMR/ceiling before GOOD.")

    jr = db.scalar(
        select(JurisdictionRule).where(
            JurisdictionRule.org_id == int(org_id),
            JurisdictionRule.city == prop.city,
            JurisdictionRule.state == prop.state,
        )
    )
    if jr is None:
        jr = db.scalar(
            select(JurisdictionRule).where(
                JurisdictionRule.org_id.is_(None),
                JurisdictionRule.city == prop.city,
                JurisdictionRule.state == prop.state,
            )
        )

    friction = compute_friction(jr)
    fr_reasons = list(getattr(friction, "reasons", []) or [])
    fr_mult = float(getattr(friction, "multiplier", 1.0) or 1.0)

    if fr_reasons:
        reasons.extend([f"[Jurisdiction] {r}" for r in fr_reasons])

    if jr is None and decision == "GOOD":
        decision = "REVIEW"
        reasons.append("No jurisdiction row → cannot mark GOOD without compliance friction data.")

    score = int(round(float(score) * fr_mult))
    score = max(0, min(100, score))

    decision = _convert_reject_to_review_when_strong(
        decision=decision,
        reasons=reasons,
        fr_reasons=fr_reasons,
        fr_mult=fr_mult,
        dscr=float(uw.dscr),
        cash_flow=float(uw.cash_flow),
    )
    decision = _normalize_decision(decision)

    existing = db.scalar(
        select(UnderwritingResult)
        .where(UnderwritingResult.deal_id == int(deal.id))
        .where(UnderwritingResult.org_id == int(org_id))
    )
    created = False
    if not existing:
        existing = UnderwritingResult(
            org_id=int(org_id),
            deal_id=int(deal.id),
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
        created = True

    existing.decision = _normalize_decision(decision)
    existing.score = int(score)
    existing.reasons_json = json.dumps(reasons)

    existing.gross_rent_used = float(rent_used)
    existing.mortgage_payment = float(uw.mortgage_payment)
    existing.operating_expenses = float(uw.operating_expenses)
    existing.noi = float(uw.noi)
    existing.cash_flow = float(uw.cash_flow)
    existing.dscr = float(uw.dscr)
    existing.cash_on_cash = float(uw.cash_on_cash)
    existing.break_even_rent = float(uw.break_even_rent)
    existing.min_rent_for_target_roi = float(uw.min_rent_for_target_roi)

    existing.decision_version = str(settings.decision_version)
    existing.payment_standard_pct_used = float(pct)
    existing.jurisdiction_multiplier = float(fr_mult)
    existing.jurisdiction_reasons_json = json.dumps(fr_reasons)
    existing.rent_cap_reason = str(cap_reason or "none")
    existing.fmr_adjusted = float(fmr_adjusted) if fmr_adjusted is not None else None

    db.add(existing)

    if emit_events:
        try:
            emit_workflow_event(
                db,
                org_id=int(org_id),
                actor_user_id=actor_user_id,
                event_type="underwriting_completed",
                property_id=int(property_id),
                payload={
                    "deal_id": int(deal.id),
                    "property_id": int(property_id),
                    "decision": _normalize_decision(decision),
                    "score": int(score),
                    "jurisdiction_multiplier": float(fr_mult),
                },
            )
        except Exception:
            pass

    if commit:
        db.commit()
        db.refresh(existing)
    else:
        db.flush()

    return {
        "ok": True,
        "property_id": int(property_id),
        "deal_id": int(deal.id),
        "decision": _normalize_decision(decision),
        "score": int(score),
        "fallback_used": fallback_used,
        "computed_ceiling": computed_ceiling,
        "cap_reason": cap_reason,
        "fmr_adjusted": fmr_adjusted,
        "created": created,
        "result_row": existing,
        "result": _result_to_schema_payload(existing, prop),
    }


@router.post("/properties", response_model=dict)
def evaluate_properties(
    payload: EvaluatePropertiesIn = Body(...),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    seen: set[int] = set()
    property_ids: list[int] = []
    for pid in payload.property_ids:
        if int(pid) in seen:
            continue
        seen.add(int(pid))
        property_ids.append(int(pid))

    ps_default = getattr(settings, "default_payment_standard_pct", None)
    if ps_default is None:
        ps_default = getattr(settings, "payment_standard_pct", 1.0)
    pct = float(payload.payment_standard_pct) if payload.payment_standard_pct is not None else float(ps_default)

    good_count = 0
    review_count = 0
    reject_count = 0
    errors: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []

    for pid in property_ids:
        try:
            deal = db.scalar(
                select(Deal)
                .where(Deal.property_id == int(pid), Deal.org_id == principal.org_id)
                .order_by(desc(Deal.id))
            )
            if deal is None:
                errors.append({"property_id": int(pid), "error": "deal_not_found"})
                continue

            deal_strategy = _norm_strategy(payload.strategy or getattr(deal, "strategy", None) or "section8")

            if payload.explain_rent_first:
                explain_rent(
                    property_id=int(pid),
                    strategy=deal_strategy,
                    payment_standard_pct=float(pct),
                    persist=True,
                    db=db,
                    p=principal,
                )

            res = evaluate_property_core(
                db,
                org_id=int(principal.org_id),
                property_id=int(pid),
                strategy=deal_strategy,
                payment_standard_pct=float(pct),
                actor_user_id=getattr(principal, "user_id", None),
                emit_events=True,
                commit=True,
            )
            if not res.get("ok"):
                errors.append({"property_id": int(pid), "error": res.get("detail")})
                continue

            if res["decision"] == "GOOD":
                good_count += 1
            elif res["decision"] == "REVIEW":
                review_count += 1
            else:
                reject_count += 1

            results.append(
                {
                    "property_id": int(pid),
                    "deal_id": res["deal_id"],
                    "decision": res["decision"],
                    "score": res["score"],
                    "result": UnderwritingResultOut.model_validate(res["result"]),
                }
            )
        except Exception as e:
            db.rollback()
            errors.append({"property_id": int(pid), "error": f"{type(e).__name__}: {e}"})

    return {
        "ok": True,
        "attempted": len(property_ids),
        "evaluated": len(results),
        "property_ids": property_ids,
        "good_count": good_count,
        "pass_count": good_count,  # backward compatibility only
        "review_count": review_count,
        "reject_count": reject_count,
        "results": results,
        "errors": errors,
    }


@router.get("/results", response_model=List[UnderwritingResultOut])
def evaluate_results(
    decision: Optional[str] = Query(None, description="GOOD|REVIEW|REJECT. PASS/FAIL/UNKNOWN accepted as aliases."),
    property_ids: Optional[str] = Query(None, description="Comma-separated property ids"),
    limit: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    """
    Property-first results view only.

    Legacy snapshot filtering was intentionally removed from the normal path.
    """
    q = (
        select(UnderwritingResult, Deal, Property)
        .join(Deal, Deal.id == UnderwritingResult.deal_id)
        .join(Property, Property.id == Deal.property_id)
        .where(UnderwritingResult.org_id == principal.org_id)
        .where(Deal.org_id == principal.org_id)
        .where(Property.org_id == principal.org_id)
    )

    if property_ids:
        ids: list[int] = []
        for x in property_ids.split(","):
            s = str(x or "").strip()
            if not s:
                continue
            try:
                ids.append(int(s))
            except Exception:
                continue
        if ids:
            q = q.where(Property.id.in_(ids))

    if decision is not None:
        q = q.where(UnderwritingResult.decision.in_(_decision_filter_values(decision)))

    rows = db.execute(q.order_by(desc(UnderwritingResult.id)).limit(limit)).all()

    out: list[UnderwritingResultOut] = []
    for r, d, p in rows:
        out.append(UnderwritingResultOut.model_validate(_result_to_schema_payload(r, p)))

    return out


@router.post("/property/{property_id}", response_model=dict)
def evaluate_property(
    property_id: int,
    strategy: Optional[str] = Query(default=None, description="section8|market (optional override)"),
    payment_standard_pct: float | None = Query(default=None, description="Override. Default from config."),
    explain_rent_first: bool = Query(default=True),
    db: Session = Depends(get_db),
    principal=Depends(get_principal),
):
    deal = db.scalar(
        select(Deal)
        .where(Deal.property_id == int(property_id), Deal.org_id == int(principal.org_id))
        .order_by(desc(Deal.id))
    )
    if deal is None:
        return {"ok": False, "detail": "Deal not found for property"}

    ps_default = getattr(settings, "default_payment_standard_pct", None)
    if ps_default is None:
        ps_default = getattr(settings, "payment_standard_pct", 1.0)
    pct = float(payment_standard_pct) if payment_standard_pct is not None else float(ps_default)

    deal_strategy = _norm_strategy(strategy or getattr(deal, "strategy", None) or "section8")

    if explain_rent_first:
        explain_rent(
            property_id=int(property_id),
            strategy=deal_strategy,
            payment_standard_pct=float(pct),
            persist=True,
            db=db,
            p=principal,
        )

    res = evaluate_property_core(
        db,
        org_id=int(principal.org_id),
        property_id=int(property_id),
        strategy=deal_strategy,
        payment_standard_pct=float(pct),
        actor_user_id=getattr(principal, "user_id", None),
        emit_events=True,
        commit=True,
    )
    if not res.get("ok"):
        return res

    return {
        "ok": True,
        "property_id": int(property_id),
        "deal_id": res["deal_id"],
        "decision": res["decision"],
        "score": res["score"],
        "fallback_used": res["fallback_used"],
        "computed_ceiling": res["computed_ceiling"],
        "cap_reason": res["cap_reason"],
        "fmr_adjusted": res["fmr_adjusted"],
        "created": res["created"],
        "result": UnderwritingResultOut.model_validate(res["result"]),
    }


@router.post("/snapshot/{snapshot_id}", response_model=dict, deprecated=True)
def evaluate_snapshot_legacy_disabled(
    snapshot_id: int,
    _db: Session = Depends(get_db),
    _principal=Depends(get_principal),
):
    raise HTTPException(
        status_code=410,
        detail={
            "code": "legacy_snapshot_evaluation_removed",
            "message": "Snapshot-based evaluation was retired. Use /evaluate/property/{property_id} or /evaluate/properties.",
            "snapshot_id": snapshot_id,
        },
    )


@router.post("/run", response_model=dict, deprecated=True)
def evaluate_run_legacy_disabled(
    _db: Session = Depends(get_db),
    _principal=Depends(get_principal),
):
    raise HTTPException(
        status_code=410,
        detail={
            "code": "legacy_snapshot_evaluation_removed",
            "message": "Legacy snapshot reevaluation was retired. Use property-first evaluation endpoints.",
        },
    )
