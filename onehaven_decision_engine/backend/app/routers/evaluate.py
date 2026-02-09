from __future__ import annotations

import json

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select

from ..db import get_db
from ..models import Deal, Property, RentAssumption, UnderwritingResult, JurisdictionRule
from ..schemas import UnderwritingResultOut
from ..config import settings
from ..domain.decision_engine import DealContext, evaluate_deal_rules, reasons_from_json
from ..domain.underwriting import UnderwritingInputs, run_underwriting

router = APIRouter(prefix="/evaluate", tags=["evaluate"])


@router.post("/deal/{deal_id}", response_model=UnderwritingResultOut)
def evaluate_deal(deal_id: int, db: Session = Depends(get_db)):
    deal = db.get(Deal, deal_id)
    if not deal:
        raise HTTPException(status_code=404, detail="Deal not found")

    prop = db.get(Property, deal.property_id)
    if not prop:
        raise HTTPException(status_code=500, detail="Deal has missing property")

    ra = db.scalar(select(RentAssumption).where(RentAssumption.property_id == prop.id))

    rent_market = ra.market_rent_estimate if ra else None
    rent_ceiling = None
    if ra:
        rent_ceiling = ra.approved_rent_ceiling if ra.approved_rent_ceiling is not None else ra.section8_fmr

    ctx = DealContext(
        asking_price=deal.asking_price,
        bedrooms=prop.bedrooms,
        has_garage=prop.has_garage,
        rent_market=rent_market,
        rent_ceiling=rent_ceiling,
        inventory_count=ra.inventory_count if ra else None,
        starbucks_minutes=ra.starbucks_minutes if ra else None,
    )
    d = evaluate_deal_rules(ctx)

    reasons = list(d.reasons)

    if rent_market is None and rent_ceiling is None:
        reasons.append("Missing rent data -> cannot underwrite")
        final_decision = "REJECT"
        final_score = 0

        result = UnderwritingResult(
            deal_id=deal.id,
            decision=final_decision,
            score=int(final_score),
            reasons_json=json.dumps(reasons, ensure_ascii=False),
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
        db.add(result)
        db.commit()
        db.refresh(result)

        return UnderwritingResultOut(
            id=result.id,
            deal_id=result.deal_id,
            decision=result.decision,
            score=result.score,
            reasons=reasons_from_json(result.reasons_json),
            gross_rent_used=result.gross_rent_used,
            mortgage_payment=result.mortgage_payment,
            operating_expenses=result.operating_expenses,
            noi=result.noi,
            cash_flow=result.cash_flow,
            dscr=result.dscr,
            cash_on_cash=result.cash_on_cash,
            break_even_rent=result.break_even_rent,
            min_rent_for_target_roi=result.min_rent_for_target_roi,
        )

    if rent_market is None:
        gross_rent_used = float(rent_ceiling)
    elif rent_ceiling is None:
        gross_rent_used = float(rent_market)
    else:
        gross_rent_used = float(min(rent_market, rent_ceiling))

    purchase = deal.estimated_purchase_price if deal.estimated_purchase_price is not None else deal.asking_price

    uw_in = UnderwritingInputs(
        purchase_price=float(purchase),
        rehab=float(deal.rehab_estimate),
        down_payment_pct=float(deal.down_payment_pct),
        interest_rate=float(deal.interest_rate),
        term_years=int(deal.term_years),
        gross_rent=float(gross_rent_used),
        vacancy_rate=float(settings.vacancy_rate),
        maintenance_rate=float(settings.maintenance_rate),
        management_rate=float(settings.management_rate),
        capex_rate=float(settings.capex_rate),
        insurance_monthly=float(settings.insurance_monthly),
        taxes_monthly=float(settings.taxes_monthly),
        utilities_monthly=float(settings.utilities_monthly),
    )
    uw_out = run_underwriting(uw_in, target_roi=settings.target_roi)

    final_decision = d.decision
    final_score = d.score

    if uw_out.dscr < settings.dscr_min:
        reasons.append(f"DSCR {uw_out.dscr:.3f} below minimum {settings.dscr_min:.2f}")
        final_decision = "REJECT"
        final_score = min(final_score, 45)
    else:
        if uw_out.cash_flow < settings.target_monthly_cashflow:
            reasons.append(f"Cash flow ${uw_out.cash_flow:.2f} below target ${settings.target_monthly_cashflow:.0f}")
            final_decision = "REVIEW" if final_decision != "REJECT" else "REJECT"
            final_score = min(final_score, 65)

    jr = db.scalar(select(JurisdictionRule).where(JurisdictionRule.city == prop.city, JurisdictionRule.state == prop.state))
    if jr and jr.processing_days is not None and jr.processing_days >= 45:
        reasons.append(f"Jurisdiction processing delay risk ({jr.processing_days} days)")
        if final_decision == "PASS":
            final_decision = "REVIEW"
            final_score = min(final_score, 70)

    result = UnderwritingResult(
        deal_id=deal.id,
        decision=final_decision,
        score=int(final_score),
        reasons_json=json.dumps(reasons, ensure_ascii=False),
        gross_rent_used=float(gross_rent_used),
        mortgage_payment=float(uw_out.mortgage_payment),
        operating_expenses=float(uw_out.operating_expenses),
        noi=float(uw_out.noi),
        cash_flow=float(uw_out.cash_flow),
        dscr=float(uw_out.dscr),
        cash_on_cash=float(uw_out.cash_on_cash),
        break_even_rent=float(uw_out.break_even_rent),
        min_rent_for_target_roi=float(uw_out.min_rent_for_target_roi),
    )
    db.add(result)
    db.commit()
    db.refresh(result)

    return UnderwritingResultOut(
        id=result.id,
        deal_id=result.deal_id,
        decision=result.decision,
        score=result.score,
        reasons=reasons_from_json(result.reasons_json),
        gross_rent_used=result.gross_rent_used,
        mortgage_payment=result.mortgage_payment,
        operating_expenses=result.operating_expenses,
        noi=result.noi,
        cash_flow=result.cash_flow,
        dscr=result.dscr,
        cash_on_cash=result.cash_on_cash,
        break_even_rent=result.break_even_rent,
        min_rent_for_target_roi=result.min_rent_for_target_roi,
    )
