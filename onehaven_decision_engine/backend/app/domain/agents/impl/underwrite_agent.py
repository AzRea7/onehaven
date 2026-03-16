from __future__ import annotations

from math import pow
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import settings
from app.models import Deal, Property


def _load_property_and_deal(db: Session, org_id: int, property_id: Optional[int]) -> tuple[Any, Any]:
    if property_id is None:
        return None, None
    prop = db.scalar(select(Property).where(Property.org_id == int(org_id), Property.id == int(property_id)))
    deal = db.scalar(
        select(Deal)
        .where(Deal.org_id == int(org_id), Deal.property_id == int(property_id))
        .order_by(Deal.id.desc())
    )
    return prop, deal


def _monthly_payment(principal: float, annual_rate: float, years: int) -> float:
    if principal <= 0:
        return 0.0
    monthly_rate = float(annual_rate or 0.0) / 12.0
    periods = max(1, int(years or 30) * 12)
    if monthly_rate <= 0:
        return principal / periods
    factor = pow(1.0 + monthly_rate, periods)
    return principal * monthly_rate * factor / (factor - 1.0)


def run(db: Session, *, org_id: int, property_id: Optional[int], input_payload: dict[str, Any]) -> dict[str, Any]:
    prop, deal = _load_property_and_deal(db, org_id, property_id)
    if prop is None:
        return {
            "agent_key": "deal_underwrite",
            "summary": "No property found for underwriting.",
            "facts": {"property_id": property_id},
            "recommendations": [],
        }

    asking_price = float(
        input_payload.get("purchase_price")
        or getattr(deal, "purchase_price", None)
        or getattr(deal, "estimated_purchase_price", None)
        or getattr(deal, "asking_price", None)
        or 0.0
    )
    rehab = float(input_payload.get("rehab_estimate") or getattr(deal, "rehab_estimate", None) or 0.0)
    down_payment_pct = float(input_payload.get("down_payment_pct") or getattr(deal, "down_payment_pct", None) or 0.2)
    interest_rate = float(input_payload.get("interest_rate") or getattr(deal, "interest_rate", None) or 0.07)
    term_years = int(input_payload.get("term_years") or getattr(deal, "term_years", None) or 30)
    gross_rent = float(input_payload.get("gross_rent") or (asking_price * 0.015 if asking_price else 0.0))

    loan_amount = max(0.0, asking_price * (1.0 - down_payment_pct))
    debt_service = _monthly_payment(loan_amount, interest_rate, term_years)
    effective_rent = gross_rent * (1.0 - float(settings.vacancy_rate))
    operating_expenses = gross_rent * (
        float(settings.maintenance_rate) + float(settings.management_rate) + float(settings.capex_rate)
    ) + float(settings.insurance_monthly) + float(settings.taxes_monthly) + float(settings.utilities_monthly)
    noi = effective_rent - operating_expenses
    cashflow = noi - debt_service
    dscr = (noi / debt_service) if debt_service > 0 else None
    cash_in = asking_price * down_payment_pct + rehab
    coc = ((cashflow * 12.0) / cash_in) if cash_in > 0 else None

    risk_flags: list[str] = []
    if dscr is not None and dscr < float(settings.dscr_min):
        risk_flags.append("DSCR below minimum")
    if cashflow < float(settings.target_monthly_cashflow):
        risk_flags.append("Cash flow below target")
    if gross_rent <= 0:
        risk_flags.append("Missing rent assumption")

    recs = []
    if risk_flags:
        recs.append(
            {
                "type": "underwrite_risk",
                "title": "Tight underwriting",
                "reason": "; ".join(risk_flags),
                "priority": "high",
            }
        )
    else:
        recs.append(
            {
                "type": "underwrite_greenlight",
                "title": "Underwrite looks workable",
                "reason": "DSCR and monthly cash flow are within configured guardrails.",
                "priority": "medium",
            }
        )

    recs.append(
        {
            "type": "sensitivity",
            "title": "Stress test rent and rehab",
            "reason": "Run ±5% rent and +10% rehab sensitivity before committing capital.",
            "priority": "medium",
        }
    )

    return {
        "agent_key": "deal_underwrite",
        "summary": "Deterministic underwriting completed using current deal, rent, and financing assumptions.",
        "facts": {
            "property_id": property_id,
            "asking_price": asking_price,
            "rehab_estimate": rehab,
            "gross_rent": gross_rent,
            "effective_rent": round(effective_rent, 2),
            "noi": round(noi, 2),
            "debt_service": round(debt_service, 2),
            "monthly_cashflow": round(cashflow, 2),
            "dscr": round(dscr, 3) if dscr is not None else None,
            "cash_on_cash": round(coc, 4) if coc is not None else None,
            "risk_flags": risk_flags,
        },
        "recommendations": recs,
    }
