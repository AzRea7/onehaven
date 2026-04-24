# backend/app/domain/agents/impl/underwrite_agent.py
from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from onehaven_platform.backend.src.config import settings
from onehaven_platform.backend.src.domain.agents.llm_router import run_llm_agent
from onehaven_platform.backend.src.adapters.intelligence_adapter import UnderwritingInputs, run_underwriting
from onehaven_platform.backend.src.models import Deal, Property


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v if v is not None else default)
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v if v is not None else default)
    except Exception:
        return default


def _latest_deal(db: Session, *, org_id: int, property_id: int) -> Optional[Deal]:
    return db.scalar(
        select(Deal)
        .where(Deal.org_id == int(org_id), Deal.property_id == int(property_id))
        .order_by(Deal.id.desc())
    )


def run_underwrite_agent(
    db: Session,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    if property_id is None:
        return {
            "agent_key": "underwrite",
            "summary": "Underwrite skipped because property_id is missing.",
            "facts": {"property_id": property_id},
            "recommendations": [
                {
                    "type": "missing_property_id",
                    "reason": "A property_id is required before underwriting can run.",
                    "priority": "high",
                }
            ],
            "actions": [],
        }

    prop = db.scalar(select(Property).where(Property.org_id == int(org_id), Property.id == int(property_id)))
    deal = _latest_deal(db, org_id=int(org_id), property_id=int(property_id))

    if prop is None or deal is None:
        return {
            "agent_key": "underwrite",
            "summary": "Underwrite unavailable because property or deal record is missing.",
            "facts": {
                "property_id": property_id,
                "property_found": prop is not None,
                "deal_found": deal is not None,
            },
            "recommendations": [
                {
                    "type": "missing_underwriting_inputs",
                    "reason": "A property and deal record are both required before underwriting can run.",
                    "priority": "high",
                }
            ],
            "actions": [],
        }

    purchase_price = _safe_float(
        input_payload.get("purchase_price"),
        _safe_float(getattr(deal, "purchase_price", None), _safe_float(getattr(prop, "asking_price", None))),
    )
    rehab = _safe_float(
        input_payload.get("rehab_estimate"),
        _safe_float(
            getattr(deal, "rehab_budget", None),
            _safe_float(getattr(deal, "estimated_rehab_cost", None)),
        ),
    )
    gross_rent = _safe_float(
        input_payload.get("gross_rent"),
        _safe_float(
            getattr(deal, "monthly_rent", None),
            _safe_float(
                getattr(deal, "target_rent", None),
                _safe_float(getattr(deal, "contract_rent", None)),
            ),
        ),
    )

    down_payment_pct = _safe_float(input_payload.get("down_payment_pct"), 0.20)
    interest_rate = _safe_float(input_payload.get("interest_rate"), 0.075)
    term_years = _safe_int(input_payload.get("term_years"), 30)

    vacancy_rate = _safe_float(input_payload.get("vacancy_rate"), float(settings.vacancy_rate))
    maintenance_rate = _safe_float(input_payload.get("maintenance_rate"), float(settings.maintenance_rate))
    management_rate = _safe_float(input_payload.get("management_rate"), float(settings.management_rate))
    capex_rate = _safe_float(input_payload.get("capex_rate"), float(settings.capex_rate))
    insurance_monthly = _safe_float(
        input_payload.get("insurance_monthly"),
        _safe_float(getattr(deal, "insurance_monthly", None), float(settings.insurance_monthly)),
    )
    taxes_monthly = _safe_float(
        input_payload.get("taxes_monthly"),
        _safe_float(getattr(deal, "taxes_monthly", None), float(settings.taxes_monthly)),
    )
    utilities_monthly = _safe_float(
        input_payload.get("utilities_monthly"),
        float(settings.utilities_monthly),
    )
    target_roi = _safe_float(input_payload.get("target_roi"), float(settings.target_roi))

    if purchase_price <= 0 or gross_rent <= 0:
        recs = []
        if purchase_price <= 0:
            recs.append(
                {
                    "type": "missing_purchase_price",
                    "reason": "Purchase price is missing or zero, so underwriting is incomplete.",
                    "priority": "high",
                }
            )
        if gross_rent <= 0:
            recs.append(
                {
                    "type": "missing_gross_rent",
                    "reason": "Gross rent is missing or zero, so underwriting is incomplete.",
                    "priority": "high",
                }
            )
        return {
            "agent_key": "underwrite",
            "summary": "Underwrite could not compute a full baseline because required inputs are missing.",
            "facts": {
                "property_id": int(property_id),
                "purchase_price": purchase_price,
                "gross_rent": gross_rent,
                "rehab": rehab,
            },
            "recommendations": recs,
            "actions": [],
        }

    inp = UnderwritingInputs(
        purchase_price=float(purchase_price),
        rehab=float(rehab),
        down_payment_pct=float(down_payment_pct),
        interest_rate=float(interest_rate),
        term_years=int(term_years),
        gross_rent=float(gross_rent),
        vacancy_rate=float(vacancy_rate),
        maintenance_rate=float(maintenance_rate),
        management_rate=float(management_rate),
        capex_rate=float(capex_rate),
        insurance_monthly=float(insurance_monthly),
        taxes_monthly=float(taxes_monthly),
        utilities_monthly=float(utilities_monthly),
    )
    uw = run_underwriting(inp, target_roi=float(target_roi))

    facts = {
        "property_id": int(property_id),
        "address": getattr(prop, "address", None),
        "purchase_price": purchase_price,
        "rehab": rehab,
        "gross_rent": gross_rent,
        "down_payment_pct": down_payment_pct,
        "interest_rate": interest_rate,
        "term_years": term_years,
        "vacancy_rate": vacancy_rate,
        "maintenance_rate": maintenance_rate,
        "management_rate": management_rate,
        "capex_rate": capex_rate,
        "insurance_monthly": insurance_monthly,
        "taxes_monthly": taxes_monthly,
        "utilities_monthly": utilities_monthly,
        "target_roi": target_roi,
        "mortgage_payment": float(uw.mortgage_payment),
        "operating_expenses": float(uw.operating_expenses),
        "noi": float(uw.noi),
        "cash_flow": float(uw.cash_flow),
        "dscr": float(uw.dscr),
        "cash_on_cash": float(uw.cash_on_cash),
        "break_even_rent": float(uw.break_even_rent),
        "min_rent_for_target_roi": float(uw.min_rent_for_target_roi),
    }

    deterministic = {
        "agent_key": "underwrite",
        "summary": "Deterministic underwriting baseline completed.",
        "facts": facts,
        "recommendations": [
            {
                "type": "underwrite_snapshot",
                "title": "Underwrite snapshot",
                "reason": "Use this deterministic baseline for decisioning and sensitivity review.",
                "priority": "medium",
            }
        ],
        "actions": [],
        "confidence": 0.92,
    }

    try:
        llm_output = run_llm_agent(
            agent_key="underwrite",
            context={"deterministic_baseline": deterministic, "input_payload": input_payload},
            mode="hybrid",
        )
        llm_output["facts"] = {**facts, **(llm_output.get("facts") or {})}
        llm_output["agent_key"] = "underwrite"
        llm_output["actions"] = []
        return llm_output
    except Exception:
        return deterministic
    