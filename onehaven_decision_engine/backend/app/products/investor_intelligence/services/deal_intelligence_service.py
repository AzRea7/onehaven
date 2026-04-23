from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Deal, Property, UnderwritingResult
from app.products.compliance.services.compliance_engine.brief_service import build_property_compliance_brief_summary


def _label_from_score(score: float) -> str:
    if score >= 80:
        return "buy"
    if score >= 60:
        return "caution"
    return "avoid"


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _latest_underwriting_for_deal(db: Session, *, deal_id: int) -> UnderwritingResult | None:
    stmt = (
        select(UnderwritingResult)
        .where(UnderwritingResult.deal_id == int(deal_id))
        .order_by(UnderwritingResult.created_at.desc(), UnderwritingResult.id.desc())
    )
    return db.scalars(stmt).first()


def _compliance_drag_penalty(compliance_brief: dict[str, Any]) -> float:
    missing = len(list(compliance_brief.get("missing_critical_items") or []))
    safe = bool(compliance_brief.get("safe_to_rent"))
    inspection_risk = str(compliance_brief.get("inspection_risk") or "unknown").lower()
    base = 0.0
    if not safe:
        base += 18.0
    if inspection_risk == "high":
        base += 12.0
    elif inspection_risk == "medium":
        base += 6.0
    base += min(15.0, float(missing * 3.0))
    return base


def _risk_adjusted_score(
    *,
    underwriting_score: float,
    cash_flow: float,
    dscr: float,
    compliance_drag: float,
) -> float:
    score = underwriting_score
    if cash_flow > 250:
        score += 8.0
    elif cash_flow > 0:
        score += 4.0
    else:
        score -= 10.0

    if dscr >= 1.25:
        score += 8.0
    elif dscr >= 1.1:
        score += 4.0
    else:
        score -= 8.0

    score -= compliance_drag
    return max(0.0, min(100.0, score))


def build_deal_intelligence_summary(
    db: Session,
    *,
    org_id: int,
    deal_id: int,
) -> dict[str, Any]:
    deal = db.get(Deal, int(deal_id))
    if deal is None:
        return {"ok": False, "error": "deal_not_found", "deal_id": int(deal_id)}

    property_row = db.get(Property, int(deal.property_id))
    underwriting = _latest_underwriting_for_deal(db, deal_id=int(deal.id))
    compliance_brief = build_property_compliance_brief_summary(
        db,
        org_id=int(org_id),
        property_id=int(deal.property_id),
    )

    underwriting_score = _safe_float(getattr(underwriting, "score", None), 50.0)
    cash_flow = _safe_float(getattr(underwriting, "cash_flow", None), 0.0)
    dscr = _safe_float(getattr(underwriting, "dscr", None), 0.0)
    compliance_drag = _compliance_drag_penalty(compliance_brief)
    risk_adjusted_score = _risk_adjusted_score(
        underwriting_score=underwriting_score,
        cash_flow=cash_flow,
        dscr=dscr,
        compliance_drag=compliance_drag,
    )

    return {
        "ok": True,
        "deal_id": int(deal.id),
        "property_id": int(deal.property_id),
        "address": getattr(property_row, "address", None),
        "city": getattr(property_row, "city", None),
        "state": getattr(property_row, "state", None),
        "asking_price": _safe_float(getattr(deal, "asking_price", None), 0.0),
        "rehab_estimate": _safe_float(getattr(deal, "rehab_estimate", None), 0.0),
        "underwriting_score": underwriting_score,
        "cash_flow": cash_flow,
        "dscr": dscr,
        "compliance_drag": compliance_drag,
        "risk_adjusted_score": risk_adjusted_score,
        "recommendation": _label_from_score(risk_adjusted_score),
        "compliance_summary": compliance_brief,
    }


def rank_deals_for_org(
    db: Session,
    *,
    org_id: int,
    limit: int = 25,
) -> dict[str, Any]:
    stmt = (
        select(Deal)
        .where(Deal.org_id == int(org_id))
        .order_by(Deal.created_at.desc(), Deal.id.desc())
    )
    rows = list(db.scalars(stmt).all())
    ranked: list[dict[str, Any]] = []
    for row in rows:
        summary = build_deal_intelligence_summary(
            db,
            org_id=int(org_id),
            deal_id=int(row.id),
        )
        if summary.get("ok"):
            ranked.append(summary)
    ranked.sort(key=lambda item: float(item.get("risk_adjusted_score") or 0.0), reverse=True)
    return {
        "ok": True,
        "count": len(ranked[:limit]),
        "rows": ranked[:limit],
    }
