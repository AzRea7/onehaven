from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from onehaven_platform.backend.src.models import ComplianceProfile, Inspection, InspectionItem, Property, RehabTask
from products.compliance.backend.src.services.compliance_engine.recommendation_service import build_compliance_recommendation
from products.compliance.backend.src.services.compliance_engine.fix_plan_service import build_fix_plan
from products.compliance.backend.src.services.compliance_engine.inspection_risk_service import build_inspection_risk_summary
from products.compliance.backend.src.services.policy_coverage.health_service import get_jurisdiction_health
from products.compliance.backend.src.services.compliance_engine.revenue_risk_service import build_revenue_risk_summary


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _norm_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _profile_monthly_rent(profile: Any) -> float | None:
    for name in ("market_rent_monthly", "monthly_rent", "rent_monthly", "expected_rent_monthly"):
        if hasattr(profile, name):
            value = _safe_float(getattr(profile, name), -1.0)
            if value >= 0:
                return value
    return None


def build_property_compliance_brief_summary(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    property_row = db.get(Property, int(property_id))
    if property_row is None:
        return {"ok": False, "error": "property_not_found", "property_id": int(property_id)}

    profile = db.scalars(
        select(ComplianceProfile).where(
            ComplianceProfile.org_id == int(org_id),
            ComplianceProfile.property_id == int(property_id),
        )
    ).first()

    inspections = list(
        db.scalars(
            select(Inspection).where(
                Inspection.org_id == int(org_id),
                Inspection.property_id == int(property_id),
            )
        ).all()
    )
    inspection_ids = [int(item.id) for item in inspections if getattr(item, "id", None) is not None]
    inspection_items: list[InspectionItem] = []
    if inspection_ids:
        inspection_items = list(
            db.scalars(
                select(InspectionItem).where(InspectionItem.inspection_id.in_(inspection_ids))
            ).all()
        )

    rehab_tasks = list(
        db.scalars(
            select(RehabTask).where(
                RehabTask.org_id == int(org_id),
                RehabTask.property_id == int(property_id),
            )
        ).all()
    )

    jurisdiction_health = {}
    property_state = getattr(property_row, "state", None)
    property_county = getattr(property_row, "county", None)
    property_city = getattr(property_row, "city", None)
    try:
        jurisdiction_health = get_jurisdiction_health(
            db,
            org_id=int(org_id),
            state=property_state,
            county=property_county,
            city=property_city,
            pha_name=None,
        )
    except Exception:
        jurisdiction_health = {"ok": False, "error": "jurisdiction_health_unavailable"}

    completeness = dict(jurisdiction_health.get("completeness") or {})
    lockout = dict(jurisdiction_health.get("lockout") or {})
    sla_summary = dict(jurisdiction_health.get("sla_summary") or {})

    missing_categories = list(completeness.get("missing_categories") or [])
    missing_critical_categories = list(
        completeness.get("missing_critical_categories")
        or completeness.get("critical_missing_categories")
        or lockout.get("critical_missing_binding_categories")
        or []
    )
    stale_authoritative_categories = list(
        completeness.get("stale_authoritative_categories")
        or jurisdiction_health.get("stale_authoritative_categories")
        or []
    )
    conflicting_categories = list(completeness.get("conflicting_categories") or [])
    manual_review_reasons = list(jurisdiction_health.get("manual_review_reasons") or [])

    inspection_risk = build_inspection_risk_summary(
        inspection_items=inspection_items,
        unresolved_requirements=missing_critical_categories,
        unresolved_conflicts=conflicting_categories,
        stale_authoritative_categories=stale_authoritative_categories,
    )

    fix_plan = build_fix_plan(
        rehab_tasks=rehab_tasks,
        missing_critical_requirements=missing_critical_categories,
        unresolved_categories=missing_categories,
        inspection_findings=inspection_risk.get("findings"),
    )

    revenue_risk = build_revenue_risk_summary(
        monthly_rent=_profile_monthly_rent(profile),
        section8_monthly_rent=_safe_float(getattr(profile, "money_at_risk_monthly", None), 0.0) or None,
        lockout_active=bool(lockout.get("lockout_active")),
        blocking_categories=list(lockout.get("lockout_causing_categories") or []),
        inspection_risk_level=str(inspection_risk.get("inspection_risk_level") or ""),
        failed_item_count=int(inspection_risk.get("failed_item_count") or 0),
        critical_failed_item_count=int(inspection_risk.get("critical_failed_item_count") or 0),
        stale_authoritative_categories=stale_authoritative_categories,
    )

    recommendation = build_compliance_recommendation(
        safe_for_projection=bool(jurisdiction_health.get("safe_for_projection", completeness.get("safe_for_projection", False))),
        safe_for_user_reliance=bool(jurisdiction_health.get("safe_to_rely_on", completeness.get("safe_for_user_reliance", False))),
        lockout_active=bool(lockout.get("lockout_active")),
        missing_categories=missing_categories,
        missing_critical_categories=missing_critical_categories,
        stale_authoritative_categories=stale_authoritative_categories,
        conflicting_categories=conflicting_categories,
        manual_review_reasons=manual_review_reasons,
        inspection_risk_level=str(inspection_risk.get("inspection_risk_level") or ""),
    )

    confidence = float(
        completeness.get("overall_completeness")
        or completeness.get("completeness_score")
        or getattr(profile, "confidence_score", None)
        or 0.0
    )
    evidence_basis = {
        "health_status": jurisdiction_health.get("health_status"),
        "confidence_label": completeness.get("confidence_label") or jurisdiction_health.get("confidence_label"),
        "covered_categories": list(completeness.get("covered_categories") or []),
        "missing_categories": missing_categories,
        "stale_authoritative_categories": stale_authoritative_categories,
        "lockout_causing_categories": list(lockout.get("lockout_causing_categories") or []),
        "review_required_categories": list(sla_summary.get("review_required_categories") or []),
    }

    return {
        "ok": True,
        "property_id": int(property_id),
        "address": getattr(property_row, "address", None),
        "city": getattr(property_row, "city", None),
        "state": getattr(property_row, "state", None),
        "status": recommendation["status"],
        "safe_to_rent": recommendation["safe_to_operate"],
        "inspection_risk_score": inspection_risk["inspection_risk_score"],
        "inspection_risk": inspection_risk["inspection_risk_level"],
        "inspection_timeline_risk": inspection_risk["inspection_timeline_risk"],
        "missing_critical_requirements": missing_critical_categories,
        "missing_categories": missing_categories,
        "fix_plan": fix_plan["steps"],
        "fix_plan_summary": fix_plan,
        "money_at_risk_monthly": revenue_risk["money_at_risk_monthly"],
        "revenue_risk": revenue_risk,
        "recommendation": recommendation["recommendation"],
        "why": recommendation["why"],
        "confidence": round(confidence, 4),
        "evidence_basis": evidence_basis,
        "jurisdiction_health": jurisdiction_health,
        "failed_item_count": inspection_risk["failed_item_count"],
        "critical_failed_item_count": inspection_risk["critical_failed_item_count"],
    }


def build_property_compliance_brief(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    return build_property_compliance_brief_summary(
        db,
        org_id=org_id,
        property_id=property_id,
    )
