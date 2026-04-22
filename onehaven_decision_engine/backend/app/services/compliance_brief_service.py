from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import ComplianceProfile, Inspection, InspectionItem, Property, RehabTask


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _inspection_risk_label(score: float) -> str:
    if score >= 70:
        return "high"
    if score >= 40:
        return "medium"
    return "low"


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
    inspection_ids = [int(item.id) for item in inspections]
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

    failed_items = [item for item in inspection_items if bool(getattr(item, "failed", False))]
    critical_failed_items = [
        item for item in failed_items if int(getattr(item, "severity", 0) or 0) >= 3
    ]

    inspection_risk_score = _safe_float(getattr(profile, "inspection_risk_score", None), float(min(100, len(failed_items) * 12)))
    money_at_risk = _safe_float(getattr(profile, "money_at_risk_monthly", None), float(len(critical_failed_items) * 150.0))
    safe_to_rent = bool(getattr(profile, "safe_to_rent", False)) if profile is not None else len(critical_failed_items) == 0

    missing_critical_items = [
        str(getattr(item, "code", None) or getattr(item, "category", None) or "inspection_item")
        for item in critical_failed_items
    ]

    fix_plan = [
        {
            "title": task.title,
            "category": task.category,
            "status": task.status,
            "cost_estimate": _safe_float(getattr(task, "cost_estimate", None), 0.0),
            "deadline": task.deadline.isoformat() if getattr(task, "deadline", None) else None,
        }
        for task in rehab_tasks
        if str(getattr(task, "status", "") or "").lower() not in {"done", "completed"}
    ]

    recommendation = "safe_to_operate"
    if not safe_to_rent:
        recommendation = "hold_and_remediate"
    elif inspection_risk_score >= 40:
        recommendation = "monitor_and_fix"

    return {
        "ok": True,
        "property_id": int(property_id),
        "address": getattr(property_row, "address", None),
        "city": getattr(property_row, "city", None),
        "state": getattr(property_row, "state", None),
        "safe_to_rent": safe_to_rent,
        "inspection_risk_score": inspection_risk_score,
        "inspection_risk": _inspection_risk_label(inspection_risk_score),
        "missing_critical_items": missing_critical_items,
        "fix_plan": fix_plan,
        "money_at_risk_monthly": money_at_risk,
        "recommendation": recommendation,
        "failed_item_count": len(failed_items),
        "critical_failed_item_count": len(critical_failed_items),
    }
