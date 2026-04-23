from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Inspection, Lease, Property, Task, Unit


def _normalize_status(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text or "unknown"


def build_property_ops_summary(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    property_row = db.get(Property, int(property_id))
    if property_row is None:
        return {"ok": False, "error": "property_not_found", "property_id": int(property_id)}

    tasks = list(
        db.scalars(
            select(Task).where(Task.org_id == int(org_id), Task.property_id == int(property_id))
        ).all()
    )
    leases = list(
        db.scalars(
            select(Lease).where(Lease.org_id == int(org_id), Lease.property_id == int(property_id))
        ).all()
    )
    units = list(
        db.scalars(
            select(Unit).where(Unit.org_id == int(org_id), Unit.property_id == int(property_id))
        ).all()
    )
    inspections = list(
        db.scalars(
            select(Inspection).where(Inspection.org_id == int(org_id), Inspection.property_id == int(property_id))
        ).all()
    )

    urgent_tasks = [
        {
            "task_id": int(task.id),
            "title": task.title,
            "priority": task.priority,
            "status": task.status,
            "due_at": task.due_at.isoformat() if task.due_at else None,
        }
        for task in tasks
        if _normalize_status(getattr(task, "status", None)) not in {"done", "completed"}
        and str(getattr(task, "priority", "") or "").lower() in {"high", "critical"}
    ]

    lease_issues = []
    now = datetime.utcnow()
    for lease in leases:
        end_date = getattr(lease, "end_date", None)
        if end_date is not None and end_date <= now:
            lease_issues.append(
                {
                    "lease_id": int(lease.id),
                    "tenant_id": int(lease.tenant_id),
                    "issue": "expired_lease",
                    "end_date": end_date.isoformat(),
                }
            )

    turnover_ready_units = [
        {
            "unit_id": int(unit.id),
            "unit_label": unit.unit_label,
            "occupancy_status": unit.occupancy_status,
        }
        for unit in units
        if _normalize_status(getattr(unit, "occupancy_status", None)) in {"vacant", "turnover", "available"}
    ]

    inspection_schedule = [
        {
            "inspection_id": int(item.id),
            "inspection_date": item.inspection_date.isoformat() if item.inspection_date else None,
            "result_status": item.result_status,
            "readiness_status": item.readiness_status,
        }
        for item in inspections
    ]

    return {
        "ok": True,
        "property_id": int(property_id),
        "address": getattr(property_row, "address", None),
        "urgent_tasks": urgent_tasks,
        "lease_issues": lease_issues,
        "inspection_schedule": inspection_schedule,
        "turnover_readiness": {
            "vacant_or_available_unit_count": len(turnover_ready_units),
            "units": turnover_ready_units,
        },
    }
