from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from onehaven_platform.backend.src.models import Tenant, Unit


def _score_tenant_to_unit(tenant: Tenant, unit: Unit) -> float:
    score = 50.0
    voucher_status = str(getattr(tenant, "voucher_status", "") or "").lower()
    if voucher_status in {"approved", "active", "issued"} and bool(getattr(unit, "voucher_eligible", False)):
        score += 25.0
    if str(getattr(unit, "occupancy_status", "") or "").lower() in {"vacant", "available"}:
        score += 15.0
    if getattr(unit, "market_rent", None):
        score += 5.0
    return min(100.0, score)


def build_tenant_match_summary(
    db: Session,
    *,
    org_id: int,
    tenant_id: int | None = None,
) -> dict[str, Any]:
    tenants = []
    if tenant_id is not None:
        tenant = db.get(Tenant, int(tenant_id))
        if tenant is None:
            return {"ok": False, "error": "tenant_not_found", "tenant_id": int(tenant_id)}
        tenants = [tenant]
    else:
        tenants = list(db.scalars(select(Tenant).where(Tenant.org_id == int(org_id))).all())

    units = list(db.scalars(select(Unit).where(Unit.org_id == int(org_id))).all())
    rows: list[dict[str, Any]] = []
    for tenant in tenants:
        ranked_units = []
        for unit in units:
            ranked_units.append(
                {
                    "unit_id": int(unit.id),
                    "property_id": int(unit.property_id),
                    "unit_label": unit.unit_label,
                    "occupancy_status": unit.occupancy_status,
                    "voucher_eligible": bool(unit.voucher_eligible),
                    "market_rent": getattr(unit, "market_rent", None),
                    "match_score": _score_tenant_to_unit(tenant, unit),
                }
            )
        ranked_units.sort(key=lambda item: float(item["match_score"]), reverse=True)

        voucher_status = str(getattr(tenant, "voucher_status", "") or "").lower()
        missing_workflow_items = []
        if voucher_status in {"", "unknown", "pending"}:
            missing_workflow_items.append("confirm_voucher_status")
        if not getattr(tenant, "phone", None) and not getattr(tenant, "email", None):
            missing_workflow_items.append("collect_contact_information")

        rows.append(
            {
                "tenant_id": int(tenant.id),
                "full_name": tenant.full_name,
                "voucher_readiness": "ready" if voucher_status in {"approved", "active", "issued"} else "needs_review",
                "missing_workflow_items": missing_workflow_items,
                "ranked_units": ranked_units[:10],
            }
        )

    return {"ok": True, "count": len(rows), "rows": rows}
