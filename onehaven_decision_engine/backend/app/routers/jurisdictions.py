# backend/app/routers/jurisdictions.py
from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, or_, desc
from sqlalchemy.orm import Session

from ..auth import get_principal, require_owner, require_operator
from ..db import get_db
from ..models import JurisdictionRule
from ..domain.audit import emit_audit

router = APIRouter(prefix="/jurisdictions", tags=["jurisdictions"])


def _norm_city(v: str) -> str:
    return (v or "").strip().title()


def _norm_state(v: str) -> str:
    s = (v or "MI").strip().upper()
    return s if len(s) == 2 else "MI"


def _row_to_dict(r: JurisdictionRule) -> dict:
    return {
        "id": r.id,
        "scope": "global" if r.org_id is None else "org",
        "org_id": r.org_id,
        "city": r.city,
        "state": r.state,
        "rental_license_required": r.rental_license_required,
        "inspection_authority": r.inspection_authority,
        "inspection_frequency": r.inspection_frequency,
        "typical_fail_points_json": r.typical_fail_points_json,
        "registration_fee": getattr(r, "registration_fee", None),
        "fees_json": getattr(r, "fees_json", None),
        "processing_days": r.processing_days,
        "tenant_waitlist_depth": r.tenant_waitlist_depth,
        "notes": r.notes,
        "updated_at": r.updated_at.isoformat() if getattr(r, "updated_at", None) else None,
        "created_at": r.created_at.isoformat() if getattr(r, "created_at", None) else None,
    }


# --------------------------
# Existing endpoints (kept)
# --------------------------
@router.get("/rules", response_model=list[dict])
def list_rules(
    city: Optional[str] = Query(default=None),
    state: str = Query(default="MI"),
    scope: str = Query(default="all", description="all|org|global"),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    """
    Returns rules visible to this org.
    Precedence: org-specific override > global.
    """
    state = _norm_state(state)
    city_norm = _norm_city(city) if city else None

    q = select(JurisdictionRule).where(
        or_(JurisdictionRule.org_id == p.org_id, JurisdictionRule.org_id.is_(None))
    )

    if scope == "org":
        q = select(JurisdictionRule).where(JurisdictionRule.org_id == p.org_id)
    elif scope == "global":
        q = select(JurisdictionRule).where(JurisdictionRule.org_id.is_(None))

    if city_norm:
        q = q.where(JurisdictionRule.city == city_norm, JurisdictionRule.state == state)
    else:
        q = q.where(JurisdictionRule.state == state)

    rows = list(db.scalars(q.order_by(desc(JurisdictionRule.org_id), JurisdictionRule.city)).all())
    return [_row_to_dict(r) for r in rows]


@router.get("/rule", response_model=dict)
def get_effective_rule(
    city: str,
    state: str = "MI",
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    """
    Return the single “effective” rule: org override if exists, else global, else 404.
    """
    city = _norm_city(city)
    state = _norm_state(state)

    org_row = db.scalar(
        select(JurisdictionRule).where(
            JurisdictionRule.org_id == p.org_id,
            JurisdictionRule.city == city,
            JurisdictionRule.state == state,
        )
    )
    if org_row:
        return {"scope": "org", "rule": _row_to_dict(org_row)}

    global_row = db.scalar(
        select(JurisdictionRule).where(
            JurisdictionRule.org_id.is_(None),
            JurisdictionRule.city == city,
            JurisdictionRule.state == state,
        )
    )
    if global_row:
        return {"scope": "global", "rule": _row_to_dict(global_row)}

    raise HTTPException(status_code=404, detail="No jurisdiction rule found (org or global).")


@router.post("/rule", response_model=dict)
def upsert_rule(
    payload: dict,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _owner=Depends(require_owner),
    scope: str = Query(default="org", description="org|global"),
):
    """
    Existing upsert endpoint (kept for backward compatibility).
    Default scope is org-only. To write global rules: ?scope=global (owner-only).
    """
    city = _norm_city(payload.get("city") or "")
    state = _norm_state(payload.get("state") or "MI")
    if not city:
        raise HTTPException(status_code=400, detail="city is required")

    org_id = None if scope == "global" else p.org_id

    existing = db.scalar(
        select(JurisdictionRule).where(
            JurisdictionRule.org_id.is_(None) if org_id is None else (JurisdictionRule.org_id == org_id),
            JurisdictionRule.city == city,
            JurisdictionRule.state == state,
        )
    )

    before = _row_to_dict(existing) if existing else None

    data = dict(payload)
    data["city"] = city
    data["state"] = state

    for k in ["id", "org_id", "updated_at", "created_at", "scope"]:
        data.pop(k, None)

    now = datetime.utcnow()

    if existing is None:
        row = JurisdictionRule(org_id=org_id, updated_at=now, created_at=now, **data)
        db.add(row)
        db.commit()
        db.refresh(row)

        emit_audit(
            db,
            org_id=p.org_id,
            actor_user_id=p.user_id,
            action="jurisdiction.create",
            entity_type="JurisdictionRule",
            entity_id=str(row.id),
            before=None,
            after=_row_to_dict(row),
        )
        db.commit()
        return {"ok": True, "id": row.id, "scope": "global" if row.org_id is None else "org"}

    for k, v in data.items():
        if hasattr(existing, k):
            setattr(existing, k, v)
    existing.updated_at = now
    db.commit()
    db.refresh(existing)

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="jurisdiction.update",
        entity_type="JurisdictionRule",
        entity_id=str(existing.id),
        before=before,
        after=_row_to_dict(existing),
    )
    db.commit()

    return {"ok": True, "id": existing.id, "scope": "global" if existing.org_id is None else "org"}


@router.delete("/rule", response_model=dict)
def delete_rule(
    city: str,
    state: str = "MI",
    scope: str = Query(default="org", description="org|global"),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _owner=Depends(require_owner),
):
    """
    Existing delete endpoint (kept for backward compatibility).
    """
    city = _norm_city(city)
    state = _norm_state(state)
    org_id = None if scope == "global" else p.org_id

    row = db.scalar(
        select(JurisdictionRule).where(
            JurisdictionRule.org_id.is_(None) if org_id is None else (JurisdictionRule.org_id == org_id),
            JurisdictionRule.city == city,
            JurisdictionRule.state == state,
        )
    )
    if not row:
        raise HTTPException(status_code=404, detail="Rule not found")

    before = _row_to_dict(row)
    rid = row.id
    db.delete(row)
    db.commit()

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="jurisdiction.delete",
        entity_type="JurisdictionRule",
        entity_id=str(rid),
        before=before,
        after=None,
    )
    db.commit()

    return {"ok": True, "deleted_id": rid}


# --------------------------
# ✅ NEW: Seed endpoint for Phase 2 operability
# --------------------------
@router.post("/seed", response_model=dict)
def seed_michigan_defaults(
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _owner=Depends(require_owner),
):
    """
    Creates a small set of global (org_id=NULL) MI defaults if missing.
    Safe to run multiple times.
    """
    now = datetime.utcnow()

    defaults = [
        dict(
            city="Detroit",
            state="MI",
            rental_license_required=True,
            inspection_authority="City of Detroit",
            inspection_frequency="annual",
            typical_fail_points_json='["GFCI missing","handrails","peeling paint","smoke/CO detectors","broken windows"]',
            processing_days=21,
            tenant_waitlist_depth="high",
            notes="Baseline default. Override per neighborhood/authority if needed.",
        ),
        dict(
            city="Pontiac",
            state="MI",
            rental_license_required=True,
            inspection_authority="City of Pontiac",
            inspection_frequency="annual",
            typical_fail_points_json='["GFCI missing","peeling paint","egress issues","utilities not secured"]',
            processing_days=14,
            tenant_waitlist_depth="medium",
            notes="Baseline default. Confirm local registration/fees.",
        ),
        dict(
            city="Royal Oak",
            state="MI",
            rental_license_required=True,
            inspection_authority="City of Royal Oak",
            inspection_frequency="periodic",
            typical_fail_points_json='["handrails","GFCI missing","smoke/CO detectors","egress"]',
            processing_days=10,
            tenant_waitlist_depth="medium",
            notes="Baseline default. Verify frequency by rental license type.",
        ),
    ]

    created = 0
    for d in defaults:
        city = _norm_city(d["city"])
        state = _norm_state(d["state"])
        exists = db.scalar(
          select(JurisdictionRule).where(
              JurisdictionRule.org_id.is_(None),
              JurisdictionRule.city == city,
              JurisdictionRule.state == state,
          )
        )
        if exists:
            continue

        row = JurisdictionRule(
            org_id=None,
            city=city,
            state=state,
            rental_license_required=bool(d.get("rental_license_required", False)),
            inspection_authority=d.get("inspection_authority"),
            inspection_frequency=d.get("inspection_frequency"),
            typical_fail_points_json=d.get("typical_fail_points_json") or "[]",
            processing_days=d.get("processing_days"),
            tenant_waitlist_depth=d.get("tenant_waitlist_depth"),
            notes=d.get("notes"),
            created_at=now,
            updated_at=now,
        )
        db.add(row)
        created += 1

    db.commit()
    return {"ok": True, "created": created}
