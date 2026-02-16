# backend/app/routers/jurisdictions.py
from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, or_, desc
from sqlalchemy.orm import Session

from ..auth import get_principal, require_owner
from ..db import get_db
from ..models import JurisdictionRule
from ..domain.audit import emit_audit

router = APIRouter(prefix="/jurisdictions", tags=["jurisdictions"])


def _norm_city(v: str) -> str:
    return (v or "").strip()


def _norm_state(v: str) -> str:
    s = (v or "MI").strip().upper()
    return s if len(s) == 2 else "MI"


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

    q = select(JurisdictionRule).where(or_(JurisdictionRule.org_id == p.org_id, JurisdictionRule.org_id.is_(None)))

    if scope == "org":
        q = select(JurisdictionRule).where(JurisdictionRule.org_id == p.org_id)
    elif scope == "global":
        q = select(JurisdictionRule).where(JurisdictionRule.org_id.is_(None))

    if city_norm:
        q = q.where(JurisdictionRule.city == city_norm, JurisdictionRule.state == state)
    else:
        q = q.where(JurisdictionRule.state == state)

    rows = list(db.scalars(q.order_by(desc(JurisdictionRule.org_id), JurisdictionRule.city)).all())

    # Convert to dict so UI can render without new pydantic schema edits
    out: list[dict] = []
    for r in rows:
        out.append(
            {
                "id": r.id,
                "scope": "global" if r.org_id is None else "org",
                "org_id": r.org_id,
                "city": r.city,
                "state": r.state,
                "rental_license_required": r.rental_license_required,
                "inspection_authority": r.inspection_authority,
                "inspection_frequency": r.inspection_frequency,
                "typical_fail_points_json": r.typical_fail_points_json,
                "registration_fee": r.registration_fee,
                "processing_days": r.processing_days,
                "tenant_waitlist_depth": r.tenant_waitlist_depth,
                "notes": r.notes,
                "updated_at": r.updated_at.isoformat() if r.updated_at else None,
            }
        )
    return out


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
        return {"scope": "org", "rule": {"id": org_row.id, "city": org_row.city, "state": org_row.state, "data": org_row.__dict__}}

    global_row = db.scalar(
        select(JurisdictionRule).where(
            JurisdictionRule.org_id.is_(None),
            JurisdictionRule.city == city,
            JurisdictionRule.state == state,
        )
    )
    if global_row:
        return {
            "scope": "global",
            "rule": {"id": global_row.id, "city": global_row.city, "state": global_row.state, "data": global_row.__dict__},
        }

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
    Upsert a rule. Default scope is org-only.
    To write global rules, call with ?scope=global (still owner-only).
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

    before = None
    if existing:
        before = {
            "id": existing.id,
            "org_id": existing.org_id,
            "city": existing.city,
            "state": existing.state,
            "rental_license_required": existing.rental_license_required,
            "inspection_frequency": existing.inspection_frequency,
            "processing_days": existing.processing_days,
            "tenant_waitlist_depth": existing.tenant_waitlist_depth,
            "notes": existing.notes,
        }

    data = dict(payload)
    data["city"] = city
    data["state"] = state

    # Remove unsafe keys
    data.pop("id", None)
    data.pop("org_id", None)
    data.pop("updated_at", None)
    data.pop("scope", None)

    now = datetime.utcnow()

    if existing is None:
        row = JurisdictionRule(org_id=org_id, updated_at=now, **data)
        db.add(row)
        db.commit()
        db.refresh(row)
        after = {"id": row.id, "org_id": row.org_id, "city": row.city, "state": row.state}

        emit_audit(
            db,
            org_id=p.org_id,
            actor_user_id=p.user_id,
            action="jurisdiction.create",
            entity_type="JurisdictionRule",
            entity_id=str(row.id),
            before=None,
            after=after,
        )
        return {"ok": True, "id": row.id, "scope": "global" if row.org_id is None else "org"}

    # update existing
    for k, v in data.items():
        if hasattr(existing, k):
            setattr(existing, k, v)
    existing.updated_at = now
    db.commit()
    db.refresh(existing)

    after = {
        "id": existing.id,
        "org_id": existing.org_id,
        "city": existing.city,
        "state": existing.state,
        "rental_license_required": existing.rental_license_required,
        "inspection_frequency": existing.inspection_frequency,
        "processing_days": existing.processing_days,
        "tenant_waitlist_depth": existing.tenant_waitlist_depth,
        "notes": existing.notes,
    }

    emit_audit(
        db,
        org_id=p.org_id,
        actor_user_id=p.user_id,
        action="jurisdiction.update",
        entity_type="JurisdictionRule",
        entity_id=str(existing.id),
        before=before,
        after=after,
    )

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

    before = {"id": row.id, "org_id": row.org_id, "city": row.city, "state": row.state}
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

    return {"ok": True, "deleted_id": rid}
