# backend/app/routers/jurisdictions.py
from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, or_, desc, func
from sqlalchemy.orm import Session

from ..auth import get_principal, require_owner
from ..db import get_db
from ..models import JurisdictionRule, Property
from ..domain.audit import emit_audit
from ..domain.jurisdiction_defaults import michigan_global_defaults

router = APIRouter(prefix="/jurisdictions", tags=["jurisdictions"])


def _norm_city(v: str) -> str:
    return (v or "").strip().title()


def _norm_state(v: str) -> str:
    s = (v or "MI").strip().upper()
    return s if len(s) == 2 else "MI"


def _has_col(model, name: str) -> bool:
    """
    True if SQLAlchemy model has this mapped attribute/column.
    Works even if DB is behind, as long as model is consistent with mapper.
    """
    return hasattr(model, name)


def _row_to_dict(r: JurisdictionRule) -> dict:
    # IMPORTANT: do not directly access columns that might not exist in DB/schema
    return {
        "id": r.id,
        "scope": "global" if r.org_id is None else "org",
        "org_id": r.org_id,
        "city": r.city,
        "state": r.state,
        "rental_license_required": r.rental_license_required,
        "inspection_authority": getattr(r, "inspection_authority", None),
        "inspection_frequency": getattr(r, "inspection_frequency", None),
        "typical_fail_points_json": getattr(r, "typical_fail_points_json", None),
        "registration_fee": getattr(r, "registration_fee", None),
        "fees_json": getattr(r, "fees_json", None),
        "processing_days": getattr(r, "processing_days", None),
        "tenant_waitlist_depth": getattr(r, "tenant_waitlist_depth", None),
        # ✅ FIX: notes may not exist in your DB
        "notes": getattr(r, "notes", None),
        "updated_at": r.updated_at.isoformat() if getattr(r, "updated_at", None) else None,
        "created_at": r.created_at.isoformat() if getattr(r, "created_at", None) else None,
    }


@router.get("/rules", response_model=list[dict])
def list_rules(
    city: Optional[str] = Query(default=None),
    state: str = Query(default="MI"),
    scope: str = Query(default="all", description="all|org|global"),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
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
    return [_row_to_dict(r) for r in rows]


@router.get("/rule", response_model=dict)
def get_effective_rule(
    city: str,
    state: str = "MI",
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
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

    # Never allow these from client
    for k in ["id", "org_id", "updated_at", "created_at", "scope"]:
        data.pop(k, None)

    # ✅ CRITICAL FIX: if your table doesn't have notes, do not write it
    if not _has_col(JurisdictionRule, "notes"):
        data.pop("notes", None)

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


@router.post("/seed", response_model=dict)
def seed_michigan_defaults(
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _owner=Depends(require_owner),
):
    """
    Seeds GLOBAL (org_id NULL) defaults.
    Idempotent: inserts missing city/state rows only.

    ✅ FIX: do NOT insert columns that don't exist in your DB (e.g., notes).
    """
    now = datetime.utcnow()
    created = 0

    allow_notes = _has_col(JurisdictionRule, "notes")

    for d in michigan_global_defaults():
        row_kwargs = d.to_row_kwargs()
        city = _norm_city(row_kwargs.get("city", ""))
        state = _norm_state(row_kwargs.get("state", "MI"))
        if not city:
            continue

        exists = db.scalar(
            select(JurisdictionRule).where(
                JurisdictionRule.org_id.is_(None),
                JurisdictionRule.city == city,
                JurisdictionRule.state == state,
            )
        )
        if exists:
            continue

        # Build insert kwargs safely
        insert_kwargs = dict(
            org_id=None,
            city=city,
            state=state,
            rental_license_required=bool(row_kwargs.get("rental_license_required", False)),
            inspection_authority=row_kwargs.get("inspection_authority"),
            inspection_frequency=row_kwargs.get("inspection_frequency"),
            typical_fail_points_json=row_kwargs.get("typical_fail_points_json") or "[]",
            registration_fee=row_kwargs.get("registration_fee"),
            processing_days=row_kwargs.get("processing_days"),
            tenant_waitlist_depth=row_kwargs.get("tenant_waitlist_depth"),
            created_at=now,
            updated_at=now,
        )
        if allow_notes:
            insert_kwargs["notes"] = row_kwargs.get("notes")

        row = JurisdictionRule(**insert_kwargs)
        db.add(row)
        created += 1

    db.commit()
    return {"ok": True, "created": created}


@router.get("/coverage", response_model=dict)
def coverage(
    state: str = Query(default="MI"),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    """
    Shows which (city,state) pairs exist in your org's portfolio but lack an org-specific rule.
    Also indicates whether a global fallback exists.
    """
    state = _norm_state(state)

    pairs = db.execute(
        select(func.lower(Property.city).label("city_lc"), Property.state)
        .where(Property.org_id == p.org_id, Property.state == state)
        .group_by(func.lower(Property.city), Property.state)
    ).all()

    rows = []
    for city_lc, st in pairs:
        city = _norm_city(city_lc)

        org_rule = db.scalar(
            select(JurisdictionRule).where(
                JurisdictionRule.org_id == p.org_id,
                JurisdictionRule.city == city,
                JurisdictionRule.state == st,
            )
        )
        global_rule = db.scalar(
            select(JurisdictionRule).where(
                JurisdictionRule.org_id.is_(None),
                JurisdictionRule.city == city,
                JurisdictionRule.state == st,
            )
        )

        provenance = "org" if org_rule else ("global" if global_rule else "missing")
        rows.append(
            {
                "city": city,
                "state": st,
                "has_org_rule": bool(org_rule),
                "has_global_fallback": bool(global_rule),
                "provenance": provenance,
            }
        )

    missing = [r for r in rows if r["provenance"] == "missing"]
    return {
        "state": state,
        "total_pairs": len(rows),
        "missing_rules": len(missing),
        "rows": sorted(rows, key=lambda r: (r["provenance"], r["city"])),
    }