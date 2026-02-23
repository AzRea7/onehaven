from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..domain.audit import audit_write
from ..domain.jurisdiction_defaults import defaults_for_michigan
from ..models import JurisdictionRule


def _norm_city(s: str) -> str:
    return (s or "").strip().title()


def _norm_state(s: str) -> str:
    return (s or "MI").strip().upper()


def ensure_seeded_for_org(db: Session, *, org_id: int) -> dict[str, Any]:
    """
    DoD requirement: new orgs must not start with empty jurisdiction rules.

    Strategy:
      - If org already has any rules: do nothing.
      - Else: copy the global defaults (org_id IS NULL) into org-scoped rows.
      - If global defaults don't exist, build from python defaults_for_michigan().
    """
    existing = db.scalar(
        select(JurisdictionRule.id).where(JurisdictionRule.org_id == org_id).limit(1)
    )
    if existing is not None:
        return {"seeded": False, "reason": "org already has jurisdiction rules"}

    globals_ = (
        db.execute(select(JurisdictionRule).where(JurisdictionRule.org_id.is_(None)))
        .scalars()
        .all()
    )

    created = 0
    now = datetime.utcnow()

    if globals_:
        for g in globals_:
            row = JurisdictionRule(
                org_id=org_id,
                city=_norm_city(g.city),
                state=_norm_state(g.state),
                rental_license_required=bool(g.rental_license_required),
                inspection_authority=g.inspection_authority,
                inspection_frequency=g.inspection_frequency,
                typical_fail_points_json=g.typical_fail_points_json,
                processing_days=g.processing_days,
                tenant_waitlist_depth=g.tenant_waitlist_depth,
                notes=g.notes,
                created_at=now,
                updated_at=now,
            )
            db.add(row)
            created += 1
    else:
        for d in defaults_for_michigan():
            # d is JurisdictionDefault dataclass
            row = JurisdictionRule(
                org_id=org_id,
                city=_norm_city(d.city),
                state=_norm_state(getattr(d, "state", "MI")),
                rental_license_required=bool(d.rental_license_required),
                inspection_authority=d.inspection_authority,
                inspection_frequency=d.inspection_frequency,
                typical_fail_points_json=json.dumps(list(d.typical_fail_points or [])),
                processing_days=d.processing_days,
                tenant_waitlist_depth=d.tenant_waitlist_depth,
                notes=d.notes,
                created_at=now,
                updated_at=now,
            )
            db.add(row)
            created += 1

    db.commit()
    return {"seeded": True, "created": int(created)}


def _jr_to_dict(jr: JurisdictionRule) -> dict[str, Any]:
    return {
        "id": int(jr.id),
        "org_id": int(jr.org_id) if jr.org_id is not None else None,
        "city": jr.city,
        "state": jr.state,
        "rental_license_required": bool(jr.rental_license_required),
        "inspection_authority": jr.inspection_authority,
        "inspection_frequency": jr.inspection_frequency,
        "typical_fail_points_json": jr.typical_fail_points_json,
        "processing_days": jr.processing_days,
        "tenant_waitlist_depth": jr.tenant_waitlist_depth,
        "notes": jr.notes,
    }


def create_rule(
    db: Session,
    *,
    org_id: int,
    actor_user_id: Optional[int],
    payload: dict[str, Any],
) -> JurisdictionRule:
    now = datetime.utcnow()
    row = JurisdictionRule(
        org_id=org_id,
        city=_norm_city(payload.get("city", "")),
        state=_norm_state(payload.get("state", "MI")),
        rental_license_required=bool(payload.get("rental_license_required", False)),
        inspection_authority=payload.get("inspection_authority"),
        inspection_frequency=payload.get("inspection_frequency"),
        typical_fail_points_json=payload.get("typical_fail_points_json"),
        processing_days=payload.get("processing_days"),
        tenant_waitlist_depth=payload.get("tenant_waitlist_depth"),
        notes=payload.get("notes"),
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.commit()
    db.refresh(row)

    audit_write(
        db,
        org_id=org_id,
        actor_user_id=actor_user_id,
        action="jurisdiction_rule_created",
        entity_type="jurisdiction_rule",
        entity_id=str(row.id),
        before=None,
        after=_jr_to_dict(row),
        commit=True,
    )
    return row


def update_rule(
    db: Session,
    *,
    org_id: int,
    actor_user_id: Optional[int],
    rule_id: int,
    payload: dict[str, Any],
) -> JurisdictionRule:
    row = db.get(JurisdictionRule, rule_id)
    if row is None or row.org_id != org_id:
        raise ValueError("jurisdiction rule not found")

    before = _jr_to_dict(row)

    if "city" in payload and payload["city"] is not None:
        row.city = _norm_city(payload["city"])
    if "state" in payload and payload["state"] is not None:
        row.state = _norm_state(payload["state"])
    if "rental_license_required" in payload and payload["rental_license_required"] is not None:
        row.rental_license_required = bool(payload["rental_license_required"])
    if "inspection_authority" in payload:
        row.inspection_authority = payload["inspection_authority"]
    if "inspection_frequency" in payload:
        row.inspection_frequency = payload["inspection_frequency"]
    if "typical_fail_points_json" in payload:
        row.typical_fail_points_json = payload["typical_fail_points_json"]
    if "processing_days" in payload:
        row.processing_days = payload["processing_days"]
    if "tenant_waitlist_depth" in payload:
        row.tenant_waitlist_depth = payload["tenant_waitlist_depth"]
    if "notes" in payload:
        row.notes = payload["notes"]

    row.updated_at = datetime.utcnow()
    db.add(row)
    db.commit()
    db.refresh(row)

    audit_write(
        db,
        org_id=org_id,
        actor_user_id=actor_user_id,
        action="jurisdiction_rule_updated",
        entity_type="jurisdiction_rule",
        entity_id=str(row.id),
        before=before,
        after=_jr_to_dict(row),
        commit=True,
    )
    return row


def delete_rule(
    db: Session,
    *,
    org_id: int,
    actor_user_id: Optional[int],
    rule_id: int,
) -> None:
    row = db.get(JurisdictionRule, rule_id)
    if row is None or row.org_id != org_id:
        raise ValueError("jurisdiction rule not found")

    before = _jr_to_dict(row)

    db.delete(row)
    db.commit()

    audit_write(
        db,
        org_id=org_id,
        actor_user_id=actor_user_id,
        action="jurisdiction_rule_deleted",
        entity_type="jurisdiction_rule",
        entity_id=str(rule_id),
        before=before,
        after=None,
        commit=True,
    )