from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import AuditEvent
from app.policy_models import JurisdictionProfile
from app.services.jurisdiction_completeness_service import profile_completeness_payload


NOTIFICATION_ENTITY_TYPE = "jurisdiction_profile"


@dataclass(frozen=True)
class JurisdictionNotification:
    action: str
    org_id: int | None
    entity_id: str
    message: str
    payload: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "org_id": self.org_id,
            "entity_id": self.entity_id,
            "message": self.message,
            "payload": self.payload,
        }


def _dumps(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, default=str)
    except Exception:
        return "{}"


def _scope_label(profile: JurisdictionProfile) -> str:
    city = (getattr(profile, "city", None) or "").strip()
    county = (getattr(profile, "county", None) or "").strip()
    state = (getattr(profile, "state", None) or "MI").strip().upper()
    pha_name = (getattr(profile, "pha_name", None) or "").strip()

    if city:
        return f"{city}, {state}"
    if county:
        return f"{county.title()} County, {state}"
    if pha_name:
        return pha_name
    return state


def _has_recent_matching_notification(
    db: Session,
    *,
    org_id: int | None,
    entity_id: str,
    action: str,
    stale_reason: str | None,
) -> bool:
    stmt = (
        select(AuditEvent)
        .where(AuditEvent.entity_type == NOTIFICATION_ENTITY_TYPE)
        .where(AuditEvent.entity_id == entity_id)
        .where(AuditEvent.action == action)
        .order_by(AuditEvent.id.desc())
        .limit(5)
    )

    if org_id is None:
        stmt = stmt.where(AuditEvent.org_id.is_(None))
    else:
        stmt = stmt.where(AuditEvent.org_id == int(org_id))

    rows = list(db.scalars(stmt).all())
    for row in rows:
        payload = {}
        try:
            payload = json.loads(getattr(row, "after_json", None) or "{}")
        except Exception:
            payload = {}
        if str(payload.get("stale_reason") or "") == str(stale_reason or ""):
            return True
    return False


def build_stale_jurisdiction_notification(
    db: Session,
    *,
    profile: JurisdictionProfile,
) -> JurisdictionNotification:
    scope_label = _scope_label(profile)
    payload = profile_completeness_payload(db, profile)

    if profile.stale_reason:
        message = f"Jurisdiction data for {scope_label} is stale ({profile.stale_reason})."
    else:
        message = f"Jurisdiction data for {scope_label} is stale."

    return JurisdictionNotification(
        action="jurisdiction_stale",
        org_id=getattr(profile, "org_id", None),
        entity_id=str(getattr(profile, "id")),
        message=message,
        payload=payload,
    )


def create_notification_audit_event(
    db: Session,
    *,
    notification: JurisdictionNotification,
) -> AuditEvent:
    row = AuditEvent(
        org_id=int(notification.org_id) if notification.org_id is not None else None,
        actor_user_id=None,
        action=notification.action,
        entity_type=NOTIFICATION_ENTITY_TYPE,
        entity_id=str(notification.entity_id),
        before_json=None,
        after_json=_dumps(
            {
                "message": notification.message,
                **notification.payload,
            }
        ),
        created_at=datetime.utcnow(),
    )
    db.add(row)
    db.flush()
    return row


def notify_if_jurisdiction_stale(
    db: Session,
    *,
    profile: JurisdictionProfile,
    force: bool = False,
) -> dict[str, Any]:
    if not bool(getattr(profile, "is_stale", False)):
        return {
            "ok": True,
            "created": False,
            "reason": "profile_not_stale",
            "jurisdiction_profile_id": int(profile.id),
        }

    entity_id = str(getattr(profile, "id"))
    org_id = getattr(profile, "org_id", None)
    stale_reason = getattr(profile, "stale_reason", None)

    if not force and _has_recent_matching_notification(
        db,
        org_id=org_id,
        entity_id=entity_id,
        action="jurisdiction_stale",
        stale_reason=stale_reason,
    ):
        return {
            "ok": True,
            "created": False,
            "reason": "duplicate_notification_suppressed",
            "jurisdiction_profile_id": int(profile.id),
        }

    notification = build_stale_jurisdiction_notification(db, profile=profile)
    event = create_notification_audit_event(db, notification=notification)
    db.commit()
    db.refresh(event)

    return {
        "ok": True,
        "created": True,
        "jurisdiction_profile_id": int(profile.id),
        "audit_event_id": int(event.id),
        "action": notification.action,
        "message": notification.message,
    }


def notify_stale_jurisdictions(
    db: Session,
    *,
    org_id: int | None = None,
    force: bool = False,
    limit: int | None = None,
) -> dict[str, Any]:
    stmt = (
        select(JurisdictionProfile)
        .where(JurisdictionProfile.is_stale.is_(True))
        .order_by(JurisdictionProfile.id.asc())
    )

    if org_id is not None:
        stmt = stmt.where(
            (JurisdictionProfile.org_id == int(org_id))
            | (JurisdictionProfile.org_id.is_(None))
        )

    if limit is not None:
        stmt = stmt.limit(max(1, int(limit)))

    rows = list(db.scalars(stmt).all())

    created = 0
    skipped = 0
    results: list[dict[str, Any]] = []

    for profile in rows:
        result = notify_if_jurisdiction_stale(
            db,
            profile=profile,
            force=force,
        )
        results.append(result)
        if result.get("created"):
            created += 1
        else:
            skipped += 1

    return {
        "ok": True,
        "org_id": org_id,
        "processed_count": len(rows),
        "created_count": created,
        "skipped_count": skipped,
        "results": results,
    }




def build_inspection_reminder_notification(
    *,
    org_id: int | None,
    inspection_id: int,
    property_id: int | None,
    scheduled_for: datetime | None,
    inspector_name: str | None,
    reminder_offset_minutes: int,
) -> JurisdictionNotification:
    when_text = scheduled_for.isoformat() if scheduled_for is not None else None
    return JurisdictionNotification(
        action="inspection_reminder_ready",
        org_id=org_id,
        entity_id=str(inspection_id),
        message=(
            f"Inspection reminder ready for property {property_id or 'unknown'} "
            f"({reminder_offset_minutes} minutes before appointment)."
        ),
        payload={
            "inspection_id": int(inspection_id),
            "property_id": int(property_id) if property_id is not None else None,
            "scheduled_for": when_text,
            "inspector_name": inspector_name,
            "reminder_offset_minutes": int(reminder_offset_minutes),
        },
    )


def create_inspection_reminder_audit_event(
    db: Session,
    *,
    org_id: int | None,
    inspection_id: int,
    property_id: int | None,
    scheduled_for: datetime | None,
    inspector_name: str | None,
    reminder_offset_minutes: int,
) -> AuditEvent:
    notification = build_inspection_reminder_notification(
        org_id=org_id,
        inspection_id=inspection_id,
        property_id=property_id,
        scheduled_for=scheduled_for,
        inspector_name=inspector_name,
        reminder_offset_minutes=reminder_offset_minutes,
    )
    row = create_notification_audit_event(db, notification=notification)
    return row


# ---- Chunk 5 notification enrichments ----
_base_build_stale_jurisdiction_notification = build_stale_jurisdiction_notification


def build_stale_jurisdiction_notification(
    db: Session,
    *,
    profile: JurisdictionProfile,
) -> JurisdictionNotification:
    note = _base_build_stale_jurisdiction_notification(db, profile=profile)
    payload = dict(note.payload)
    payload['coverage_confidence'] = payload.get('coverage_confidence') or ('low' if payload.get('is_stale') else 'medium')
    payload['missing_local_rule_areas'] = list(payload.get('missing_local_rule_areas') or payload.get('missing_categories') or [])
    payload['stale_warning'] = True
    return JurisdictionNotification(
        action=note.action,
        org_id=note.org_id,
        entity_id=note.entity_id,
        message=note.message,
        payload=payload,
    )
