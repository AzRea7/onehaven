from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import AuditEvent
from app.policy_models import JurisdictionProfile, PolicyAssertion, PolicySource
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


def _utcnow() -> datetime:
    return datetime.utcnow()


def _dumps(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, default=str)
    except Exception:
        return "{}"


def _scope_label_from_values(
    *,
    state: str | None,
    county: str | None,
    city: str | None,
    pha_name: str | None,
) -> str:
    city_val = (city or "").strip()
    county_val = (county or "").strip()
    state_val = (state or "MI").strip().upper()
    pha_val = (pha_name or "").strip()

    if city_val:
        return f"{city_val}, {state_val}"
    if county_val:
        return f"{county_val.title()} County, {state_val}"
    if pha_val:
        return pha_val
    return state_val


def _scope_label(profile: JurisdictionProfile) -> str:
    return _scope_label_from_values(
        state=getattr(profile, "state", None),
        county=getattr(profile, "county", None),
        city=getattr(profile, "city", None),
        pha_name=getattr(profile, "pha_name", None),
    )


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


def _build_base_stale_jurisdiction_notification(
    db: Session,
    *,
    profile: JurisdictionProfile,
) -> JurisdictionNotification:
    scope_label = _scope_label(profile)
    payload = profile_completeness_payload(db, profile)

    if getattr(profile, "stale_reason", None):
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


def build_stale_jurisdiction_notification(
    db: Session,
    *,
    profile: JurisdictionProfile,
) -> JurisdictionNotification:
    note = _build_base_stale_jurisdiction_notification(db, profile=profile)
    payload = dict(note.payload)
    payload["coverage_confidence"] = payload.get("coverage_confidence") or ("low" if payload.get("is_stale") else "medium")
    payload["missing_local_rule_areas"] = list(payload.get("missing_local_rule_areas") or payload.get("missing_categories") or [])
    payload["stale_warning"] = True
    payload["stale_reason"] = getattr(profile, "stale_reason", None)
    return JurisdictionNotification(
        action=note.action,
        org_id=note.org_id,
        entity_id=note.entity_id,
        message=note.message,
        payload=payload,
    )


def create_notification_audit_event(
    db: Session,
    *,
    notification: JurisdictionNotification,
    entity_type: str = NOTIFICATION_ENTITY_TYPE,
) -> AuditEvent:
    row = AuditEvent(
        org_id=int(notification.org_id) if notification.org_id is not None else None,
        actor_user_id=None,
        action=notification.action,
        entity_type=entity_type,
        entity_id=str(notification.entity_id),
        before_json=None,
        after_json=_dumps(
            {
                "message": notification.message,
                **notification.payload,
            }
        ),
        created_at=_utcnow(),
    )
    db.add(row)
    db.flush()
    return row


def record_notification_event(
    db: Session,
    *,
    payload: dict[str, Any],
) -> dict[str, Any]:
    entity_type = str(payload.get("entity_type") or NOTIFICATION_ENTITY_TYPE)
    entity_id = str(
        payload.get("entity_id")
        or payload.get("jurisdiction_profile_id")
        or payload.get("source_id")
        or payload.get("inspection_id")
        or "unknown"
    )
    action = str(payload.get("kind") or payload.get("action") or "jurisdiction_event")
    message = str(payload.get("message") or payload.get("title") or action)

    notification = JurisdictionNotification(
        action=action,
        org_id=payload.get("org_id"),
        entity_id=entity_id,
        message=message,
        payload=payload,
    )
    event = create_notification_audit_event(db, notification=notification, entity_type=entity_type)
    db.commit()
    db.refresh(event)
    return {
        "ok": True,
        "recorded": True,
        "audit_event_id": int(event.id),
        "payload": payload,
    }


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


def build_source_refresh_notification(
    *,
    source: PolicySource,
    refresh_result: dict[str, Any],
) -> dict[str, Any]:
    changed = bool(refresh_result.get("changed"))
    ok = bool(refresh_result.get("ok"))
    stale = (getattr(source, "freshness_status", None) or "").lower() == "stale"

    level = "info"
    if not ok:
        level = "error"
    elif changed or stale:
        level = "warning"

    return {
        "kind": "policy_source_refresh",
        "level": level,
        "entity_type": "policy_source",
        "entity_id": str(getattr(source, "id", "unknown")),
        "source_id": int(source.id),
        "org_id": getattr(source, "org_id", None),
        "jurisdiction_slug": getattr(source, "jurisdiction_slug", None),
        "title": getattr(source, "title", None),
        "message": f"Policy source refresh for {getattr(source, 'title', None) or getattr(source, 'url', None)}",
        "url": getattr(source, "url", None),
        "source_type": getattr(source, "source_type", None),
        "status": getattr(source, "freshness_status", None),
        "changed": changed,
        "ok": ok,
        "reason": refresh_result.get("fetch_error") or refresh_result.get("reason"),
        "previous_fingerprint": refresh_result.get("previous_fingerprint"),
        "current_fingerprint": refresh_result.get("current_fingerprint"),
        "created_at": _utcnow().isoformat(),
        "payload": refresh_result,
    }


def build_rule_change_notification(
    *,
    source: PolicySource,
    governance_result: dict[str, Any],
) -> dict[str, Any]:
    active_count = int(governance_result.get("active_count", 0) or 0)
    replaced_count = int(governance_result.get("replaced_count", 0) or 0)
    approved_count = int(governance_result.get("approved_count", 0) or 0)
    changed_count = active_count + replaced_count

    level = "warning" if changed_count > 0 else "info"

    return {
        "kind": "jurisdiction_rule_change",
        "level": level,
        "entity_type": "policy_source",
        "entity_id": str(getattr(source, "id", "unknown")),
        "source_id": int(source.id),
        "org_id": getattr(source, "org_id", None),
        "jurisdiction_slug": getattr(source, "jurisdiction_slug", None),
        "title": getattr(source, "title", None),
        "message": f"Jurisdiction rules changed for source {getattr(source, 'title', None) or getattr(source, 'url', None)}",
        "active_count": active_count,
        "replaced_count": replaced_count,
        "approved_count": approved_count,
        "changed": changed_count > 0,
        "created_at": _utcnow().isoformat(),
        "payload": governance_result,
    }


def build_stale_source_notification(
    *,
    source: PolicySource,
) -> dict[str, Any]:
    return {
        "kind": "stale_policy_source",
        "level": "warning",
        "entity_type": "policy_source",
        "entity_id": str(getattr(source, "id", "unknown")),
        "source_id": int(source.id),
        "org_id": getattr(source, "org_id", None),
        "jurisdiction_slug": getattr(source, "jurisdiction_slug", None),
        "title": getattr(source, "title", None),
        "message": f"Policy source is stale: {getattr(source, 'title', None) or getattr(source, 'url', None)}",
        "url": getattr(source, "url", None),
        "freshness_status": getattr(source, "freshness_status", None),
        "last_fetched_at": getattr(source, "last_fetched_at", None).isoformat() if getattr(source, "last_fetched_at", None) else None,
        "last_verified_at": getattr(source, "last_verified_at", None).isoformat() if getattr(source, "last_verified_at", None) else None,
        "next_refresh_due_at": getattr(source, "next_refresh_due_at", None).isoformat() if getattr(source, "next_refresh_due_at", None) else None,
        "created_at": _utcnow().isoformat(),
    }


def build_jurisdiction_profile_stale_notification(
    *,
    profile: JurisdictionProfile,
) -> dict[str, Any]:
    return {
        "kind": "jurisdiction_profile_stale",
        "level": "warning",
        "entity_type": NOTIFICATION_ENTITY_TYPE,
        "entity_id": str(getattr(profile, "id", "unknown")),
        "jurisdiction_profile_id": int(profile.id),
        "org_id": getattr(profile, "org_id", None),
        "title": f"Jurisdiction profile stale: {_scope_label(profile)}",
        "message": getattr(profile, "stale_reason", None) or "Jurisdiction profile needs refresh.",
        "state": getattr(profile, "state", None),
        "county": getattr(profile, "county", None),
        "city": getattr(profile, "city", None),
        "pha_name": getattr(profile, "pha_name", None),
        "last_verified_at": getattr(profile, "last_verified_at", None).isoformat() if getattr(profile, "last_verified_at", None) else None,
        "last_refresh_success_at": getattr(profile, "last_refresh_success_at", None).isoformat() if getattr(profile, "last_refresh_success_at", None) else None,
        "stale_reason": getattr(profile, "stale_reason", None),
        "created_at": _utcnow().isoformat(),
    }


def build_review_queue_payload(
    db: Session | None = None,
    *,
    org_id: int | None = None,
    state: str | None = None,
    county: str | None = None,
    city: str | None = None,
    pha_name: str | None = None,
    assertions: list[PolicyAssertion] | None = None,
) -> dict[str, Any]:
    rows = list(assertions or [])

    if db is not None and not rows and state is not None:
        stmt = select(PolicyAssertion).where(PolicyAssertion.state == (state or "MI").strip().upper())
        if org_id is None:
            stmt = stmt.where(PolicyAssertion.org_id.is_(None))
        else:
            stmt = stmt.where((PolicyAssertion.org_id == int(org_id)) | (PolicyAssertion.org_id.is_(None)))
        rows = list(db.scalars(stmt).all())

    draft_ids: list[int] = []
    approved_ids: list[int] = []
    stale_ids: list[int] = []
    changed_candidate_ids: list[int] = []

    norm_county = (county or "").strip().lower() or None
    norm_city = (city or "").strip().lower() or None
    norm_pha = (pha_name or "").strip() or None

    scoped_rows: list[PolicyAssertion] = []
    for row in rows:
        if state is not None and (getattr(row, "state", None) or "MI").strip().upper() != (state or "MI").strip().upper():
            continue
        if norm_county is not None and (getattr(row, "county", None) or None) != norm_county:
            continue
        if norm_city is not None and (getattr(row, "city", None) or None) != norm_city:
            continue
        row_pha = getattr(row, "pha_name", None) or None
        if norm_pha is not None and row_pha != norm_pha:
            continue
        scoped_rows.append(row)

    for row in scoped_rows:
        gov = str(getattr(row, "governance_state", "") or "").lower()
        review_status = str(getattr(row, "review_status", "") or "").lower()
        change_summary = str(getattr(row, "change_summary", "") or "").lower()

        if gov == "draft":
            draft_ids.append(int(row.id))
        if gov == "approved":
            approved_ids.append(int(row.id))
        if review_status == "stale":
            stale_ids.append(int(row.id))
        if "change" in change_summary or "new_candidate" in change_summary:
            changed_candidate_ids.append(int(row.id))

    return {
        "org_id": org_id,
        "state": (state or "MI").strip().upper() if state is not None else None,
        "county": norm_county,
        "city": norm_city,
        "pha_name": norm_pha,
        "draft_count": len(draft_ids),
        "draft_ids": draft_ids,
        "approved_count": len(approved_ids),
        "approved_ids": approved_ids,
        "stale_count": len(stale_ids),
        "stale_ids": stale_ids,
        "changed_candidate_count": len(changed_candidate_ids),
        "changed_candidate_ids": changed_candidate_ids,
    }