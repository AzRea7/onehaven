# backend/app/services/jurisdiction_notification_service.py
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.models import AuditEvent
from app.policy_models import JurisdictionProfile, PolicyAssertion, PolicySource, PropertyComplianceProjection
from app.services.jurisdiction_completeness_service import profile_completeness_payload


NOTIFICATION_ENTITY_TYPE = "jurisdiction_profile"




@dataclass(frozen=True)
class JurisdictionReviewQueueEntry:
    jurisdiction_profile_id: int
    org_id: int | None
    state: str | None
    county: str | None
    city: str | None
    pha_name: str | None
    scope_label: str
    severity: str
    severity_rank: int
    queue_status: str
    decision_needed: str
    changed_sources: list[dict[str, Any]]
    failed_validations: list[dict[str, Any]]
    review_required_categories: list[str]
    conflicting_categories: list[str]
    source_change_count: int
    validation_failure_count: int
    expires_at: str | None
    reviewer_action: str | None
    reviewer_rationale: str | None
    reviewed_by_user_id: int | None
    reviewed_at: str | None
    payload: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "jurisdiction_profile_id": self.jurisdiction_profile_id,
            "org_id": self.org_id,
            "state": self.state,
            "county": self.county,
            "city": self.city,
            "pha_name": self.pha_name,
            "scope_label": self.scope_label,
            "severity": self.severity,
            "severity_rank": self.severity_rank,
            "queue_status": self.queue_status,
            "decision_needed": self.decision_needed,
            "changed_sources": self.changed_sources,
            "failed_validations": self.failed_validations,
            "review_required_categories": self.review_required_categories,
            "conflicting_categories": self.conflicting_categories,
            "source_change_count": self.source_change_count,
            "validation_failure_count": self.validation_failure_count,
            "expires_at": self.expires_at,
            "reviewer_action": self.reviewer_action,
            "reviewer_rationale": self.reviewer_rationale,
            "reviewed_by_user_id": self.reviewed_by_user_id,
            "reviewed_at": self.reviewed_at,
            "payload": self.payload,
        }

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


def _loads(value: Any, default: Any) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        parsed = json.loads(value)
        return parsed if parsed is not None else default
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


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


def _has_recent_matching_escalation(
    db: Session,
    *,
    org_id: int | None,
    entity_id: str,
    escalation_code: str,
) -> bool:
    stmt = (
        select(AuditEvent)
        .where(AuditEvent.entity_type == NOTIFICATION_ENTITY_TYPE)
        .where(AuditEvent.entity_id == entity_id)
        .where(
            AuditEvent.action.in_(
                [
                    "jurisdiction_gap_escalation",
                    "jurisdiction_review_queue_entry",
                ]
            )
        )
        .order_by(AuditEvent.id.desc())
        .limit(10)
    )

    if org_id is None:
        stmt = stmt.where(AuditEvent.org_id.is_(None))
    else:
        stmt = stmt.where(AuditEvent.org_id == int(org_id))

    rows = list(db.scalars(stmt).all())
    for row in rows:
        payload = _loads(getattr(row, "after_json", None), {})
        if str(payload.get("escalation_code") or "") == escalation_code:
            return True
    return False


def _critical_categories(payload: dict[str, Any]) -> list[str]:
    raw = payload.get("critical_categories") or []
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    return []


def _list_overlap(left: list[str], right: list[str]) -> list[str]:
    right_set = {str(x).strip() for x in right if str(x).strip()}
    return [str(x).strip() for x in left if str(x).strip() in right_set]


def _discovery_retry_exhausted(profile: JurisdictionProfile, payload: dict[str, Any]) -> bool:
    source_freshness = _loads(getattr(profile, "source_freshness_json", None), {})
    meta = payload.get("source_freshness") or {}
    candidates = [
        source_freshness.get("discovery_retries_exhausted"),
        source_freshness.get("retry_exhausted"),
        source_freshness.get("retries_exhausted"),
        meta.get("discovery_retries_exhausted"),
        meta.get("retry_exhausted"),
        meta.get("retries_exhausted"),
    ]
    for candidate in candidates:
        if candidate is True:
            return True

    retry_count = _safe_int(source_freshness.get("discovery_retry_count"), 0)
    max_retry_count = _safe_int(
        source_freshness.get("discovery_retry_max")
        or source_freshness.get("discovery_max_retries")
        or 0,
        0,
    )
    return max_retry_count > 0 and retry_count >= max_retry_count


def _authoritative_conflict_present(payload: dict[str, Any]) -> bool:
    conflicting_categories = list(payload.get("conflicting_categories") or [])
    category_details = payload.get("category_details") or {}
    if not conflicting_categories:
        return False

    for category in conflicting_categories:
        detail = category_details.get(category) or {}
        if _safe_int(detail.get("authoritative_source_count"), 0) > 0:
            return True
    return bool(conflicting_categories)


def evaluate_jurisdiction_gap_escalations(
    db: Session,
    *,
    profile: JurisdictionProfile,
) -> dict[str, Any]:
    payload = profile_completeness_payload(db, profile)
    scope_label = _scope_label(profile)

    critical_categories = _critical_categories(payload)
    missing_categories = list(payload.get("missing_categories") or [])
    stale_categories = list(payload.get("stale_categories") or [])
    conflicting_categories = list(payload.get("conflicting_categories") or [])

    critical_missing = _list_overlap(critical_categories, missing_categories)
    critical_stale = _list_overlap(critical_categories, stale_categories)
    authoritative_conflict = _authoritative_conflict_present(payload)
    discovery_retries_exhausted = _discovery_retry_exhausted(profile, payload)

    escalation_reasons: list[dict[str, Any]] = []

    if critical_missing:
        escalation_reasons.append(
            {
                "code": "critical_category_missing",
                "severity": "high",
                "title": "Critical category missing",
                "message": f"{scope_label} is missing critical jurisdiction coverage.",
                "categories": critical_missing,
            }
        )

    if critical_stale:
        escalation_reasons.append(
            {
                "code": "critical_category_stale",
                "severity": "high",
                "title": "Critical category stale",
                "message": f"{scope_label} has stale coverage in critical categories.",
                "categories": critical_stale,
            }
        )

    if authoritative_conflict:
        escalation_reasons.append(
            {
                "code": "authoritative_conflict",
                "severity": "high",
                "title": "Authoritative conflict requires review",
                "message": f"{scope_label} has conflicting authoritative jurisdiction evidence.",
                "categories": conflicting_categories,
            }
        )

    if discovery_retries_exhausted:
        escalation_reasons.append(
            {
                "code": "discovery_retries_exhausted",
                "severity": "medium",
                "title": "Discovery retries exhausted",
                "message": f"{scope_label} exhausted automated discovery retries without closing coverage gaps.",
                "categories": [],
            }
        )

    needs_escalation = bool(escalation_reasons)

    return {
        "needs_escalation": needs_escalation,
        "scope_label": scope_label,
        "jurisdiction_profile_id": int(profile.id),
        "org_id": getattr(profile, "org_id", None),
        "state": getattr(profile, "state", None),
        "county": getattr(profile, "county", None),
        "city": getattr(profile, "city", None),
        "pha_name": getattr(profile, "pha_name", None),
        "completeness": payload,
        "critical_categories": critical_categories,
        "critical_missing_categories": critical_missing,
        "critical_stale_categories": critical_stale,
        "conflicting_categories": conflicting_categories,
        "authoritative_conflict": authoritative_conflict,
        "discovery_retries_exhausted": discovery_retries_exhausted,
        "escalation_reasons": escalation_reasons,
    }


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


def build_gap_escalation_notifications(
    db: Session,
    *,
    profile: JurisdictionProfile,
) -> list[JurisdictionNotification]:
    evaluation = evaluate_jurisdiction_gap_escalations(db, profile=profile)
    if not evaluation.get("needs_escalation"):
        return []

    notes: list[JurisdictionNotification] = []
    entity_id = str(getattr(profile, "id"))
    org_id = getattr(profile, "org_id", None)
    scope_label = evaluation["scope_label"]
    base_payload = {
        **evaluation["completeness"],
        "scope_label": scope_label,
        "needs_escalation": True,
    }

    for reason in evaluation["escalation_reasons"]:
        escalation_code = str(reason.get("code") or "jurisdiction_gap")
        categories = list(reason.get("categories") or [])
        notes.append(
            JurisdictionNotification(
                action="jurisdiction_gap_escalation",
                org_id=org_id,
                entity_id=entity_id,
                message=str(reason.get("message") or f"{scope_label} requires jurisdiction coverage review."),
                payload={
                    **base_payload,
                    "escalation_code": escalation_code,
                    "escalation_title": reason.get("title"),
                    "escalation_severity": reason.get("severity"),
                    "escalation_categories": categories,
                },
            )
        )

    return notes


def build_review_queue_entry(
    db: Session,
    *,
    profile: JurisdictionProfile,
) -> dict[str, Any]:
    evaluation = evaluate_jurisdiction_gap_escalations(db, profile=profile)
    completeness = evaluation["completeness"]
    escalation_reasons = list(evaluation.get("escalation_reasons") or [])

    highest_severity = "none"
    if any(str(item.get("severity")) == "high" for item in escalation_reasons):
        highest_severity = "high"
    elif any(str(item.get("severity")) == "medium" for item in escalation_reasons):
        highest_severity = "medium"

    return {
        "queue_entry_type": "jurisdiction_review",
        "jurisdiction_profile_id": int(profile.id),
        "org_id": getattr(profile, "org_id", None),
        "scope_label": evaluation["scope_label"],
        "state": getattr(profile, "state", None),
        "county": getattr(profile, "county", None),
        "city": getattr(profile, "city", None),
        "pha_name": getattr(profile, "pha_name", None),
        "needs_escalation": bool(evaluation.get("needs_escalation")),
        "priority": highest_severity,
        "completeness_score": completeness.get("completeness_score"),
        "completeness_status": completeness.get("completeness_status"),
        "confidence_label": completeness.get("confidence_label") or completeness.get("coverage_confidence"),
        "production_readiness": completeness.get("production_readiness"),
        "discovery_status": completeness.get("discovery_status"),
        "last_refresh": completeness.get("last_refresh") or completeness.get("last_refreshed"),
        "last_discovery_run": completeness.get("last_discovery_run"),
        "critical_categories": evaluation.get("critical_categories") or [],
        "critical_missing_categories": evaluation.get("critical_missing_categories") or [],
        "critical_stale_categories": evaluation.get("critical_stale_categories") or [],
        "conflicting_categories": evaluation.get("conflicting_categories") or [],
        "authoritative_conflict": bool(evaluation.get("authoritative_conflict")),
        "discovery_retries_exhausted": bool(evaluation.get("discovery_retries_exhausted")),
        "escalation_reasons": escalation_reasons,
    }


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
        or payload.get("property_id")
        or payload.get("projection_id")
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


def notify_unresolved_jurisdiction_gaps(
    db: Session,
    *,
    profile: JurisdictionProfile,
    force: bool = False,
) -> dict[str, Any]:
    notifications = build_gap_escalation_notifications(db, profile=profile)
    if not notifications:
        return {
            "ok": True,
            "created": False,
            "reason": "no_escalation_needed",
            "jurisdiction_profile_id": int(profile.id),
            "notifications": [],
        }

    created_events: list[dict[str, Any]] = []
    for notification in notifications:
        escalation_code = str(notification.payload.get("escalation_code") or "")
        if not force and _has_recent_matching_escalation(
            db,
            org_id=notification.org_id,
            entity_id=notification.entity_id,
            escalation_code=escalation_code,
        ):
            continue
        event = create_notification_audit_event(db, notification=notification)
        db.flush()
        created_events.append(
            {
                "audit_event_id": int(event.id),
                "action": notification.action,
                "message": notification.message,
                "escalation_code": escalation_code,
            }
        )

    if created_events:
        db.commit()

    return {
        "ok": True,
        "created": bool(created_events),
        "jurisdiction_profile_id": int(profile.id),
        "created_count": len(created_events),
        "notifications": created_events,
    }


def build_jurisdiction_review_queue(
    db: Session,
    *,
    org_id: int | None = None,
    state: str | None = None,
    county: str | None = None,
    city: str | None = None,
    pha_name: str | None = None,
    only_needs_escalation: bool = True,
    limit: int | None = None,
) -> dict[str, Any]:
    stmt = select(JurisdictionProfile).order_by(JurisdictionProfile.id.asc())

    if org_id is not None:
        stmt = stmt.where(
            or_(
                JurisdictionProfile.org_id == int(org_id),
                JurisdictionProfile.org_id.is_(None),
            )
        )
    if state is not None:
        stmt = stmt.where(JurisdictionProfile.state == (state or "MI").strip().upper())
    if county is not None:
        stmt = stmt.where(JurisdictionProfile.county == (county or "").strip().lower())
    if city is not None:
        stmt = stmt.where(JurisdictionProfile.city == (city or "").strip().lower())
    if pha_name is not None:
        stmt = stmt.where(JurisdictionProfile.pha_name == ((pha_name or "").strip() or None))
    if limit is not None:
        stmt = stmt.limit(max(1, int(limit)))

    rows = list(db.scalars(stmt).all())
    entries: list[dict[str, Any]] = []
    for profile in rows:
        entry = build_review_queue_entry(db, profile=profile)
        if only_needs_escalation and not entry.get("needs_escalation"):
            continue
        entries.append(entry)

    entries.sort(
        key=lambda row: (
            0 if row.get("priority") == "high" else 1 if row.get("priority") == "medium" else 2,
            float(row.get("completeness_score") or 0.0),
            int(row.get("jurisdiction_profile_id") or 0),
        )
    )

    return {
        "ok": True,
        "count": len(entries),
        "entries": entries,
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

    fingerprint = (
        refresh_result.get("current_fingerprint")
        or refresh_result.get("fingerprint")
        or refresh_result.get("content_sha256")
    )

    return {
        "kind": "policy_source_refresh",
        "entity_type": "policy_source",
        "entity_id": str(getattr(source, "id", "unknown")),
        "source_id": int(source.id),
        "org_id": getattr(source, "org_id", None),
        "jurisdiction_slug": getattr(source, "jurisdiction_slug", None),
        "message": f"Policy source refresh: {getattr(source, 'title', None) or source.url}",
        "changed": changed,
        "ok": ok,
        "fingerprint": fingerprint,
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


def _projection_rows_for_scope(
    db: Session,
    *,
    org_id: int | None,
    jurisdiction_slug: str | None = None,
    limit: int | None = None,
) -> list[PropertyComplianceProjection]:
    stmt = select(PropertyComplianceProjection).where(
        PropertyComplianceProjection.is_current.is_(True)
    )

    if org_id is None:
        stmt = stmt.where(PropertyComplianceProjection.org_id.is_(None))
    else:
        stmt = stmt.where(
            or_(
                PropertyComplianceProjection.org_id == int(org_id),
                PropertyComplianceProjection.org_id.is_(None),
            )
        )

    if jurisdiction_slug:
        stmt = stmt.where(PropertyComplianceProjection.jurisdiction_slug == jurisdiction_slug)

    stmt = stmt.order_by(PropertyComplianceProjection.id.asc())
    if limit is not None:
        stmt = stmt.limit(max(1, int(limit)))
    return list(db.scalars(stmt).all())


def build_property_rule_change_notification(
    *,
    property_projection: PropertyComplianceProjection,
    changed_rules: list[dict[str, Any]] | None = None,
    trigger_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    changed_rules = list(changed_rules or [])
    impacted_rules = _loads(getattr(property_projection, "impacted_rules_json", None), [])
    projection_reason = _loads(getattr(property_projection, "projection_reason_json", None), {})
    blocking_count = _safe_int(getattr(property_projection, "blocking_count", None))
    unknown_count = _safe_int(getattr(property_projection, "unknown_count", None))
    stale_count = _safe_int(getattr(property_projection, "stale_count", None))
    conflicting_count = _safe_int(getattr(property_projection, "conflicting_count", None))
    confidence_score = _safe_float(getattr(property_projection, "confidence_score", None), 0.0)

    severity = "info"
    if blocking_count > 0 or conflicting_count > 0:
        severity = "high"
    elif stale_count > 0 or unknown_count > 0 or confidence_score < 0.65:
        severity = "warning"

    if blocking_count > 0:
        reason = "Rule changes created active compliance blockers for this property."
    elif conflicting_count > 0:
        reason = "Rule changes introduced conflicting compliance evidence."
    elif stale_count > 0:
        reason = "Rule changes require stale proof to be refreshed."
    elif unknown_count > 0:
        reason = "Rule changes introduced unknown compliance requirements."
    elif confidence_score < 0.65:
        reason = "Rule changes reduced compliance confidence."
    else:
        reason = "Property should be re-evaluated after jurisdiction rule changes."

    return {
        "kind": "property_rule_change_impact",
        "level": severity,
        "entity_type": "property_compliance_projection",
        "entity_id": str(getattr(property_projection, "id", "unknown")),
        "projection_id": int(property_projection.id),
        "property_id": int(property_projection.property_id),
        "org_id": getattr(property_projection, "org_id", None),
        "jurisdiction_slug": getattr(property_projection, "jurisdiction_slug", None),
        "message": reason,
        "changed_rules": changed_rules,
        "impacted_rules": impacted_rules,
        "blocking_count": blocking_count,
        "unknown_count": unknown_count,
        "stale_count": stale_count,
        "conflicting_count": conflicting_count,
        "confidence_score": confidence_score,
        "projection_status": getattr(property_projection, "projection_status", None),
        "projected_compliance_cost": getattr(property_projection, "projected_compliance_cost", None),
        "projected_days_to_rent": getattr(property_projection, "projected_days_to_rent", None),
        "projection_reason": projection_reason,
        "trigger_payload": trigger_payload or {},
        "created_at": _utcnow().isoformat(),
    }


def build_post_close_reevaluation_trigger(
    *,
    property_projection: PropertyComplianceProjection,
    changed_rules: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    changed_rules = list(changed_rules or [])
    return {
        "kind": "property_post_close_recheck",
        "level": "warning",
        "entity_type": "property_compliance_projection",
        "entity_id": str(getattr(property_projection, "id", "unknown")),
        "projection_id": int(property_projection.id),
        "property_id": int(property_projection.property_id),
        "org_id": getattr(property_projection, "org_id", None),
        "jurisdiction_slug": getattr(property_projection, "jurisdiction_slug", None),
        "message": "Post-close compliance should be re-evaluated due to changed or stale jurisdiction rules.",
        "changed_rules": changed_rules,
        "blocking_count": _safe_int(getattr(property_projection, "blocking_count", None)),
        "unknown_count": _safe_int(getattr(property_projection, "unknown_count", None)),
        "stale_count": _safe_int(getattr(property_projection, "stale_count", None)),
        "conflicting_count": _safe_int(getattr(property_projection, "conflicting_count", None)),
        "confidence_score": _safe_float(getattr(property_projection, "confidence_score", None), 0.0),
        "created_at": _utcnow().isoformat(),
    }


def build_impacted_property_notifications(
    db: Session,
    *,
    org_id: int | None,
    jurisdiction_slug: str | None,
    changed_rules: list[dict[str, Any]] | None = None,
    trigger_payload: dict[str, Any] | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    rows = _projection_rows_for_scope(
        db,
        org_id=org_id,
        jurisdiction_slug=jurisdiction_slug,
        limit=limit,
    )
    notifications: list[dict[str, Any]] = []
    for row in rows:
        notifications.append(
            build_property_rule_change_notification(
                property_projection=row,
                changed_rules=changed_rules,
                trigger_payload=trigger_payload,
            )
        )
    return {
        "ok": True,
        "org_id": org_id,
        "jurisdiction_slug": jurisdiction_slug,
        "count": len(notifications),
        "notifications": notifications,
    }


def notify_impacted_properties_for_rule_change(
    db: Session,
    *,
    org_id: int | None,
    jurisdiction_slug: str | None,
    changed_rules: list[dict[str, Any]] | None = None,
    trigger_payload: dict[str, Any] | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    payload = build_impacted_property_notifications(
        db,
        org_id=org_id,
        jurisdiction_slug=jurisdiction_slug,
        changed_rules=changed_rules,
        trigger_payload=trigger_payload,
        limit=limit,
    )

    created = 0
    results: list[dict[str, Any]] = []
    for item in payload["notifications"]:
        result = record_notification_event(db, payload=item)
        results.append(result)
        if result.get("recorded"):
            created += 1

    return {
        "ok": True,
        "org_id": org_id,
        "jurisdiction_slug": jurisdiction_slug,
        "processed_count": len(payload["notifications"]),
        "created_count": created,
        "results": results,
    }

def build_critical_stale_lockout_notification(
    *,
    profile: JurisdictionProfile,
    categories: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "kind": "jurisdiction_lockout",
        "level": "error",
        "entity_type": NOTIFICATION_ENTITY_TYPE,
        "entity_id": str(getattr(profile, "id", "unknown")),
        "jurisdiction_profile_id": int(profile.id),
        "org_id": getattr(profile, "org_id", None),
        "title": f"Jurisdiction lockout: {_scope_label(profile)}",
        "message": "Critical authoritative compliance data exceeded freshness SLA and the jurisdiction has been locked.",
        "state": getattr(profile, "state", None),
        "county": getattr(profile, "county", None),
        "city": getattr(profile, "city", None),
        "pha_name": getattr(profile, "pha_name", None),
        "refresh_state": getattr(profile, "refresh_state", None),
        "refresh_status_reason": getattr(profile, "refresh_status_reason", None),
        "critical_stale_categories": list(categories or []),
        "created_at": _utcnow().isoformat(),
    }


def notify_if_profile_locked(
    db: Session,
    *,
    profile: JurisdictionProfile,
    categories: list[str] | None = None,
) -> dict[str, Any]:
    payload = build_critical_stale_lockout_notification(profile=profile, categories=categories)
    return record_notification_event(db, payload=payload)



def _review_queue_meta(profile: JurisdictionProfile) -> dict[str, Any]:
    meta = _loads(getattr(profile, "metadata_json", None), {})
    review = meta.get("jurisdiction_review_queue") or {}
    return review if isinstance(review, dict) else {}


def _set_review_queue_meta(profile: JurisdictionProfile, payload: dict[str, Any]) -> None:
    meta = _loads(getattr(profile, "metadata_json", None), {})
    if not isinstance(meta, dict):
        meta = {}
    meta["jurisdiction_review_queue"] = payload
    profile.metadata_json = _dumps(meta)


def _source_change_rows(db: Session, *, profile: JurisdictionProfile, limit: int = 8) -> list[dict[str, Any]]:
    stmt = select(PolicySource).where(PolicySource.state == getattr(profile, "state", None))
    if getattr(profile, "county", None) is None:
        stmt = stmt.where(PolicySource.county.is_(None))
    else:
        stmt = stmt.where(PolicySource.county == getattr(profile, "county", None))
    if getattr(profile, "city", None) is None:
        stmt = stmt.where(PolicySource.city.is_(None))
    else:
        stmt = stmt.where(PolicySource.city == getattr(profile, "city", None))
    if getattr(profile, "pha_name", None) is None:
        stmt = stmt.where(or_(PolicySource.pha_name.is_(None), PolicySource.pha_name == ""))
    else:
        stmt = stmt.where(PolicySource.pha_name == getattr(profile, "pha_name", None))
    if getattr(profile, "org_id", None) is None:
        stmt = stmt.where(PolicySource.org_id.is_(None))
    else:
        stmt = stmt.where(or_(PolicySource.org_id == getattr(profile, "org_id", None), PolicySource.org_id.is_(None)))
    rows = list(db.scalars(stmt.order_by(PolicySource.last_refresh_completed_at.desc().nullslast(), PolicySource.last_changed_at.desc().nullslast(), PolicySource.id.desc())).all())
    out=[]
    for row in rows:
        change_summary = _loads(getattr(row, "last_change_summary_json", None), {})
        refresh_outcome = _loads(getattr(row, "last_refresh_outcome_json", None), {})
        change_kind = str(change_summary.get("change_kind") or refresh_outcome.get("change_kind") or "").strip()
        refresh_state = str(getattr(row, "refresh_state", None) or "").strip().lower()
        if not change_kind and refresh_state not in {"failed", "blocked", "degraded", "validating"}:
            continue
        out.append({
            "source_id": int(getattr(row, "id", 0) or 0),
            "title": getattr(row, "title", None),
            "publisher": getattr(row, "publisher", None),
            "url": getattr(row, "url", None),
            "change_kind": change_kind or refresh_state or "changed",
            "refresh_state": getattr(row, "refresh_state", None),
            "refresh_status_reason": getattr(row, "refresh_status_reason", None),
            "validation_due_at": getattr(row, "validation_due_at", None).isoformat() if getattr(row, "validation_due_at", None) else None,
            "last_refresh_completed_at": getattr(row, "last_refresh_completed_at", None).isoformat() if getattr(row, "last_refresh_completed_at", None) else None,
        })
        if len(out) >= limit:
            break
    return out


def _validation_failure_rows(db: Session, *, profile: JurisdictionProfile, limit: int = 12) -> list[dict[str, Any]]:
    stmt = select(PolicyAssertion).where(PolicyAssertion.state == getattr(profile, "state", None))
    if hasattr(PolicyAssertion, "county"):
        if getattr(profile, "county", None) is None:
            stmt = stmt.where(PolicyAssertion.county.is_(None))
        else:
            stmt = stmt.where(PolicyAssertion.county == getattr(profile, "county", None))
    if hasattr(PolicyAssertion, "city"):
        if getattr(profile, "city", None) is None:
            stmt = stmt.where(PolicyAssertion.city.is_(None))
        else:
            stmt = stmt.where(PolicyAssertion.city == getattr(profile, "city", None))
    if hasattr(PolicyAssertion, "pha_name"):
        if getattr(profile, "pha_name", None) is None:
            stmt = stmt.where(or_(PolicyAssertion.pha_name.is_(None), PolicyAssertion.pha_name == ""))
        else:
            stmt = stmt.where(PolicyAssertion.pha_name == getattr(profile, "pha_name", None))
    if hasattr(PolicyAssertion, "org_id"):
        if getattr(profile, "org_id", None) is None:
            stmt = stmt.where(PolicyAssertion.org_id.is_(None))
        else:
            stmt = stmt.where(or_(PolicyAssertion.org_id == getattr(profile, "org_id", None), PolicyAssertion.org_id.is_(None)))
    rows = list(db.scalars(stmt.order_by(PolicyAssertion.reviewed_at.desc().nullslast(), PolicyAssertion.id.desc())).all())
    out=[]
    for row in rows:
        validation_state = str(getattr(row, "validation_state", None) or "").strip().lower()
        trust_state = str(getattr(row, "trust_state", None) or "").strip().lower()
        if validation_state not in {"conflicting", "unsupported", "ambiguous", "weak_support"} and trust_state not in {"needs_review", "downgraded"}:
            continue
        out.append({
            "assertion_id": int(getattr(row, "id", 0) or 0),
            "rule_key": getattr(row, "rule_key", None),
            "rule_category": getattr(row, "normalized_category", None) or getattr(row, "rule_category", None),
            "validation_state": getattr(row, "validation_state", None),
            "validation_reason": getattr(row, "validation_reason", None),
            "trust_state": getattr(row, "trust_state", None),
            "review_status": getattr(row, "review_status", None),
            "source_id": getattr(row, "source_id", None),
            "reviewed_at": getattr(row, "reviewed_at", None).isoformat() if getattr(row, "reviewed_at", None) else None,
        })
        if len(out) >= limit:
            break
    return out


def build_review_queue_entries(
    db: Session,
    *,
    org_id: int | None = None,
    state: str | None = None,
    county: str | None = None,
    city: str | None = None,
    pha_name: str | None = None,
) -> dict[str, Any]:
    stmt = select(JurisdictionProfile)
    if state:
        stmt = stmt.where(JurisdictionProfile.state == str(state).strip().upper())
    if county is not None:
        stmt = stmt.where(JurisdictionProfile.county == ((county or '').strip().lower() or None))
    if city is not None:
        stmt = stmt.where(JurisdictionProfile.city == ((city or '').strip().lower() or None))
    if pha_name is not None and hasattr(JurisdictionProfile, 'pha_name'):
        stmt = stmt.where(JurisdictionProfile.pha_name == ((pha_name or '').strip() or None))
    if org_id is None:
        stmt = stmt.where(JurisdictionProfile.org_id.is_(None))
    else:
        stmt = stmt.where(or_(JurisdictionProfile.org_id == int(org_id), JurisdictionProfile.org_id.is_(None)))
    profiles = list(db.scalars(stmt.order_by(JurisdictionProfile.id.desc())).all())
    entries=[]
    severity_counts={"high":0,"medium":0,"low":0}
    grouped={}
    for profile in profiles:
        evaluation = evaluate_jurisdiction_gap_escalations(db, profile=profile)
        completeness = evaluation.get("completeness") or {}
        lockout = completeness.get("lockout") or {}
        review_required_categories = sorted(set(list(completeness.get("supporting_only_categories") or []) + list(completeness.get("authority_unmet_categories") or []) + list(completeness.get("critical_stale_categories") or [])))
        conflicting_categories = list(evaluation.get("conflicting_categories") or [])
        changed_sources = _source_change_rows(db, profile=profile)
        failed_validations = _validation_failure_rows(db, profile=profile)
        needs_review = bool(evaluation.get("needs_escalation") or review_required_categories or conflicting_categories or changed_sources or failed_validations)
        if not needs_review:
            continue
        queue_meta = _review_queue_meta(profile)
        queue_status = str(queue_meta.get("queue_status") or "open").strip().lower() or "open"
        reviewer_action = queue_meta.get("reviewer_action")
        reviewer_rationale = queue_meta.get("reviewer_rationale")
        reviewed_by_user_id = _safe_int(queue_meta.get("reviewed_by_user_id"), 0) or None
        reviewed_at = queue_meta.get("reviewed_at")
        expires_at = queue_meta.get("expires_at")
        severity = "medium"
        if evaluation.get("critical_missing_categories") or evaluation.get("critical_stale_categories") or evaluation.get("authoritative_conflict") or bool(lockout.get("lockout_active")):
            severity = "high"
        elif changed_sources or failed_validations or evaluation.get("discovery_retries_exhausted"):
            severity = "medium"
        else:
            severity = "low"
        severity_rank = {"high":3,"medium":2,"low":1}.get(severity,1)
        decision_needed = "Resolve conflicting authority and choose governing rule" if conflicting_categories else "Confirm reviewer disposition and expiry" if changed_sources or failed_validations else "Review missing or stale critical categories"
        entry = JurisdictionReviewQueueEntry(
            jurisdiction_profile_id=int(profile.id),
            org_id=getattr(profile, "org_id", None),
            state=getattr(profile, "state", None),
            county=getattr(profile, "county", None),
            city=getattr(profile, "city", None),
            pha_name=getattr(profile, "pha_name", None),
            scope_label=evaluation.get("scope_label") or _scope_label(profile),
            severity=severity,
            severity_rank=severity_rank,
            queue_status=queue_status,
            decision_needed=decision_needed,
            changed_sources=changed_sources,
            failed_validations=failed_validations,
            review_required_categories=review_required_categories,
            conflicting_categories=conflicting_categories,
            source_change_count=len(changed_sources),
            validation_failure_count=len(failed_validations),
            expires_at=expires_at,
            reviewer_action=reviewer_action,
            reviewer_rationale=reviewer_rationale,
            reviewed_by_user_id=reviewed_by_user_id,
            reviewed_at=reviewed_at,
            payload={
                "completeness": completeness,
                "escalation_reasons": evaluation.get("escalation_reasons") or [],
                "lockout": lockout,
            },
        )
        entries.append(entry.as_dict())
        severity_counts[severity] = severity_counts.get(severity, 0) + 1
        grouped.setdefault(entry.scope_label, []).append(entry.as_dict())
    entries.sort(key=lambda row: (-int(row.get("severity_rank") or 0), row.get("scope_label") or "", -(row.get("source_change_count") or 0), -(row.get("validation_failure_count") or 0)))
    return {
        "ok": True,
        "count": len(entries),
        "severity_counts": severity_counts,
        "entries": entries,
        "grouped": [{"scope_label": key, "items": value} for key, value in sorted(grouped.items())],
    }


def persist_review_queue_decision(
    db: Session,
    *,
    profile: JurisdictionProfile,
    reviewer_user_id: int | None,
    reviewer_action: str,
    reviewer_rationale: str | None = None,
    expires_at: datetime | None = None,
) -> dict[str, Any]:
    now = _utcnow()
    payload = {
        "queue_status": "resolved" if reviewer_action in {"approved", "waived", "rejected", "resolved"} else "open",
        "reviewer_action": str(reviewer_action or "").strip() or None,
        "reviewer_rationale": str(reviewer_rationale or "").strip() or None,
        "reviewed_by_user_id": int(reviewer_user_id) if reviewer_user_id is not None else None,
        "reviewed_at": now.isoformat(),
        "expires_at": expires_at.isoformat() if expires_at is not None else None,
    }
    _set_review_queue_meta(profile, payload)
    db.add(profile)
    db.flush()
    audit = AuditEvent(
        org_id=getattr(profile, "org_id", None),
        entity_type=NOTIFICATION_ENTITY_TYPE,
        entity_id=str(getattr(profile, "id")),
        action="jurisdiction_review_queue_decision",
        before_json=_dumps({"previous_review_queue": _review_queue_meta(profile)}),
        after_json=_dumps(payload),
        created_at=now,
    )
    db.add(audit)
    db.commit()
    db.refresh(profile)
    return {"ok": True, "jurisdiction_profile_id": int(profile.id), **payload}
