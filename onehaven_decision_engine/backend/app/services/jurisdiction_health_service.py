from __future__ import annotations

import json
from typing import Any, Optional

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.domain.jurisdiction_categories import normalize_category
from app.policy_models import JurisdictionProfile, PolicyAssertion
from app.services.jurisdiction_completeness_service import profile_completeness_payload
from app.services.jurisdiction_lockout_service import profile_lockout_payload
from app.services.jurisdiction_sla_service import collect_profile_source_sla_summary, profile_next_actions
from app.services.policy_review_service import summarize_policy_overrides


def _loads_json_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        value = str(item or "").strip().lower()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _norm_lower(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    out = str(v).strip().lower()
    return out or None


def _norm_text(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    out = str(v).strip()
    return out or None


def _artifact_backed_refresh_only(sla_summary: dict[str, Any]) -> bool:
    return bool(sla_summary.get("artifact_backed_refresh_only"))


def _blocking_validation_pending_categories(
    *,
    validation_pending_categories: list[str],
    authority_gap_categories: list[str],
    lockout_causing_categories: list[str],
    artifact_backed_refresh_only: bool,
) -> list[str]:
    pending = _dedupe(list(validation_pending_categories or []))
    if not artifact_backed_refresh_only:
        return pending

    authority_or_lockout = set(
        str(x).strip().lower()
        for x in list(authority_gap_categories or []) + list(lockout_causing_categories or [])
        if str(x).strip()
    )
    return [item for item in pending if item in authority_or_lockout]


def _repo_artifact_evidence(sla_summary: dict[str, Any]) -> dict[str, Any]:
    artifact_snapshot = dict(sla_summary.get("repo_artifact_snapshot") or {})
    policy_raw = artifact_snapshot.get("policy_raw") or {}
    pdfs = artifact_snapshot.get("pdfs") or {}
    return {
        "artifact_support_state": artifact_snapshot.get("artifact_support_state") or "unknown",
        "repo_policy_raw_count": int(policy_raw.get("count") or 0),
        "repo_pdf_count": int(pdfs.get("count") or 0),
        "repo_policy_raw_latest_mtime": policy_raw.get("latest_mtime"),
        "repo_pdf_latest_mtime": pdfs.get("latest_mtime"),
        "repo_policy_raw_examples": list(policy_raw.get("examples") or []),
        "repo_pdf_examples": list(pdfs.get("examples") or []),
        "repo_pdf_names": list(pdfs.get("names") or []),
    }


def _reliance_boundary(health_status: str, safe_to_rely_on: bool, artifact_backed_refresh_only: bool) -> dict[str, Any]:
    if safe_to_rely_on:
        return {
            "status": "operationally_reliable",
            "message": "Evidence is strong enough for operational product reliance. Freshness remains a support signal, not the sole source of truth.",
        }
    if artifact_backed_refresh_only and health_status in {"warning", "degraded"}:
        return {
            "status": "degraded_review_required",
            "message": "Stored evidence is present, but freshness, validation, or authority gaps still require review before relying on this jurisdiction.",
        }
    return {
        "status": "not_safe_to_rely_on",
        "message": "The jurisdiction still has blocking evidence, authority, or lockout gaps.",
    }


_RULE_TO_CATEGORY = {
    "lead_hazard_assessment_required": "lead",
    "lead_paint_affidavit_required": "lead",
    "lead_clearance_required": "lead",
    "lead_inspection_required": "lead",
    "permit_required": "permits",
    "local_contact_required": "contacts",
    "local_documents_required": "documents",
    "fee_schedule_reference": "fees",
    "program_overlay_requirement": "program_overlay",
    "source_of_income_protection": "source_of_income",
    "rental_license_required": "rental_license",
    "rental_registration_required": "registration",
    "inspection_required": "inspection",
    "inspection_program_exists": "inspection",
    "fire_safety_inspection_required": "inspection",
    "certificate_required_before_occupancy": "occupancy",
    "certificate_of_occupancy_required": "occupancy",
    "certificate_of_compliance_required": "occupancy",
}


def _assertion_scope_match(row: Any, county: str | None, city: str | None, pha_name: str | None) -> bool:
    return (
        _norm_lower(getattr(row, "county", None)) in {None, _norm_lower(county)}
        and _norm_lower(getattr(row, "city", None)) in {None, _norm_lower(city)}
        and _norm_text(getattr(row, "pha_name", None)) in {None, _norm_text(pha_name)}
    )


def _assertion_category(row: Any) -> str | None:
    for key in ("normalized_category", "rule_category"):
        value = str(getattr(row, key, "") or "").strip().lower()
        if value:
            return value
    rk = str(getattr(row, "rule_key", "") or "").strip()
    if rk in _RULE_TO_CATEGORY:
        return _RULE_TO_CATEGORY[rk]
    rf = str(getattr(row, "rule_family", "") or "").strip().lower()
    return {
        "lead_hazard_assessment_required": "lead",
        "permit_required": "permits",
        "local_contact_required": "contacts",
        "local_documents_required": "documents",
        "fee_schedule_reference": "fees",
        "program_overlay_requirement": "program_overlay",
        "source_of_income_protection": "source_of_income",
        "rental_license": "rental_license",
        "rental_registration": "registration",
        "inspection_program": "inspection",
        "certificate_before_occupancy": "occupancy",
    }.get(rf)


def _covered_categories(
    db: Session,
    *,
    org_id: int | None,
    state: str | None,
    county: str | None,
    city: str | None,
    pha_name: str | None,
) -> set[str]:
    if org_id is None:
        stmt = select(PolicyAssertion).where(PolicyAssertion.org_id.is_(None))
    else:
        stmt = select(PolicyAssertion).where(or_(PolicyAssertion.org_id == org_id, PolicyAssertion.org_id.is_(None)))
    if state:
        stmt = stmt.where(PolicyAssertion.state == str(state).strip().upper())

    rows = list(db.scalars(stmt).all())
    covered: set[str] = set()
    for row in rows:
        if getattr(row, "superseded_by_assertion_id", None) is not None:
            continue
        if not _assertion_scope_match(row, county, city, pha_name):
            continue
        review_status = str(getattr(row, "review_status", "") or "").strip().lower()
        governance_state = str(getattr(row, "governance_state", "") or "").strip().lower()
        validation_state = str(getattr(row, "validation_state", "") or "").strip().lower()
        if (
            review_status not in {"verified", "accepted", "approved", "projected", "needs_manual_review", "extracted"}
            and governance_state not in {"active", "approved"}
            and validation_state not in {"validated", "trusted", "weak_support"}
        ):
            continue
        category = _assertion_category(row)
        if category:
            covered.add(category)
    return covered


def _resolve_profile(
    db: Session,
    *,
    profile_id: int | None,
    org_id: int | None,
    state: str | None,
    county: str | None,
    city: str | None,
    pha_name: str | None,
) -> JurisdictionProfile | None:
    if profile_id is not None:
        return db.get(JurisdictionProfile, int(profile_id))

    stmt = select(JurisdictionProfile)
    if state:
        stmt = stmt.where(JurisdictionProfile.state == str(state).strip().upper())
    if county is not None:
        stmt = stmt.where(JurisdictionProfile.county == (county.strip().lower() or None))
    if city is not None:
        stmt = stmt.where(JurisdictionProfile.city == (city.strip().lower() or None))
    if pha_name is not None:
        stmt = stmt.where(JurisdictionProfile.pha_name == (pha_name.strip() or None))
    if org_id is None:
        stmt = stmt.where(JurisdictionProfile.org_id.is_(None))
    else:
        stmt = stmt.where(or_(JurisdictionProfile.org_id == int(org_id), JurisdictionProfile.org_id.is_(None)))
    return db.scalars(
        stmt.order_by(JurisdictionProfile.org_id.desc().nulls_last(), JurisdictionProfile.id.desc())
    ).first()


def _evidence_safe_to_rely_on(sla_summary: dict[str, Any]) -> bool:
    # Evidence safety should only be about evidence truth basis, not downstream category coverage.
    if list(sla_summary.get("legal_lockout_categories") or []):
        return False
    if list(sla_summary.get("critical_fetch_failure_categories") or []):
        return False
    if int(sla_summary.get("failed_binding_source_count") or 0) > 0:
        return False

    truth_model = dict(sla_summary.get("truth_model") or {})
    if (
        str(truth_model.get("mode") or "").strip().lower() == "evidence_first"
        and str(truth_model.get("freshness_role") or "").strip().lower() == "support_only"
        and str(truth_model.get("crawler_role") or "").strip().lower() == "discovery_and_refresh_only"
    ):
        return True

    return bool(sla_summary.get("safe_to_rely_on", True))


def get_jurisdiction_health(
    db: Session,
    *,
    profile_id: int | None = None,
    org_id: int | None = None,
    state: str | None = None,
    county: str | None = None,
    city: str | None = None,
    pha_name: str | None = None,
) -> dict[str, Any]:
    profile = _resolve_profile(
        db,
        profile_id=profile_id,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )
    if profile is None:
        return {"ok": False, "error": "jurisdiction_profile_not_found"}

    completeness = profile_completeness_payload(db, profile)
    sla_summary = collect_profile_source_sla_summary(db, profile=profile)
    lockout = profile_lockout_payload(profile, completeness)
    next_actions = profile_next_actions(profile)
    refresh_outcome = _loads_json_dict(getattr(profile, "last_refresh_outcome_json", None))
    override_summary = summarize_policy_overrides(
        db,
        org_id=getattr(profile, "org_id", None),
        state=getattr(profile, "state", None),
        county=getattr(profile, "county", None),
        city=getattr(profile, "city", None),
        pha_name=getattr(profile, "pha_name", None),
        jurisdiction_profile_id=int(profile.id),
    )
    if bool(override_summary.get("carrying_critical_override")):
        lockout["override_review_required"] = True

    lockout_causing_categories = _dedupe(
        list(lockout.get("lockout_causing_categories") or [])
        + list(sla_summary.get("legal_lockout_categories") or [])
        + list(sla_summary.get("critical_fetch_failure_categories") or [])
    )
    validation_pending_categories = _dedupe(
        list(lockout.get("validation_pending_categories") or [])
        + list(completeness.get("validation_pending_categories") or [])
        + list(sla_summary.get("review_required_categories") or [])
    )
    authority_gap_categories = _dedupe(
        list(lockout.get("authority_gap_categories") or [])
        + list(completeness.get("authority_unmet_categories") or [])
    )
    informational_gap_categories = _dedupe(
        list(lockout.get("informational_gap_categories") or [])
        + list(completeness.get("inferred_categories") or [])
    )

    artifact_backed = _artifact_backed_refresh_only(sla_summary)
    blocking_validation_pending = _blocking_validation_pending_categories(
        validation_pending_categories=validation_pending_categories,
        authority_gap_categories=authority_gap_categories,
        lockout_causing_categories=lockout_causing_categories,
        artifact_backed_refresh_only=artifact_backed,
    )

    artifact_evidence = _repo_artifact_evidence(sla_summary)
    covered = _covered_categories(
        db,
        org_id=getattr(profile, "org_id", None),
        state=getattr(profile, "state", None),
        county=getattr(profile, "county", None),
        city=getattr(profile, "city", None),
        pha_name=getattr(profile, "pha_name", None),
    )

    raw_unsafe = _dedupe(
        authority_gap_categories
        + lockout_causing_categories
        + blocking_validation_pending
        + list(completeness.get("missing_categories") or [])
    )

    degraded = set(_dedupe(list(sla_summary.get("degraded_categories") or [])))
    freshness_only = set(
        _dedupe(
            list(sla_summary.get("freshness_signal_only_categories") or [])
            + list(sla_summary.get("informational_overdue_categories") or [])
        )
    )

    filtered_unsafe: list[str] = []
    for item in raw_unsafe:
        if item in covered:
            continue
        if item in {"registration", "inspection", "occupancy"}:
            if item in freshness_only or (artifact_backed and item in degraded):
                continue
        filtered_unsafe.append(item)

    evidence_safe_to_rely_on = _evidence_safe_to_rely_on(sla_summary)
    coverage_safe_to_rely_on = len(filtered_unsafe) == 0
    safe_to_rely_on = bool(evidence_safe_to_rely_on and coverage_safe_to_rely_on)

    if safe_to_rely_on:
        health_status = "ok"
        operational_reason = None
    elif evidence_safe_to_rely_on and not coverage_safe_to_rely_on:
        health_status = "blocked"
        operational_reason = "critical_categories_missing_binding_authority"
    elif not evidence_safe_to_rely_on and coverage_safe_to_rely_on:
        health_status = "warning" if artifact_backed else "degraded"
        operational_reason = "freshness_review_required" if artifact_backed else "evidence_not_safe_to_rely_on"
    else:
        health_status = "blocked"
        operational_reason = "critical_categories_missing_binding_authority"

    reliance_boundary = _reliance_boundary(health_status, safe_to_rely_on, artifact_backed)

    return {
        "ok": True,
        "jurisdiction_profile_id": int(profile.id),
        "org_id": getattr(profile, "org_id", None),
        "state": getattr(profile, "state", None),
        "county": getattr(profile, "county", None),
        "city": getattr(profile, "city", None),
        "pha_name": getattr(profile, "pha_name", None),
        "health_status": health_status,
        "operational_state": health_status,
        "operational_reason": operational_reason,
        "evidence_safe_to_rely_on": evidence_safe_to_rely_on,
        "coverage_safe_to_rely_on": coverage_safe_to_rely_on,
        "safe_for_user_reliance": coverage_safe_to_rely_on,
        "safe_for_projection": bool(completeness.get("safe_for_projection")),
        "safe_to_rely_on": safe_to_rely_on,
        "unsafe_reasons": filtered_unsafe,
        "covered_categories": sorted(covered),
        "assertion_category_coverage": {
            "covered_categories": sorted(covered),
            "category_counts": {k: 1 for k in sorted(covered)},
        },
        "completeness": completeness,
        "lockout": lockout,
        "next_actions": next_actions,
        "sla_summary": sla_summary,
        "critical_stale_categories": list(completeness.get("critical_stale_categories") or []),
        "lockout_causing_categories": lockout_causing_categories,
        "critical_fetch_failure_categories": list(sla_summary.get("critical_fetch_failure_categories") or []),
        "legal_lockout_categories": list(sla_summary.get("legal_lockout_categories") or []),
        "rejected_source_count": int(sla_summary.get("rejected_source_count") or 0),
        "guessed_source_count": int(sla_summary.get("guessed_source_count") or 0),
        "failed_binding_source_count": int(sla_summary.get("failed_binding_source_count") or 0),
        "artifact_backed_failed_source_count": int(sla_summary.get("artifact_backed_failed_source_count") or 0),
        "artifact_backed_refresh_only": artifact_backed,
        "informational_gap_categories": informational_gap_categories,
        "validation_pending_categories": validation_pending_categories,
        "blocking_validation_pending_categories": blocking_validation_pending,
        "authority_gap_categories": authority_gap_categories,
        "legal_stale_categories": list(completeness.get("legal_stale_categories") or sla_summary.get("legal_overdue_categories") or []),
        "informational_stale_categories": list(completeness.get("informational_stale_categories") or sla_summary.get("informational_overdue_categories") or []),
        "stale_authoritative_categories": list(completeness.get("stale_authoritative_categories") or sla_summary.get("stale_authoritative_categories") or []),
        "override_summary": override_summary,
        "artifact_evidence": artifact_evidence,
        "repo_artifact_support_state": artifact_evidence.get("artifact_support_state"),
        "repo_policy_raw_count": int(artifact_evidence.get("repo_policy_raw_count") or 0),
        "repo_pdf_count": int(artifact_evidence.get("repo_pdf_count") or 0),
        "repo_pdf_names": list(artifact_evidence.get("repo_pdf_names") or []),
        "review_required": (
            bool(override_summary.get("review_required"))
            or bool(override_summary.get("carrying_critical_override"))
            or not bool(completeness.get("safe_for_user_reliance"))
            or bool(sla_summary.get("review_required_categories"))
            or int(sla_summary.get("rejected_source_count") or 0) > 0
        ),
        "refresh_state": getattr(profile, "refresh_state", None),
        "last_validation_at": sla_summary.get("latest_validated_at") if isinstance(sla_summary, dict) else None,
        "next_due_step": (next_actions.get("next_step") if isinstance(next_actions, dict) else None),
        "next_due_at": (
            (next_actions.get("next_search_retry_due_at") if isinstance(next_actions, dict) else None)
            or (sla_summary.get("next_validation_due_at") if isinstance(sla_summary, dict) else None)
            or (sla_summary.get("next_refresh_due_at") if isinstance(sla_summary, dict) else None)
            or sla_summary.get("next_due_at")
        ),
        "refresh_status_reason": getattr(profile, "refresh_status_reason", None),
        "last_refresh_success_at": getattr(profile, "last_refresh_success_at", None).isoformat() if getattr(profile, "last_refresh_success_at", None) else None,
        "last_refresh_completed_at": getattr(profile, "last_refresh_completed_at", None).isoformat() if getattr(profile, "last_refresh_completed_at", None) else None,
        "refresh_retry_count": int(getattr(profile, "refresh_retry_count", 0) or 0),
        "refresh_outcome": refresh_outcome,
        "reliance_boundary": reliance_boundary,
        "truth_model": dict(
            sla_summary.get("truth_model")
            or {
                "mode": "evidence_first",
                "freshness_role": "support_only",
                "crawler_role": "discovery_and_refresh_only",
            }
        ),
        "evidence_family": dict(sla_summary.get("evidence_family") or {}),
        "blocking_categories": list(sla_summary.get("blocking_categories") or []),
        "degraded_categories": list(sla_summary.get("degraded_categories") or []),
        "freshness_signal_only_categories": list(sla_summary.get("freshness_signal_only_categories") or []),
        "operational_reliance_message": reliance_boundary.get("message"),
    }


def _due_now_reasons(health: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if list(health.get("lockout_causing_categories") or []):
        reasons.append("blocking legal categories need manual action")
    if list(health.get("stale_authoritative_categories") or []):
        reasons.append("authoritative categories are stale")
    if list(health.get("blocking_validation_pending_categories") or []):
        reasons.append("blocking validation is still pending")
    conflicting = list(((health.get("completeness") or {}).get("conflicting_categories") or []))
    if conflicting:
        reasons.append("conflicts need reviewer resolution")
    if list(health.get("authority_gap_categories") or []):
        reasons.append("binding authority gaps remain")
    if not reasons and bool(health.get("artifact_backed_refresh_only")) and int(health.get("artifact_backed_failed_source_count") or 0) > 0:
        reasons.append("artifact-backed evidence is usable but source freshness needs review")
    if not reasons and bool(health.get("review_required")):
        reasons.append("manual review is still required")
    if not reasons and bool((health.get("completeness") or {}).get("missing_categories")):
        reasons.append("jurisdiction coverage is incomplete")
    return _dedupe(reasons)


def _dashboard_tags(health: dict[str, Any]) -> list[str]:
    tags: list[str] = []
    health_state = str(health.get("health_status") or health.get("operational_state") or "").strip().lower()
    if health_state in {"blocked", "degraded", "warning", "ok"}:
        tags.append(health_state)
    if bool(health.get("review_required")):
        tags.append("review-required")
    if bool(health.get("stale_authoritative_categories")) or bool(health.get("legal_stale_categories")) or bool(health.get("informational_stale_categories")):
        tags.append("stale")
    if bool(((health.get("completeness") or {}).get("missing_categories") or [])) or bool(health.get("authority_gap_categories")) or bool(health.get("lockout_causing_categories")):
        tags.append("missing-proof")
    if bool(health.get("artifact_backed_refresh_only")):
        tags.append("artifact-backed")
    return _dedupe(tags)


def get_manual_stale_review_dashboard(
    db: Session,
    *,
    org_id: int | None = None,
    state: str | None = None,
    county: str | None = None,
    city: str | None = None,
    pha_name: str | None = None,
    status_filter: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    stmt = select(JurisdictionProfile)
    if state:
        stmt = stmt.where(JurisdictionProfile.state == str(state).strip().upper())
    if county is not None:
        stmt = stmt.where(JurisdictionProfile.county == (county.strip().lower() or None))
    if city is not None:
        stmt = stmt.where(JurisdictionProfile.city == (city.strip().lower() or None))
    if pha_name is not None:
        stmt = stmt.where(JurisdictionProfile.pha_name == (pha_name.strip() or None))
    if org_id is None:
        stmt = stmt.where(JurisdictionProfile.org_id.is_(None))
    else:
        stmt = stmt.where(or_(JurisdictionProfile.org_id == int(org_id), JurisdictionProfile.org_id.is_(None)))

    rows = list(db.scalars(stmt.order_by(JurisdictionProfile.id.desc())).all())
    items: list[dict[str, Any]] = []
    counts = {
        "blocked": 0,
        "degraded": 0,
        "warning": 0,
        "ok": 0,
        "review_required": 0,
        "stale": 0,
        "missing_proof": 0,
    }

    wanted = str(status_filter or "all").strip().lower()
    for profile in rows:
        health = get_jurisdiction_health(db, profile_id=int(profile.id), org_id=org_id)
        if not health.get("ok"):
            continue
        tags = _dashboard_tags(health)
        state_key = str(health.get("health_status") or "ok").strip().lower()
        if state_key in counts:
            counts[state_key] += 1
        if "review-required" in tags:
            counts["review_required"] += 1
        if "stale" in tags:
            counts["stale"] += 1
        if "missing-proof" in tags:
            counts["missing_proof"] += 1

        if wanted not in {"", "all"} and wanted not in tags:
            continue

        completeness = health.get("completeness") or {}
        items.append(
            {
                "jurisdiction_profile_id": int(profile.id),
                "state": getattr(profile, "state", None),
                "county": getattr(profile, "county", None),
                "city": getattr(profile, "city", None),
                "pha_name": getattr(profile, "pha_name", None),
                "health_status": health.get("health_status"),
                "operational_state": health.get("operational_state"),
                "operational_reason": health.get("operational_reason"),
                "review_required": bool(health.get("review_required")),
                "safe_to_rely_on": bool(health.get("safe_to_rely_on")),
                "tags": tags,
                "why_due_now": _due_now_reasons(health),
                "what_to_do_next": health.get("next_due_step") or ((health.get("next_actions") or {}).get("next_step")) or "review_jurisdiction_state",
                "next_due_at": health.get("next_due_at"),
                "last_refresh_success_at": health.get("last_refresh_success_at"),
                "last_validation_at": health.get("last_validation_at"),
                "last_refresh_completed_at": health.get("last_refresh_completed_at"),
                "refresh_state": health.get("refresh_state"),
                "refresh_status_reason": health.get("refresh_status_reason"),
                "lockout_causing_categories": list(health.get("lockout_causing_categories") or []),
                "stale_authoritative_categories": list(health.get("stale_authoritative_categories") or []),
                "validation_pending_categories": list(health.get("validation_pending_categories") or []),
                "blocking_validation_pending_categories": list(health.get("blocking_validation_pending_categories") or []),
                "authority_gap_categories": list(health.get("authority_gap_categories") or []),
                "artifact_gap_categories": list((health.get("lockout") or {}).get("artifact_gap_categories") or []),
                "informational_gap_categories": list(health.get("informational_gap_categories") or []),
                "conflicting_categories": list(completeness.get("conflicting_categories") or []),
                "missing_categories": list(completeness.get("missing_categories") or []),
                "completeness_status": completeness.get("completeness_status"),
                "completeness_score": completeness.get("completeness_score"),
            }
        )

    severity_rank = {"blocked": 0, "degraded": 1, "warning": 2, "ok": 3}
    items.sort(
        key=lambda row: (
            severity_rank.get(str(row.get("health_status") or "ok").lower(), 9),
            0 if row.get("review_required") else 1,
            0 if row.get("next_due_at") else 1,
            str(row.get("next_due_at") or ""),
            -(int(row.get("jurisdiction_profile_id") or 0)),
        )
    )

    return {
        "ok": True,
        "filter": wanted or "all",
        "count": len(items[:limit]),
        "summary": counts,
        "rows": items[:limit],
    }
