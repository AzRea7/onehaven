# backend/app/services/policy_pipeline_service.py
from __future__ import annotations

import json

from typing import Any, Optional

from sqlalchemy import or_, select
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import ObjectDeletedError

from app.policy_models import JurisdictionProfile, PolicyAssertion, PolicySource
from app.services.jurisdiction_completeness_service import (
    profile_completeness_payload,
    recompute_profile_and_coverage,
)
from app.services.jurisdiction_notification_service import build_review_queue_payload, build_gap_escalation_notifications
from app.services.policy_cleanup_service import (
    ARCHIVE_MARKER,
    archive_stale_market_sources,
)
from app.services.policy_coverage_service import (
    compute_coverage_status,
    upsert_coverage_status,
)
from app.services.policy_extractor_service import extract_assertions_for_source, mark_assertions_stale_for_source
from app.services.policy_projection_service import (
    build_property_compliance_brief,
    project_verified_assertions_to_profile,
)
from app.services.policy_review_service import (
    apply_governance_lifecycle,
    auto_verify_market_assertions,
    cleanup_market_stale_assertions,
    diff_active_rules_for_source,
    normalize_market_assertions,
    supersede_replaced_assertions,
)
from app.services.policy_source_service import (
    collect_catalog_for_market,
    discover_policy_sources_for_market,
    inventory_summary_for_market,
    list_sources_for_market,
    refresh_policy_source_and_detect_changes,
)
from app.services.policy_discovery_service import expected_inventory_hints
from app.services.policy_change_detection_service import summarize_refresh_runs
from app.services.policy_validation_service import validate_market_assertions
from app.services.jurisdiction_sla_service import build_refresh_requirements, collect_profile_source_sla_summary
from app.services.policy_catalog_admin_service import merged_catalog_for_market

def _norm_state(s: Optional[str]) -> str:
    return (s or "MI").strip().upper()


def _norm_lower(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    v = s.strip().lower()
    return v or None


def _norm_text(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    v = s.strip()
    return v or None


def _is_archived_source(src: PolicySource) -> bool:
    return ARCHIVE_MARKER in (src.notes or "").lower()

def _official_catalog_urls_for_market(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
    focus: str,
) -> list[str]:
    items = merged_catalog_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
    )
    return [str(item.url or "").strip().lower() for item in items if str(item.url or "").strip()]

def _find_matching_profile(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> JurisdictionProfile | None:
    stmt = select(JurisdictionProfile).where(JurisdictionProfile.state == state)

    if org_id is None:
        stmt = stmt.where(JurisdictionProfile.org_id.is_(None))
    else:
        stmt = stmt.where(or_(JurisdictionProfile.org_id == org_id, JurisdictionProfile.org_id.is_(None)))

    rows = list(db.scalars(stmt).all())
    for row in rows:
        row_county = _norm_lower(getattr(row, "county", None))
        row_city = _norm_lower(getattr(row, "city", None))
        row_pha = _norm_text(getattr(row, "pha_name", None))

        if row_county not in {None, county}:
            continue
        if row_city not in {None, city}:
            continue
        if pha_name is not None and row_pha not in {None, pha_name}:
            continue
        if row_city == city and row_county == county:
            return row

    for row in rows:
        row_county = _norm_lower(getattr(row, "county", None))
        row_city = _norm_lower(getattr(row, "city", None))
        if row_city is None and row_county == county:
            return row

    for row in rows:
        if _norm_lower(getattr(row, "city", None)) is None and _norm_lower(getattr(row, "county", None)) is None:
            return row
    return None


def _market_sources_from_catalog(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
    focus: str,
) -> list[PolicySource]:
    collect_catalog_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
    )

    allowed_urls = set(
        _official_catalog_urls_for_market(
            db,
            org_id=org_id,
            state=state,
            county=county,
            city=city,
            pha_name=pha_name,
            focus=focus,
        )
    )
    rows = list_sources_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )

    out: list[PolicySource] = []
    seen: set[int] = set()

    for row in rows:
        if _is_archived_source(row):
            continue
        if int(row.id) in seen:
            continue
        row_url = str(getattr(row, "url", "") or "").strip().lower()
        if row_url not in allowed_urls:
            continue
        seen.add(int(row.id))
        out.append(row)

    return out


def _extract_raw_candidates(source_result: Any) -> list[dict[str, Any]]:
    if isinstance(source_result, dict):
        candidates = source_result.get("assertions") or source_result.get("items") or source_result.get("candidates") or []
        return [dict(item) for item in candidates if isinstance(item, dict)]

    out: list[dict[str, Any]] = []
    if isinstance(source_result, list):
        for item in source_result:
            if not isinstance(item, PolicyAssertion):
                continue
            out.append(
                {
                    "rule_key": getattr(item, "rule_key", None),
                    "rule_family": getattr(item, "rule_family", None),
                    "rule_category": getattr(item, "rule_category", None) or getattr(item, "normalized_category", None),
                    "source_level": getattr(item, "source_level", None),
                    "property_type": getattr(item, "property_type", None),
                    "required": getattr(item, "required", None),
                    "blocking": getattr(item, "blocking", None),
                    "confidence": getattr(item, "confidence", None),
                    "governance_state": getattr(item, "governance_state", None),
                    "rule_status": getattr(item, "rule_status", None),
                    "normalized_version": getattr(item, "normalized_version", None),
                    "version_group": getattr(item, "version_group", None),
                    "value_json": getattr(item, "value_json", None),
                    "source_citation": getattr(item, "source_citation", None),
                    "raw_excerpt": getattr(item, "raw_excerpt", None),
                    "source_id": getattr(item, "source_id", None),
                    "source_version_id": getattr(item, "source_version_id", None),
                    "state": getattr(item, "state", None),
                    "county": getattr(item, "county", None),
                    "city": getattr(item, "city", None),
                    "pha_name": getattr(item, "pha_name", None),
                    "program_type": getattr(item, "program_type", None),
                    "source_freshness_status": getattr(item, "source_freshness_status", None),
                }
            )
    return out


def _governance_summary(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> dict[str, Any]:
    stmt = select(PolicyAssertion).where(PolicyAssertion.state == state)
    stmt = stmt.where(PolicyAssertion.county == county if county is not None else PolicyAssertion.county.is_(None))
    stmt = stmt.where(PolicyAssertion.city == city if city is not None else PolicyAssertion.city.is_(None))
    rows = list(db.scalars(stmt).all())

    counts = {
        "draft": 0,
        "approved": 0,
        "active": 0,
        "replaced": 0,
        "candidate": 0,
        "verified": 0,
        "stale": 0,
        "conflicting": 0,
    }
    current_ids: list[int] = []
    for row in rows:
        gov = (row.governance_state or "").lower()
        rule_status = (row.rule_status or "").lower()
        review_status = (row.review_status or "").lower()
        if gov in counts:
            counts[gov] += 1
        if rule_status in counts:
            counts[rule_status] += 1
        if review_status in counts:
            counts[review_status] += 1
        if bool(getattr(row, "is_current", False)):
            current_ids.append(int(row.id))

    return {
        "counts": counts,
        "current_assertion_ids": sorted(set(current_ids)),
        "total": len(rows),
    }


def _discovery_missing_categories(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> list[str]:
    coverage_before = compute_coverage_status(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )
    missing = coverage_before.get("missing_categories") or []
    return [str(item).strip().lower() for item in missing if str(item).strip()]



def _recompute_profile_for_market(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> dict[str, Any]:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    profile = _find_matching_profile(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )
    inventory_hints = expected_inventory_hints(
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        include_section8=True,
    )
    inventory_summary = inventory_summary_for_market(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        program_type="section8" if "section8" in set(inventory_hints.get("expected_categories") or []) else None,
    )

    if profile is None:
        return {
            "ok": True,
            "expected_inventory": inventory_hints,
            "inventory_summary": inventory_summary,
            "recomputed": False,
            "jurisdiction_profile_id": None,
        }

    refreshed_profile, coverage = recompute_profile_and_coverage(
        db,
        profile,
        commit=True,
    )
    completeness_payload = profile_completeness_payload(db, refreshed_profile)
    sla_summary = collect_profile_source_sla_summary(db, profile=refreshed_profile)
    refresh_requirements = build_refresh_requirements(
        refreshed_profile,
        next_step="refresh" if list(sla_summary.get("legal_overdue_categories") or completeness_payload.get("critical_stale_categories") or []) else "monitor",
        missing_categories=list(completeness_payload.get("missing_categories") or []),
        stale_categories=list(completeness_payload.get("stale_categories") or []),
        overdue_categories=list(sla_summary.get("overdue_categories") or []),
        critical_overdue_categories=list(sla_summary.get("critical_overdue_categories") or []),
        legal_overdue_categories=list(sla_summary.get("legal_overdue_categories") or []),
        informational_overdue_categories=list(sla_summary.get("informational_overdue_categories") or []),
        stale_authoritative_categories=list(sla_summary.get("stale_authoritative_categories") or []),
        inventory_summary=inventory_summary,
    )
    if hasattr(refreshed_profile, "refresh_requirements_json"):
        import json as _json
        refreshed_profile.refresh_requirements_json = _json.dumps(refresh_requirements, sort_keys=True, default=str)
        db.add(refreshed_profile)
        db.commit()
        db.refresh(refreshed_profile)

    return {
        "ok": True,
        "expected_inventory": inventory_hints,
        "inventory_summary": inventory_summary,
        "recomputed": True,
        "jurisdiction_profile_id": int(refreshed_profile.id),
        "profile": completeness_payload,
        "sla_summary": sla_summary,
        "refresh_requirements": refresh_requirements,
        "coverage": {
            "id": int(coverage.id),
            "coverage_status": getattr(coverage, "coverage_status", None),
            "production_readiness": getattr(coverage, "production_readiness", None),
            "completeness_status": getattr(coverage, "completeness_status", None),
            "is_stale": bool(getattr(coverage, "is_stale", False)),
        },
    }


def _run_source_refresh_batch(

    db: Session,
    *,
    sources: list[PolicySource],
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
    reviewer_user_id: int | None,
) -> dict[str, Any]:
    source_runs: list[dict[str, Any]] = []
    total_changed_rules = 0
    total_new_rules = 0
    total_missing_rules = 0
    changed_source_ids: list[int] = []
    failed_source_ids: list[int] = []

    for source in sources:
        fetch_result = refresh_policy_source_and_detect_changes(db, source=source)
        content_changed = bool(fetch_result.get("change_detected") or fetch_result.get("changed"))
        extract_result: dict[str, Any]
        diff_result: dict[str, Any]
        normalize_result: dict[str, Any] | None = None
        validation_result: dict[str, Any] | None = None

        if not fetch_result.get("ok"):
            failed_source_ids.append(int(source.id))
            stale_mark_result = mark_assertions_stale_for_source(
                db,
                source_id=int(source.id),
                reason="source_fetch_failed",
            )
            extract_result = {
                "ok": False,
                "created_count": 0,
                "assertion_ids": [],
                "stale_mark_result": stale_mark_result,
                "reason": "fetch_failed",
            }
            diff_result = {
                "ok": False,
                "changed_count": 0,
                "new_count": 0,
                "missing_count": 0,
                "reason": "fetch_failed",
            }
        else:
            raw_candidates: list[dict[str, Any]] = []
            if content_changed:
                changed_source_ids.append(int(source.id))
                mark_assertions_stale_for_source(
                    db,
                    source_id=int(source.id),
                    reason="source_changed",
                )
                extracted = extract_assertions_for_source(
                    db,
                    source=source,
                    org_id=org_id,
                    org_scope=(org_id is not None),
                )
                raw_candidates = _extract_raw_candidates(extracted)
                extract_result = {
                    "ok": True,
                    "created_count": len(extracted),
                    "assertion_ids": [int(a.id) for a in extracted if getattr(a, "id", None) is not None],
                }
                diff_result = diff_active_rules_for_source(
                    db,
                    org_id=org_id,
                    source_id=int(source.id),
                    state=state,
                    county=county,
                    city=city,
                    pha_name=pha_name,
                    raw_candidates=raw_candidates,
                )
                normalize_result = normalize_market_assertions(
                    db,
                    org_id=org_id,
                    state=state,
                    county=county,
                    city=city,
                    pha_name=pha_name,
                    reviewer_user_id=reviewer_user_id,
                    source_id=int(source.id),
                    raw_candidates=raw_candidates,
                )
                validation_result = validate_market_assertions(
                    db,
                    org_id=org_id,
                    state=state,
                    county=county,
                    city=city,
                    pha_name=pha_name,
                    source_id=int(source.id),
                )
            else:
                extract_result = {
                    "ok": True,
                    "created_count": 0,
                    "assertion_ids": [],
                    "reason": "no_content_change",
                }
                diff_result = {
                    "ok": True,
                    "changed_count": 0,
                    "new_count": 0,
                    "missing_count": 0,
                    "reason": "no_content_change",
                }
                validation_result = {
                    "validated_count": 0,
                    "weak_support_count": 0,
                    "ambiguous_count": 0,
                    "conflicting_count": 0,
                    "unsupported_count": 0,
                    "updated_ids": [],
                }

        total_changed_rules += int(diff_result.get("changed_count") or 0)
        total_new_rules += int(diff_result.get("new_count") or 0)
        total_missing_rules += int(diff_result.get("missing_count") or 0)

        source_runs.append(
            {
                "source_id": int(source.id),
                "refresh": fetch_result,
                "changed": content_changed,
                "comparison_state": fetch_result.get("comparison_state"),
                "change_kind": fetch_result.get("change_kind") or (fetch_result.get("change_summary") or {}).get("change_kind"),
                "revalidation_required": bool(fetch_result.get("revalidation_required")),
                "source_version_id": fetch_result.get("source_version_id"),
                "previous_version_id": fetch_result.get("previous_version_id"),
                "extract_result": extract_result,
                "diff": diff_result,
                "normalized": normalize_result,
                "validation": validation_result,
                "requires_revalidation": bool(fetch_result.get("revalidation_required")) or bool((validation_result or {}).get("blocking_issue_count")),
            }
        )

    return {
        "source_runs": source_runs,
        "changed_source_ids": sorted(set(changed_source_ids)),
        "failed_source_ids": sorted(set(failed_source_ids)),
        "summary": {
            "changed_source_count": len(set(changed_source_ids)),
            "failed_source_count": len(set(failed_source_ids)),
            "changed_rule_count": total_changed_rules,
            "new_rule_count": total_new_rules,
            "missing_rule_count": total_missing_rules,
        },
    }



def run_market_policy_pipeline(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = "se_mi_extended",
    reviewer_user_id: int | None = None,
    auto_activate: bool = True,
) -> dict[str, Any]:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    inventory_hints = expected_inventory_hints(
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        include_section8=True,
    )
    missing_categories = _discovery_missing_categories(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )

    # Discovery is now selection only; it cannot invent URLs.
    discovery_result = discover_policy_sources_for_market(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        missing_categories=missing_categories,
        focus=focus,
        probe=False,
    )

    sources = _market_sources_from_catalog(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        focus=focus,
    )

    refresh_batch = _run_source_refresh_batch(
        db,
        sources=sources,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        reviewer_user_id=reviewer_user_id,
    )
    inventory_summary = inventory_summary_for_market(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        program_type="section8" if "section8" in set(inventory_hints.get("expected_categories") or []) else None,
    )
    refresh_state_summary = summarize_refresh_runs(refresh_batch["source_runs"])

    lifecycle_result = apply_governance_lifecycle(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        reviewer_user_id=reviewer_user_id,
        auto_activate=auto_activate,
    )

    recompute = _recompute_profile_for_market(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )

    review_queue = build_review_queue_payload(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )

    gap_escalations = []
    try:
        profile_id = ((recompute or {}).get("jurisdiction_profile_id") if isinstance(recompute, dict) else None)
        profile = db.get(JurisdictionProfile, int(profile_id)) if profile_id else None
        if profile is not None:
            gap_escalations = [note.as_dict() for note in build_gap_escalation_notifications(db, profile=profile)]
    except Exception:
        gap_escalations = []

    return {
        "ok": True,
        "missing_categories": missing_categories,
        "expected_inventory": inventory_hints,
        "discovery_result": {
            **discovery_result,
            "selection_mode": "curated_only",
            "guessed_source_count": 0,
        },
        "sources_processed": len(refresh_batch["source_runs"]),
        "total_changed_rules": int(refresh_batch["summary"]["changed_rule_count"]),
        "changed_source_count": int(refresh_batch["summary"]["changed_source_count"]),
        "failed_source_count": int(refresh_batch["summary"]["failed_source_count"]),
        "source_runs": refresh_batch["source_runs"],
        "refresh_summary": {
            **refresh_batch["summary"],
            "manual_review_assertion_ids": [],
            "revalidation_required_source_ids": [
                row["source_id"] for row in refresh_batch["source_runs"]
                if bool((row.get("refresh") or {}).get("revalidation_required"))
            ],
            "changed_or_failed_source_ids": [
                row["source_id"] for row in refresh_batch["source_runs"]
                if bool(row.get("changed")) or not bool((row.get("refresh") or {}).get("ok", False))
            ],
            "validation_blocked_source_ids": [
                row["source_id"] for row in refresh_batch["source_runs"]
                if isinstance(row.get("validation"), dict)
                and int((row.get("validation") or {}).get("blocking_issue_count", 0) or 0) > 0
            ],
        },
        "refresh_state": refresh_state_summary,
        "lifecycle_result": lifecycle_result,
        "recompute": recompute,
        "review_queue": review_queue,
        "gap_escalations": gap_escalations,
        "inventory_summary": inventory_summary,
    }


def refresh_market_policy_pipeline(

    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = "se_mi_extended",
    reviewer_user_id: int | None = None,
    auto_activate: bool = True,
) -> dict[str, Any]:
    archive_result = archive_stale_market_sources(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
    )
    pipeline_result = run_market_policy_pipeline(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
        reviewer_user_id=reviewer_user_id,
        auto_activate=auto_activate,
    )
    return {
        "ok": True,
        "archive_result": archive_result,
        "pipeline_result": pipeline_result,
    }


def refresh_single_policy_source(
    db: Session,
    *,
    org_id: Optional[int],
    source_id: int,
    reviewer_user_id: int | None = None,
    auto_activate: bool = True,
) -> dict[str, Any]:
    source = db.get(PolicySource, int(source_id))
    if source is None:
        return {"ok": False, "error": "source_not_found", "source_id": int(source_id)}

    st = _norm_state(getattr(source, "state", None))
    cnty = _norm_lower(getattr(source, "county", None))
    cty = _norm_lower(getattr(source, "city", None))
    pha = _norm_text(getattr(source, "pha_name", None))

    fetch_result = refresh_policy_source_and_detect_changes(db, source=source, force=True)
    content_changed = bool(fetch_result.get("change_detected") or fetch_result.get("changed"))

    if content_changed:
        stale_mark_result = mark_assertions_stale_for_source(
            db,
            source_id=int(source.id),
            reason="source_content_changed",
        )
        extract_result_rows = extract_assertions_for_source(
            db,
            source=source,
            org_id=org_id,
            org_scope=(org_id is not None),
        )
        raw_candidates = _extract_raw_candidates(extract_result_rows)
        diff_result = diff_active_rules_for_source(
            db,
            org_id=org_id,
            source_id=int(source.id),
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
            raw_candidates=raw_candidates,
        )
        normalize_result = normalize_market_assertions(
            db,
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
            reviewer_user_id=reviewer_user_id,
            source_id=int(source.id),
            raw_candidates=raw_candidates,
        )
    else:
        stale_mark_result = {
            "ok": True,
            "source_id": int(source.id),
            "stale_count": 0,
            "stale_ids": [],
            "reason": "no_source_change",
        }
        extract_result_rows = []
        raw_candidates = []
        diff_result = {
            "ok": True,
            "changed_count": 0,
            "new_count": 0,
            "missing_count": 0,
            "reason": "no_content_change",
        }
        normalize_result = None

    auto_verify_result = auto_verify_market_assertions(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        reviewer_user_id=reviewer_user_id,
    )
    lifecycle_result = apply_governance_lifecycle(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        reviewer_user_id=reviewer_user_id,
        auto_activate=auto_activate,
    )
    supersede_result = supersede_replaced_assertions(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        reviewer_user_id=reviewer_user_id,
    )
    cleanup_result = cleanup_market_stale_assertions(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        reviewer_user_id=reviewer_user_id,
    )

    coverage_payload = compute_coverage_status(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )
    upsert_coverage_status(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )

    return {
        "ok": True,
        "source_id": int(source.id),
        "source_name": getattr(source, "source_name", None) or getattr(source, "title", None),
        "fetch_result": fetch_result,
        "stale_mark_result": stale_mark_result,
        "extract_result": {
            "created_count": len(extract_result_rows),
            "assertion_ids": [int(a.id) for a in extract_result_rows if getattr(a, "id", None) is not None],
            "assertions": raw_candidates,
        },
        "diff_result": diff_result,
        "normalize_result": normalize_result,
        "auto_verify_result": auto_verify_result,
        "lifecycle_result": lifecycle_result,
        "supersede_result": supersede_result,
        "cleanup_result": cleanup_result,
        "coverage": coverage_payload,
        "governance": _governance_summary(
            db,
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
        ),
    }


def mark_source_assertions_stale(
    db: Session,
    *,
    source_id: int,
    reason: str = "source_refreshed",
) -> dict[str, Any]:
    return mark_assertions_stale_for_source(db, source_id=source_id, reason=reason)


from app.services.jurisdiction_health_service import get_jurisdiction_health as _chunk3_pipeline_get_jurisdiction_health
from app.services.jurisdiction_refresh_service import finalize_jurisdiction_profile_lifecycle as _chunk3_finalize_jurisdiction_profile_lifecycle

_chunk3_original_run_market_policy_pipeline = run_market_policy_pipeline
_chunk3_original_refresh_market_policy_pipeline = refresh_market_policy_pipeline


def run_market_policy_pipeline(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = "se_mi_extended",
    reviewer_user_id: int | None = None,
    auto_activate: bool = True,
) -> dict[str, Any]:
    result = _chunk3_original_run_market_policy_pipeline(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
        reviewer_user_id=reviewer_user_id,
        auto_activate=auto_activate,
    )
    if not result.get("ok"):
        return result

    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)
    profile = _find_matching_profile(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )
    if profile is None:
        return result

    refresh_results = []
    for row in list(result.get("source_runs") or []):
        if isinstance(row, dict) and isinstance(row.get("refresh"), dict):
            refresh_results.append(dict(row["refresh"]))

    finalized = _chunk3_finalize_jurisdiction_profile_lifecycle(
        db,
        profile=profile,
        refresh_results=refresh_results,
        discovery_result=result.get("discovery_result") if isinstance(result.get("discovery_result"), dict) else None,
        governance_result=result.get("lifecycle_result") if isinstance(result.get("lifecycle_result"), dict) else None,
    )
    result["recompute"] = {
        **dict(result.get("recompute") or {}),
        "profile": finalized["completeness"],
        "coverage": finalized["coverage"],
    }
    result["health"] = finalized["health"]
    result["lockout"] = finalized["lockout"]
    result["sla_summary"] = finalized["sla_summary"]
    result["requirements"] = finalized["requirements"]
    return result


def run_market_pipeline(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = "se_mi_extended",
    reviewer_user_id: int | None = None,
    auto_activate: bool = True,
) -> dict[str, Any]:
    """
    Backward-compatible alias expected by routers/policy.py.
    """
    return run_market_policy_pipeline(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
        reviewer_user_id=reviewer_user_id,
        auto_activate=auto_activate,
    )


def cleanup_market(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = "se_mi_extended",
    archive_extracted_duplicates: bool = True,
) -> dict[str, Any]:
    """
    Backward-compatible cleanup entrypoint expected by routers/policy.py.
    """
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    archive_result = archive_stale_market_sources(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        focus=focus,
    )

    stale_cleanup = cleanup_market_stale_assertions(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )

    governance = _governance_summary(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )

    return {
        "ok": True,
        "state": st,
        "county": cnty,
        "city": cty,
        "pha_name": pha,
        "focus": focus,
        "archive_extracted_duplicates": bool(archive_extracted_duplicates),
        "archive_result": archive_result,
        "stale_assertion_cleanup": stale_cleanup,
        "governance_summary": governance,
    }


def repair_market(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = "se_mi_extended",
    archive_extracted_duplicates: bool = True,
    reviewer_user_id: int | None = None,
    auto_activate: bool = True,
) -> dict[str, Any]:
    """
    Backward-compatible repair entrypoint expected by routers/policy.py.
    Performs cleanup, normalization/governance repair, and recomputation.
    """
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    cleanup_result = cleanup_market(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        focus=focus,
        archive_extracted_duplicates=archive_extracted_duplicates,
    )

    pipeline_result = run_market_policy_pipeline(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        focus=focus,
        reviewer_user_id=reviewer_user_id,
        auto_activate=auto_activate,
    )

    return {
        "ok": True,
        "state": st,
        "county": cnty,
        "city": cty,
        "pha_name": pha,
        "focus": focus,
        "cleanup_result": cleanup_result,
        "pipeline_result": pipeline_result,
        "recompute": (pipeline_result or {}).get("recompute"),
        "review_queue": (pipeline_result or {}).get("review_queue"),
        "refresh_state": (pipeline_result or {}).get("refresh_state"),
        "refresh_summary": (pipeline_result or {}).get("refresh_summary"),
    }

def refresh_market_policy_pipeline(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = "se_mi_extended",
    reviewer_user_id: int | None = None,
    auto_activate: bool = True,
) -> dict[str, Any]:
    result = _chunk3_original_refresh_market_policy_pipeline(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
        reviewer_user_id=reviewer_user_id,
        auto_activate=auto_activate,
    )
    pipeline_result = dict(result.get("pipeline_result") or {})
    if pipeline_result.get("ok"):
        st = _norm_state(state)
        cnty = _norm_lower(county)
        cty = _norm_lower(city)
        pha = _norm_text(pha_name)
        profile = _find_matching_profile(
            db,
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
        )
        if profile is not None:
            pipeline_result["health"] = _chunk3_pipeline_get_jurisdiction_health(db, profile_id=int(profile.id))
    result["pipeline_result"] = pipeline_result
    return result


# --- Story 4.2 additive governed-truth overlays ---
from app.services.policy_cleanup_service import cleanup_non_projectable_assertions_for_market
from app.services.jurisdiction_rules_service import governed_assertions_for_scope

_chunk42_original_run_market_policy_pipeline = run_market_policy_pipeline

def run_market_policy_pipeline(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = "se_mi_extended",
    reviewer_user_id: int | None = None,
    auto_activate: bool = True,
) -> dict[str, Any]:
    result = _chunk42_original_run_market_policy_pipeline(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
        reviewer_user_id=reviewer_user_id,
        auto_activate=auto_activate,
    )
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    cleanup_result = cleanup_market_stale_assertions(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        reviewer_user_id=reviewer_user_id,
    )
    non_projectable_cleanup = cleanup_non_projectable_assertions_for_market(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )
    governed_truth = governed_assertions_for_scope(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )

    lifecycle_result = dict(result.get("lifecycle_result") or {})
    lifecycle_result["cleanup_result"] = cleanup_result
    lifecycle_result["non_projectable_cleanup"] = non_projectable_cleanup
    lifecycle_result["governed_truth"] = governed_truth
    lifecycle_result["manual_review_count"] = int(governed_truth.get("manual_review_count", 0) or 0)
    lifecycle_result["manual_review_ids"] = list(governed_truth.get("manual_review_ids") or [])
    result["lifecycle_result"] = lifecycle_result

    refresh_summary = dict(result.get("refresh_summary") or {})
    refresh_summary["manual_review_assertion_ids"] = list(governed_truth.get("manual_review_ids") or [])
    refresh_summary["active_governed_assertion_ids"] = list(governed_truth.get("safe_assertion_ids") or [])
    refresh_summary["replaced_cleanup_ids"] = list((cleanup_result or {}).get("archived_duplicate_ids") or [])
    result["refresh_summary"] = refresh_summary

    recompute = dict(result.get("recompute") or {})
    profile = dict(recompute.get("profile") or {})
    profile["governed_truth"] = governed_truth
    profile["governed_active_assertion_ids"] = list(governed_truth.get("safe_assertion_ids") or [])
    profile["manual_review_assertion_ids"] = list(governed_truth.get("manual_review_ids") or [])
    recompute["profile"] = profile
    result["recompute"] = recompute
    return result


_chunk42_original_refresh_market_policy_pipeline = refresh_market_policy_pipeline

def refresh_market_policy_pipeline(*args, **kwargs):
    result = _chunk42_original_refresh_market_policy_pipeline(*args, **kwargs)
    pipeline_result = dict(result.get("pipeline_result") or {})
    if pipeline_result.get("ok"):
        lifecycle_result = dict(pipeline_result.get("lifecycle_result") or {})
        governed_truth = dict((lifecycle_result.get("governed_truth") or {}))
        pipeline_result.setdefault("refresh_summary", {})["manual_review_assertion_ids"] = list(governed_truth.get("manual_review_ids") or [])
        pipeline_result.setdefault("refresh_summary", {})["active_governed_assertion_ids"] = list(governed_truth.get("safe_assertion_ids") or [])
    result["pipeline_result"] = pipeline_result
    return result


# --- Final profile bootstrap override ---
def _has_active_projectable_assertions_for_market(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> bool:
    stmt = select(PolicyAssertion).where(PolicyAssertion.state == _norm_state(state))
    stmt = stmt.where(PolicyAssertion.county == _norm_lower(county) if county is not None else PolicyAssertion.county.is_(None))
    stmt = stmt.where(PolicyAssertion.city == _norm_lower(city) if city is not None else PolicyAssertion.city.is_(None))
    if hasattr(PolicyAssertion, "pha_name"):
        if pha_name is None:
            stmt = stmt.where(or_(PolicyAssertion.pha_name.is_(None), PolicyAssertion.pha_name == ""))
        else:
            stmt = stmt.where(PolicyAssertion.pha_name == _norm_text(pha_name))
    if hasattr(PolicyAssertion, "org_id"):
        if org_id is None:
            stmt = stmt.where(PolicyAssertion.org_id.is_(None))
        else:
            stmt = stmt.where(or_(PolicyAssertion.org_id == org_id, PolicyAssertion.org_id.is_(None)))

    rows = list(db.scalars(stmt).all())
    for row in rows:
        if not getattr(row, "normalized_category", None):
            continue
        if (getattr(row, "review_status", None) or "").lower() != "verified":
            continue
        if (getattr(row, "governance_state", None) or "").lower() != "active":
            continue
        if (getattr(row, "rule_status", None) or "").lower() != "active":
            continue
        if not bool(getattr(row, "is_current", False)):
            continue
        return True
    return False


def _bootstrap_profile_if_missing(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
    notes: str | None = None,
) -> JurisdictionProfile | None:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    existing = _find_matching_profile(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )
    if existing is not None:
        return existing

    projected = project_verified_assertions_to_profile(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        notes=notes,
    )
    if projected is not None:
        return projected

    if not _has_active_projectable_assertions_for_market(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    ):
        return None

    profile = JurisdictionProfile()
    if hasattr(profile, "org_id"):
        profile.org_id = org_id
    if hasattr(profile, "state"):
        profile.state = st
    if hasattr(profile, "county"):
        profile.county = cnty
    if hasattr(profile, "city"):
        profile.city = cty
    if hasattr(profile, "pha_name"):
        profile.pha_name = pha
    if hasattr(profile, "notes"):
        profile.notes = notes or "Bootstrapped from active verified policy assertions."
    if hasattr(profile, "policy_json"):
        profile.policy_json = json.dumps(
            {
                "bootstrapped": True,
                "bootstrapped_from": "policy_pipeline_service",
                "state": st,
                "county": cnty,
                "city": cty,
                "pha_name": pha,
            },
            ensure_ascii=False,
        )
    if hasattr(profile, "refresh_state"):
        profile.refresh_state = "degraded"
    if hasattr(profile, "refresh_status_reason"):
        profile.refresh_status_reason = "bootstrapped_from_active_verified_assertions"

    db.add(profile)
    db.commit()
    db.refresh(profile)
    return profile


_final_original_run_market_policy_pipeline = run_market_policy_pipeline

def run_market_policy_pipeline(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = "se_mi_extended",
    reviewer_user_id: int | None = None,
    auto_activate: bool = True,
) -> dict[str, Any]:
    result = _final_original_run_market_policy_pipeline(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
        reviewer_user_id=reviewer_user_id,
        auto_activate=auto_activate,
    )

    try:
        profile = _bootstrap_profile_if_missing(
            db,
            org_id=org_id,
            state=state,
            county=county,
            city=city,
            pha_name=pha_name,
            notes="Bootstrapped after run_market_policy_pipeline.",
        )
        if profile is not None:
            recompute = dict(result.get("recompute") or {})
            recompute["jurisdiction_profile_id"] = int(getattr(profile, "id", 0) or 0)
            result["recompute"] = recompute
            if "health" not in result or result.get("health") is None:
                try:
                    result["health"] = _chunk3_pipeline_get_jurisdiction_health(db, profile_id=int(profile.id))
                except Exception:
                    pass
    except Exception as exc:
        result["profile_bootstrap_error"] = str(exc)

    return result


def run_market_pipeline(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = "se_mi_extended",
    reviewer_user_id: int | None = None,
    auto_activate: bool = True,
) -> dict[str, Any]:
    """
    Backward-compatible alias expected by routers/policy.py.
    """
    return run_market_policy_pipeline(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
        reviewer_user_id=reviewer_user_id,
        auto_activate=auto_activate,
    )


# --- Final hardened profile persistence + finalization override ---
from datetime import datetime as _pp_datetime

def _profile_set_if_present(profile: JurisdictionProfile, field_name: str, value) -> None:
    if hasattr(profile, field_name):
        try:
            setattr(profile, field_name, value)
        except Exception:
            pass


def _utcnow_profile() -> str:
    return _pp_datetime.utcnow().isoformat()


def _build_bootstrap_policy_payload(
    *,
    state: str,
    county: str | None,
    city: str | None,
    pha_name: str | None,
) -> dict[str, Any]:
    return {
        "bootstrapped": True,
        "bootstrapped_from": "policy_pipeline_service_hardened",
        "state": state,
        "county": county,
        "city": city,
        "pha_name": pha_name,
        "created_at": _utcnow_profile(),
        "coverage": {
            "coverage_status": "partial",
            "production_readiness": "not_ready",
            "completeness_status": "partial",
            "completeness_score": 0.0,
        },
    }


def _populate_profile_defaults(
    profile: JurisdictionProfile,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
    notes: str | None,
) -> None:
    _profile_set_if_present(profile, "org_id", org_id)
    _profile_set_if_present(profile, "state", state)
    _profile_set_if_present(profile, "county", county)
    _profile_set_if_present(profile, "city", city)
    _profile_set_if_present(profile, "pha_name", pha_name)
    _profile_set_if_present(profile, "notes", notes or "Bootstrapped from active verified policy assertions.")
    _profile_set_if_present(profile, "policy_json", json.dumps(_build_bootstrap_policy_payload(
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    ), ensure_ascii=False))
    _profile_set_if_present(profile, "refresh_state", "degraded")
    _profile_set_if_present(profile, "refresh_status_reason", "bootstrapped_from_active_verified_assertions")
    _profile_set_if_present(profile, "coverage_status", "partial")
    _profile_set_if_present(profile, "production_readiness", "not_ready")
    _profile_set_if_present(profile, "completeness_status", "partial")
    _profile_set_if_present(profile, "completeness_score", 0.0)
    _profile_set_if_present(profile, "coverage_confidence", "low")
    _profile_set_if_present(profile, "friction_multiplier", 1.0)
    _profile_set_if_present(profile, "source_count", 0)
    _profile_set_if_present(profile, "verified_rule_count", 0)
    _profile_set_if_present(profile, "fetch_failure_count", 0)
    _profile_set_if_present(profile, "stale_warning_count", 0)
    _profile_set_if_present(profile, "is_stale", False)
    _profile_set_if_present(profile, "last_refresh_completed_at", None)
    _profile_set_if_present(profile, "last_refresh_success_at", None)
    _profile_set_if_present(profile, "last_validation_at", None)


def _persist_bootstrap_profile(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
    notes: str | None,
) -> JurisdictionProfile | None:
    profile = JurisdictionProfile()
    _populate_profile_defaults(
        profile,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        notes=notes,
    )
    try:
        db.add(profile)
        db.commit()
        db.refresh(profile)
        return profile
    except Exception:
        db.rollback()

    # Second attempt with even slimmer/default-safe payload
    profile = JurisdictionProfile()
    _profile_set_if_present(profile, "org_id", org_id)
    _profile_set_if_present(profile, "state", state)
    _profile_set_if_present(profile, "county", county)
    _profile_set_if_present(profile, "city", city)
    _profile_set_if_present(profile, "pha_name", pha_name)
    _profile_set_if_present(profile, "notes", notes or "Bootstrapped from active verified policy assertions.")
    _profile_set_if_present(profile, "refresh_state", "degraded")
    _profile_set_if_present(profile, "refresh_status_reason", "bootstrapped_from_active_verified_assertions")
    try:
        db.add(profile)
        db.commit()
        db.refresh(profile)
        return profile
    except Exception:
        db.rollback()
        return None


def _bootstrap_profile_if_missing_hardened(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
    notes: str | None = None,
) -> JurisdictionProfile | None:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    existing = _find_matching_profile(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )
    if existing is not None:
        return existing

    try:
        projected = project_verified_assertions_to_profile(
            db,
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
            notes=notes,
        )
        if projected is not None:
            return projected
    except Exception:
        db.rollback()

    if not _has_active_projectable_assertions_for_market(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    ):
        return None

    return _persist_bootstrap_profile(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        notes=notes,
    )


_hardened_original_run_market_policy_pipeline = run_market_policy_pipeline

def run_market_policy_pipeline(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = "se_mi_extended",
    reviewer_user_id: int | None = None,
    auto_activate: bool = True,
) -> dict[str, Any]:
    result = _hardened_original_run_market_policy_pipeline(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
        reviewer_user_id=reviewer_user_id,
        auto_activate=auto_activate,
    )

    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    profile = None
    bootstrap_error = None
    try:
        profile = _bootstrap_profile_if_missing_hardened(
            db,
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
            notes="Bootstrapped after run_market_policy_pipeline (hardened).",
        )
    except Exception as exc:
        db.rollback()
        bootstrap_error = str(exc)

    if profile is None:
        result["profile_bootstrap_error"] = bootstrap_error or result.get("profile_bootstrap_error") or "profile_not_created"
        return result

    try:
        refresh_results = []
        for row in list(result.get("source_runs") or []):
            if isinstance(row, dict) and isinstance(row.get("refresh"), dict):
                refresh_results.append(dict(row["refresh"]))

        finalized = _chunk3_finalize_jurisdiction_profile_lifecycle(
            db,
            profile=profile,
            refresh_results=refresh_results,
            discovery_result=result.get("discovery_result") if isinstance(result.get("discovery_result"), dict) else None,
            governance_result=result.get("lifecycle_result") if isinstance(result.get("lifecycle_result"), dict) else None,
        )

        recompute = dict(result.get("recompute") or {})
        recompute["jurisdiction_profile_id"] = int(getattr(profile, "id", 0) or 0)
        recompute["profile"] = finalized.get("completeness")
        recompute["coverage"] = finalized.get("coverage")
        result["recompute"] = recompute
        result["health"] = finalized.get("health")
        result["lockout"] = finalized.get("lockout")
        result["sla_summary"] = finalized.get("sla_summary")
        result["requirements"] = finalized.get("requirements")
        result["profile_bootstrap_error"] = None
    except Exception as exc:
        db.rollback()
        recompute = dict(result.get("recompute") or {})
        recompute["jurisdiction_profile_id"] = int(getattr(profile, "id", 0) or 0)
        result["recompute"] = recompute
        result["profile_bootstrap_error"] = f"finalization_failed: {exc}"

    return result


_hardened_original_refresh_market_policy_pipeline = refresh_market_policy_pipeline

def refresh_market_policy_pipeline(
    db: Session,
    *args,
    **kwargs,
) -> dict[str, Any]:
    result = _hardened_original_refresh_market_policy_pipeline(db, *args, **kwargs)
    pipeline_result = dict(result.get("pipeline_result") or {})
    if pipeline_result.get("ok"):
        state = kwargs.get("state")
        county = kwargs.get("county")
        city = kwargs.get("city")
        pha_name = kwargs.get("pha_name")
        org_id = kwargs.get("org_id")
        profile = _find_matching_profile(
            db,
            org_id=org_id,
            state=_norm_state(state),
            county=_norm_lower(county),
            city=_norm_lower(city),
            pha_name=_norm_text(pha_name),
        )
        if profile is not None:
            try:
                pipeline_result["health"] = _chunk3_pipeline_get_jurisdiction_health(db, profile_id=int(profile.id))
            except Exception:
                pass
            recompute = dict(pipeline_result.get("recompute") or {})
            recompute["jurisdiction_profile_id"] = int(getattr(profile, "id", 0) or 0)
            pipeline_result["recompute"] = recompute
    result["pipeline_result"] = pipeline_result
    return result


def run_market_pipeline(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = "se_mi_extended",
    reviewer_user_id: int | None = None,
    auto_activate: bool = True,
) -> dict[str, Any]:
    return run_market_policy_pipeline(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
        reviewer_user_id=reviewer_user_id,
        auto_activate=auto_activate,
    )


# --- Final forced profile bootstrap + stale ORM hardening override ---
def _safe_profile_id(profile: JurisdictionProfile | None) -> int | None:
    if profile is None:
        return None
    try:
        value = getattr(profile, "id", None)
        return int(value) if value is not None else None
    except ObjectDeletedError:
        return None
    except Exception:
        return None


def _refetch_profile_by_scope(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> JurisdictionProfile | None:
    return _find_matching_profile(
        db,
        org_id=org_id,
        state=_norm_state(state),
        county=_norm_lower(county),
        city=_norm_lower(city),
        pha_name=_norm_text(pha_name),
    )


def _profile_set_if_present(profile: JurisdictionProfile, field_name: str, value) -> None:
    if hasattr(profile, field_name):
        try:
            setattr(profile, field_name, value)
        except Exception:
            pass


def _create_bootstrap_profile(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
    notes: str | None,
) -> JurisdictionProfile | None:
    profile = JurisdictionProfile()
    _profile_set_if_present(profile, "org_id", org_id)
    _profile_set_if_present(profile, "state", _norm_state(state))
    _profile_set_if_present(profile, "county", _norm_lower(county))
    _profile_set_if_present(profile, "city", _norm_lower(city))
    _profile_set_if_present(profile, "pha_name", _norm_text(pha_name))
    _profile_set_if_present(profile, "notes", notes or "Bootstrapped from active verified policy assertions.")
    _profile_set_if_present(profile, "refresh_state", "review_required")
    _profile_set_if_present(profile, "refresh_status_reason", "bootstrapped_from_active_verified_assertions")
    if hasattr(profile, "policy_json"):
        _profile_set_if_present(profile, "policy_json", json.dumps({
            "bootstrapped": True,
            "bootstrapped_from": "policy_pipeline_service",
            "state": _norm_state(state),
            "county": _norm_lower(county),
            "city": _norm_lower(city),
            "pha_name": _norm_text(pha_name),
        }, ensure_ascii=False))
    try:
        db.add(profile)
        db.commit()
        pid = _safe_profile_id(profile)
        if pid is not None:
            fresh = db.get(JurisdictionProfile, pid)
            if fresh is not None:
                return fresh
        return _refetch_profile_by_scope(
            db,
            org_id=org_id,
            state=state,
            county=county,
            city=city,
            pha_name=pha_name,
        )
    except Exception:
        db.rollback()
        return None


def _force_find_or_create_profile(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
    notes: str | None = None,
) -> JurisdictionProfile | None:
    existing = _refetch_profile_by_scope(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )
    if existing is not None:
        return existing
    try:
        projected = project_verified_assertions_to_profile(
            db,
            org_id=org_id,
            state=_norm_state(state),
            county=_norm_lower(county),
            city=_norm_lower(city),
            pha_name=_norm_text(pha_name),
            notes=notes,
        )
        pid = _safe_profile_id(projected)
        if pid is not None:
            fresh = db.get(JurisdictionProfile, pid)
            if fresh is not None:
                return fresh
    except Exception:
        db.rollback()
    existing = _refetch_profile_by_scope(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )
    if existing is not None:
        return existing
    if _has_active_projectable_assertions_for_market(
        db,
        org_id=org_id,
        state=_norm_state(state),
        county=_norm_lower(county),
        city=_norm_lower(city),
        pha_name=_norm_text(pha_name),
    ):
        return _create_bootstrap_profile(
            db,
            org_id=org_id,
            state=state,
            county=county,
            city=city,
            pha_name=pha_name,
            notes=notes,
        )
    return None


_pipeline_final_orig = run_market_policy_pipeline

def run_market_policy_pipeline(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = "se_mi_extended",
    reviewer_user_id: int | None = None,
    auto_activate: bool = True,
) -> dict[str, Any]:
    result = _pipeline_final_orig(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
        reviewer_user_id=reviewer_user_id,
        auto_activate=auto_activate,
    )
    if not result.get("ok"):
        return result

    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    profile = _force_find_or_create_profile(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        notes="Forced bootstrap after run_market_policy_pipeline.",
    )
    if profile is None:
        result["profile_bootstrap_error"] = result.get("profile_bootstrap_error") or "profile_not_created_after_forced_bootstrap"
        return result

    profile = _refetch_profile_by_scope(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )
    if profile is None:
        result["profile_bootstrap_error"] = "profile_not_refetchable_after_forced_bootstrap"
        return result

    refresh_results = []
    for row in list(result.get("source_runs") or []):
        if isinstance(row, dict) and isinstance(row.get("refresh"), dict):
            refresh_results.append(dict(row["refresh"]))

    try:
        finalized = _chunk3_finalize_jurisdiction_profile_lifecycle(
            db,
            profile=profile,
            refresh_results=refresh_results,
            discovery_result=result.get("discovery_result") if isinstance(result.get("discovery_result"), dict) else None,
            governance_result=result.get("lifecycle_result") if isinstance(result.get("lifecycle_result"), dict) else None,
        )
    except Exception as exc:
        db.rollback()
        result["profile_bootstrap_error"] = f"finalization_failed: {exc}"
        pid = _safe_profile_id(_refetch_profile_by_scope(db, org_id=org_id, state=st, county=cnty, city=cty, pha_name=pha))
        recompute = dict(result.get("recompute") or {})
        recompute["jurisdiction_profile_id"] = pid
        result["recompute"] = recompute
        return result

    profile = _refetch_profile_by_scope(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )
    pid = _safe_profile_id(profile)
    recompute = dict(result.get("recompute") or {})
    recompute["jurisdiction_profile_id"] = pid
    recompute["profile"] = finalized.get("completeness")
    recompute["coverage"] = finalized.get("coverage")
    result["recompute"] = recompute
    result["health"] = finalized.get("health")
    result["lockout"] = finalized.get("lockout")
    result["sla_summary"] = finalized.get("sla_summary")
    result["requirements"] = finalized.get("requirements")
    result["profile_bootstrap_error"] = None
    return result


_refresh_pipeline_final_orig = refresh_market_policy_pipeline

def refresh_market_policy_pipeline(
    db: Session,
    *args,
    **kwargs,
) -> dict[str, Any]:
    result = _refresh_pipeline_final_orig(db, *args, **kwargs)
    pipeline_result = dict(result.get("pipeline_result") or {})
    if pipeline_result.get("ok"):
        state = kwargs.get("state")
        county = kwargs.get("county")
        city = kwargs.get("city")
        pha_name = kwargs.get("pha_name")
        org_id = kwargs.get("org_id")
        profile = _refetch_profile_by_scope(
            db,
            org_id=org_id,
            state=_norm_state(state),
            county=_norm_lower(county),
            city=_norm_lower(city),
            pha_name=_norm_text(pha_name),
        )
        if profile is not None:
            try:
                pipeline_result["health"] = _chunk3_pipeline_get_jurisdiction_health(db, profile_id=int(_safe_profile_id(profile) or 0))
            except Exception:
                pass
            recompute = dict(pipeline_result.get("recompute") or {})
            recompute["jurisdiction_profile_id"] = _safe_profile_id(profile)
            pipeline_result["recompute"] = recompute
    result["pipeline_result"] = pipeline_result
    return result
