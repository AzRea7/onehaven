# backend/app/services/policy_pipeline_service.py
from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.policy_models import JurisdictionProfile, PolicyAssertion, PolicySource
from app.services.jurisdiction_completeness_service import (
    profile_completeness_payload,
    recompute_profile_and_coverage,
)
from app.services.jurisdiction_notification_service import build_review_queue_payload
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

    return {
        "ok": True,
        "expected_inventory": inventory_hints,
        "inventory_summary": inventory_summary,
        "recomputed": True,
        "jurisdiction_profile_id": int(refreshed_profile.id),
        "profile": profile_completeness_payload(db, refreshed_profile),
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
                "extract_result": extract_result,
                "diff": diff_result,
                "normalized": normalize_result,
                "validation": validation_result,
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

    inventory_hints = expected_inventory_hints(state=st, county=cnty, city=cty, pha_name=pha, include_section8=True)
    missing_categories = _discovery_missing_categories(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )
    discovery_result = discover_policy_sources_for_market(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        missing_categories=missing_categories,
        focus=focus,
        probe=True,
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

    return {
        "ok": True,
        "missing_categories": missing_categories,
        "expected_inventory": inventory_hints,
        "discovery_result": discovery_result,
        "sources_processed": len(refresh_batch["source_runs"]),
        "total_changed_rules": int(refresh_batch["summary"]["changed_rule_count"]),
        "changed_source_count": int(refresh_batch["summary"]["changed_source_count"]),
        "failed_source_count": int(refresh_batch["summary"]["failed_source_count"]),
        "source_runs": refresh_batch["source_runs"],
        "refresh_summary": refresh_batch["summary"],
        "refresh_state": refresh_state_summary,
        "lifecycle_result": lifecycle_result,
        "recompute": recompute,
        "review_queue": review_queue,
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
