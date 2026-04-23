# backend/app/services/policy_pipeline_service.py
from __future__ import annotations

import json
from datetime import datetime as _pp_datetime
from typing import Any, Optional

from sqlalchemy import inspect, or_, select
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import ObjectDeletedError

from app.policy_models import JurisdictionProfile, PolicyAssertion, PolicySource
from app.services.policy_coverage.completeness_service import (
    profile_completeness_payload,
    recompute_profile_and_coverage,
)
from app.services.policy_governance.notification_service import (
    build_gap_escalation_notifications,
    build_review_queue_payload,
)
from app.services.policy_coverage.health_service import (
    get_jurisdiction_health as _chunk3_pipeline_get_jurisdiction_health,
)
from app.services.policy_governance.refresh_service import (
    finalize_jurisdiction_profile_lifecycle as _chunk3_finalize_jurisdiction_profile_lifecycle,
)
from app.services.policy_governance.rules_service import governed_assertions_for_scope
from app.services.policy_coverage.sla_service import (
    build_refresh_requirements,
    collect_profile_source_sla_summary,
)
from app.services.policy_sources.catalog_admin_service import merged_catalog_for_market
from app.services.policy_change_detection_service import summarize_refresh_runs
from app.services.policy_assertions.cleanup_service import (
    ARCHIVE_MARKER,
    archive_stale_market_sources,
    cleanup_non_projectable_assertions_for_market,
)
from app.services.policy_coverage.coverage_service import (
    compute_coverage_status,
    upsert_coverage_status,
)
from app.services.policy_sources.discovery_service import expected_inventory_hints
from app.services.policy_assertions.extractor_service import (
    extract_assertions_for_source,
    mark_assertions_stale_for_source,
)
from app.services.compliance_engine.projection_service import (
    build_property_compliance_brief,
    project_verified_assertions_to_profile,
)
from app.services.policy_assertions.review_service import (
    apply_governance_lifecycle,
    auto_verify_market_assertions,
    cleanup_market_stale_assertions,
    diff_active_rules_for_source,
    normalize_market_assertions,
    supersede_replaced_assertions,
)
from app.services.policy_sources.source_service import (
    collect_catalog_for_market,
    discover_policy_sources_for_market,
    inventory_summary_for_market,
    list_sources_for_market,
    refresh_policy_source_and_detect_changes,
)
from app.services.policy_assertions.validation_service import validate_market_assertions
from app.services.policy_sources.crawl_inventory_service import sync_crawl_result_to_inventory


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


def _safe_profile_pk(profile: JurisdictionProfile | None) -> int | None:
    if profile is None:
        return None
    try:
        state = inspect(profile)
        identity = getattr(state, "identity", None)
        if identity and len(identity) > 0 and identity[0] is not None:
            return int(identity[0])
    except Exception:
        pass
    try:
        value = getattr(profile, "id", None)
        return int(value) if value is not None else None
    except ObjectDeletedError:
        return None
    except Exception:
        return None


def _safe_profile_id(profile: JurisdictionProfile | None) -> int | None:
    return _safe_profile_pk(profile)


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
    requested_pha = _norm_text(pha_name)

    for row in rows:
        row_county = _norm_lower(getattr(row, "county", None))
        row_city = _norm_lower(getattr(row, "city", None))
        row_pha = _norm_text(getattr(row, "pha_name", None)) if _profile_has_attr("pha_name") else None

        if row_county not in {None, county}:
            continue
        if row_city not in {None, city}:
            continue
        if _profile_has_attr("pha_name") and requested_pha is not None and row_pha not in {None, requested_pha}:
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


def _refetch_profile_by_pk_or_scope(
    db: Session,
    *,
    profile_id: int | None,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> JurisdictionProfile | None:
    if profile_id:
        try:
            row = db.get(JurisdictionProfile, int(profile_id))
            if row is not None:
                return row
        except Exception:
            db.rollback()
    return _find_matching_profile(
        db,
        org_id=org_id,
        state=_norm_state(state),
        county=_norm_lower(county),
        city=_norm_lower(city),
        pha_name=_norm_text(pha_name),
    )


def _safe_profile_identity_or_lookup(
    db: Session,
    *,
    profile: JurisdictionProfile | None,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> tuple[JurisdictionProfile | None, int | None]:
    pid = _safe_profile_id(profile)
    if pid is not None:
        try:
            fresh = db.get(JurisdictionProfile, int(pid))
            if fresh is not None:
                return fresh, int(pid)
        except Exception:
            db.rollback()
    fresh = _refetch_profile_by_scope(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )
    return fresh, _safe_profile_id(fresh)


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

def _profile_has_attr(name: str) -> bool:
    return hasattr(JurisdictionProfile, name)


def _profile_pha_value(profile: JurisdictionProfile | None) -> str | None:
    if profile is None:
        return None
    return _norm_text(getattr(profile, "pha_name", None))


def _build_market_compliance_brief(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> dict[str, Any]:
    """
    Safely build a market-scope brief even when build_property_compliance_brief
    has been overridden elsewhere to a property-only signature.
    """
    try:
        return dict(
            build_property_compliance_brief(
                db,
                org_id=org_id,
                state=state,
                county=county,
                city=city,
                pha_name=pha_name,
            )
        )
    except TypeError:
        governed = governed_assertions_for_scope(
            db,
            org_id=org_id,
            state=state,
            county=county,
            city=city,
            pha_name=pha_name,
        )
        category_counts = dict(governed.get("category_counts") or {})
        covered_categories = sorted(
            cat
            for cat, counts in category_counts.items()
            if int((counts or {}).get("safe", 0) or 0) > 0 or int((counts or {}).get("partial", 0) or 0) > 0
        )
        return {
            "ok": True,
            "scope": {
                "state": state,
                "county": county,
                "city": city,
                "pha_name": pha_name,
            },
            "covered_categories": covered_categories,
            "counts": {
                "safe": len(list(governed.get("safe_assertion_ids") or [])),
                "partial": len(list(governed.get("partial_assertion_ids") or [])),
                "manual_review": len(list(governed.get("manual_review_ids") or [])),
                "excluded": len(list(governed.get("excluded_assertion_ids") or [])),
            },
            "category_counts": category_counts,
            "assertion_ids": {
                "safe": list(governed.get("safe_assertion_ids") or []),
                "partial": list(governed.get("partial_assertion_ids") or []),
                "manual_review": list(governed.get("manual_review_ids") or []),
                "excluded": list(governed.get("excluded_assertion_ids") or []),
            },
        }


def _create_empty_market_profile(
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

    profile = JurisdictionProfile()

    if hasattr(profile, "org_id"):
        profile.org_id = org_id
    if hasattr(profile, "state"):
        profile.state = _norm_state(state)
    if hasattr(profile, "county"):
        profile.county = _norm_lower(county)
    if hasattr(profile, "city"):
        profile.city = _norm_lower(city)
    if _profile_has_attr("pha_name"):
        setattr(profile, "pha_name", _norm_text(pha_name))
    if hasattr(profile, "notes"):
        profile.notes = notes or "Bootstrapped empty jurisdiction profile for market scope."
    if hasattr(profile, "policy_json"):
        profile.policy_json = json.dumps(
            {
                "bootstrapped": True,
                "bootstrapped_from": "empty_market_scope",
                "state": _norm_state(state),
                "county": _norm_lower(county),
                "city": _norm_lower(city),
                "pha_name": _norm_text(pha_name),
            },
            ensure_ascii=False,
        )
    if hasattr(profile, "refresh_state"):
        profile.refresh_state = "pending"
    if hasattr(profile, "refresh_status_reason"):
        profile.refresh_status_reason = "bootstrapped_empty_market_scope"
    if hasattr(profile, "completeness_status"):
        profile.completeness_status = "unknown"
    if hasattr(profile, "completeness_score"):
        profile.completeness_score = 0.0
    if hasattr(profile, "confidence_score"):
        profile.confidence_score = 0.0
    if hasattr(profile, "missing_categories_json"):
        profile.missing_categories_json = "[]"
    if hasattr(profile, "stale_categories_json"):
        profile.stale_categories_json = "[]"
    if hasattr(profile, "inferred_categories_json"):
        profile.inferred_categories_json = "[]"
    if hasattr(profile, "conflicting_categories_json"):
        profile.conflicting_categories_json = "[]"
    if hasattr(profile, "required_categories_json"):
        profile.required_categories_json = "[]"
    if hasattr(profile, "covered_categories_json"):
        profile.covered_categories_json = "[]"
    if hasattr(profile, "unmet_categories_json"):
        profile.unmet_categories_json = "[]"
    if hasattr(profile, "undiscovered_categories_json"):
        profile.undiscovered_categories_json = "[]"
    if hasattr(profile, "weak_support_categories_json"):
        profile.weak_support_categories_json = "[]"
    if hasattr(profile, "authority_unmet_categories_json"):
        profile.authority_unmet_categories_json = "[]"

    try:
        db.add(profile)
        db.commit()
        db.refresh(profile)
        return profile
    except Exception:
        db.rollback()
        return _refetch_profile_by_scope(
            db,
            org_id=org_id,
            state=state,
            county=county,
            city=city,
            pha_name=pha_name,
        )

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
        profile = _create_empty_market_profile(
            db,
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
            notes="Bootstrapped during recompute for missing market profile.",
        )

    if profile is None:
        return {
            "ok": True,
            "expected_inventory": inventory_hints,
            "inventory_summary": inventory_summary,
            "recomputed": False,
            "jurisdiction_profile_id": None,
            "profile_error": "jurisdiction_profile_not_created_for_market_scope",
        }

    profile_id = _safe_profile_pk(profile)

    refreshed_profile, coverage = recompute_profile_and_coverage(
        db,
        profile,
        commit=True,
    )

    refreshed_profile_id = _safe_profile_pk(refreshed_profile) or profile_id
    refreshed_profile = _refetch_profile_by_pk_or_scope(
        db,
        profile_id=refreshed_profile_id,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )

    if refreshed_profile is None:
        return {
            "ok": True,
            "expected_inventory": inventory_hints,
            "inventory_summary": inventory_summary,
            "recomputed": False,
            "jurisdiction_profile_id": None,
            "profile_error": "jurisdiction_profile_not_found_after_recompute",
        }

    refreshed_profile_id = _safe_profile_pk(refreshed_profile)

    completeness_payload = profile_completeness_payload(db, refreshed_profile)
    sla_summary = collect_profile_source_sla_summary(db, profile=refreshed_profile)
    refresh_requirements = build_refresh_requirements(
        refreshed_profile,
        next_step="refresh" if list(
            sla_summary.get("legal_overdue_categories")
            or completeness_payload.get("critical_stale_categories")
            or []
        ) else "monitor",
        missing_categories=list(completeness_payload.get("missing_categories") or []),
        stale_categories=list(completeness_payload.get("stale_categories") or []),
        overdue_categories=list(sla_summary.get("overdue_categories") or []),
        critical_overdue_categories=list(sla_summary.get("critical_overdue_categories") or []),
        legal_overdue_categories=list(sla_summary.get("legal_overdue_categories") or []),
        informational_overdue_categories=list(sla_summary.get("informational_overdue_categories") or []),
        stale_authoritative_categories=list(sla_summary.get("stale_authoritative_categories") or []),
        inventory_summary={
            **dict(sla_summary),
            "inventory_summary": inventory_summary,
        },
    )

    if hasattr(refreshed_profile, "refresh_requirements_json"):
        import json as _json

        refreshed_profile.refresh_requirements_json = _json.dumps(refresh_requirements, sort_keys=True, default=str)
        db.add(refreshed_profile)
        db.commit()

        refreshed_profile = _refetch_profile_by_pk_or_scope(
            db,
            profile_id=refreshed_profile_id,
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
        )
        refreshed_profile_id = _safe_profile_pk(refreshed_profile)

    return {
        "ok": True,
        "expected_inventory": inventory_hints,
        "inventory_summary": inventory_summary,
        "recomputed": True,
        "jurisdiction_profile_id": refreshed_profile_id,
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
        inventory_result = sync_crawl_result_to_inventory(db, source=source, fetch_result=fetch_result)
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
                "inventory": inventory_result,
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
            "inventory_sync_failed_source_ids": [
                row["source_id"] for row in refresh_batch["source_runs"]
                if not bool((row.get("inventory") or {}).get("ok"))
            ],
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
    inventory_result = sync_crawl_result_to_inventory(db, source=source, fetch_result=fetch_result)
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
        "inventory_result": inventory_result,
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

def _create_empty_market_profile(
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

    profile = JurisdictionProfile()
    if hasattr(profile, "org_id"):
        profile.org_id = org_id
    if hasattr(profile, "state"):
        profile.state = _norm_state(state)
    if hasattr(profile, "county"):
        profile.county = _norm_lower(county)
    if hasattr(profile, "city"):
        profile.city = _norm_lower(city)
    if hasattr(profile, "pha_name"):
        profile.pha_name = _norm_text(pha_name)
    if hasattr(profile, "notes"):
        profile.notes = notes or "Bootstrapped empty jurisdiction profile for market scope."
    if hasattr(profile, "policy_json"):
        profile.policy_json = json.dumps(
            {
                "bootstrapped": True,
                "bootstrapped_from": "empty_market_scope",
                "state": _norm_state(state),
                "county": _norm_lower(county),
                "city": _norm_lower(city),
                "pha_name": _norm_text(pha_name),
            },
            ensure_ascii=False,
        )
    if hasattr(profile, "refresh_state"):
        profile.refresh_state = "pending"
    if hasattr(profile, "refresh_status_reason"):
        profile.refresh_status_reason = "bootstrapped_empty_market_scope"
    if hasattr(profile, "completeness_status"):
        profile.completeness_status = "unknown"
    if hasattr(profile, "completeness_score"):
        profile.completeness_score = 0.0
    if hasattr(profile, "confidence_score"):
        profile.confidence_score = 0.0
    if hasattr(profile, "missing_categories_json"):
        profile.missing_categories_json = "[]"
    if hasattr(profile, "stale_categories_json"):
        profile.stale_categories_json = "[]"
    if hasattr(profile, "inferred_categories_json"):
        profile.inferred_categories_json = "[]"
    if hasattr(profile, "conflicting_categories_json"):
        profile.conflicting_categories_json = "[]"
    if hasattr(profile, "required_categories_json"):
        profile.required_categories_json = "[]"
    if hasattr(profile, "covered_categories_json"):
        profile.covered_categories_json = "[]"
    if hasattr(profile, "unmet_categories_json"):
        profile.unmet_categories_json = "[]"
    if hasattr(profile, "undiscovered_categories_json"):
        profile.undiscovered_categories_json = "[]"
    if hasattr(profile, "weak_support_categories_json"):
        profile.weak_support_categories_json = "[]"
    if hasattr(profile, "authority_unmet_categories_json"):
        profile.authority_unmet_categories_json = "[]"

    try:
        db.add(profile)
        db.commit()
        db.refresh(profile)
        return profile
    except Exception:
        db.rollback()
        return _refetch_profile_by_scope(
            db,
            org_id=org_id,
            state=state,
            county=county,
            city=city,
            pha_name=pha_name,
        )


def mark_source_assertions_stale(
    db: Session,
    *,
    source_id: int,
    reason: str = "source_refreshed",
) -> dict[str, Any]:
    return mark_assertions_stale_for_source(db, source_id=source_id, reason=reason)


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
        profile, profile_id = _safe_profile_identity_or_lookup(
            db,
            profile=profile,
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
        )
        if profile is not None and profile_id is not None:
            try:
                pipeline_result["health"] = _chunk3_pipeline_get_jurisdiction_health(db, profile_id=int(profile_id))
            except Exception:
                pass
            recompute = dict(pipeline_result.get("recompute") or {})
            recompute["jurisdiction_profile_id"] = profile_id
            pipeline_result["recompute"] = recompute
    result["pipeline_result"] = pipeline_result
    return result


_chunk42_original_run_market_policy_pipeline = run_market_policy_pipeline
_chunk42_original_refresh_market_policy_pipeline = refresh_market_policy_pipeline


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
    profile_payload = dict(recompute.get("profile") or {})
    profile_payload["governed_truth"] = governed_truth
    profile_payload["governed_active_assertion_ids"] = list(governed_truth.get("safe_assertion_ids") or [])
    profile_payload["manual_review_assertion_ids"] = list(governed_truth.get("manual_review_ids") or [])
    recompute["profile"] = profile_payload
    result["recompute"] = recompute
    return result


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

    return _create_empty_market_profile(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        notes=notes,
    )

_final_original_run_market_policy_pipeline = run_market_policy_pipeline

def run_market_policy_pipeline(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: str,
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
            st = _norm_state(state)
            cnty = _norm_lower(county)
            cty = _norm_lower(city)
            pha = _norm_text(pha_name)
            profile, profile_id = _safe_profile_identity_or_lookup(
                db,
                profile=profile,
                org_id=org_id,
                state=st,
                county=cnty,
                city=cty,
                pha_name=pha,
            )
            recompute = dict(result.get("recompute") or {})
            recompute["jurisdiction_profile_id"] = profile_id
            result["recompute"] = recompute
            if "health" not in result or result.get("health") is None:
                try:
                    if profile_id is not None:
                        result["health"] = _chunk3_pipeline_get_jurisdiction_health(db, profile_id=int(profile_id))
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
    _profile_set_if_present(
        profile,
        "policy_json",
        json.dumps(
            _build_bootstrap_policy_payload(
                state=state,
                county=county,
                city=city,
                pha_name=pha_name,
            ),
            ensure_ascii=False,
        ),
    )
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

    return _create_empty_market_profile(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        notes=notes,
    )

_hardened_original_run_market_policy_pipeline = run_market_policy_pipeline
_hardened_original_refresh_market_policy_pipeline = refresh_market_policy_pipeline

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

        profile, profile_id = _safe_profile_identity_or_lookup(
            db,
            profile=profile,
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
        )
        if profile is None:
            recompute = dict(result.get("recompute") or {})
            recompute["jurisdiction_profile_id"] = profile_id
            result["recompute"] = recompute
            result["profile_bootstrap_error"] = "profile_not_refetchable_before_finalization"
            return result

        finalized = _chunk3_finalize_jurisdiction_profile_lifecycle(
            db,
            profile=profile,
            refresh_results=refresh_results,
            discovery_result=result.get("discovery_result") if isinstance(result.get("discovery_result"), dict) else None,
            governance_result=result.get("lifecycle_result") if isinstance(result.get("lifecycle_result"), dict) else None,
        )

        profile, profile_id = _safe_profile_identity_or_lookup(
            db,
            profile=profile,
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
        )

        recompute = dict(result.get("recompute") or {})
        recompute["jurisdiction_profile_id"] = profile_id
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
        profile, profile_id = _safe_profile_identity_or_lookup(
            db,
            profile=None,
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
        )
        recompute = dict(result.get("recompute") or {})
        recompute["jurisdiction_profile_id"] = profile_id
        result["recompute"] = recompute
        result["profile_bootstrap_error"] = f"finalization_failed: {exc}"

    return result


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
        profile, profile_id = _safe_profile_identity_or_lookup(
            db,
            profile=profile,
            org_id=org_id,
            state=_norm_state(state),
            county=_norm_lower(county),
            city=_norm_lower(city),
            pha_name=_norm_text(pha_name),
        )
        if profile is not None and profile_id is not None:
            try:
                pipeline_result["health"] = _chunk3_pipeline_get_jurisdiction_health(db, profile_id=int(profile_id))
            except Exception:
                pass
            recompute = dict(pipeline_result.get("recompute") or {})
            recompute["jurisdiction_profile_id"] = profile_id
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
        _profile_set_if_present(
            profile,
            "policy_json",
            json.dumps(
                {
                    "bootstrapped": True,
                    "bootstrapped_from": "policy_pipeline_service",
                    "state": _norm_state(state),
                    "county": _norm_lower(county),
                    "city": _norm_lower(city),
                    "pha_name": _norm_text(pha_name),
                },
                ensure_ascii=False,
            ),
        )
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

    return _create_empty_market_profile(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        notes=notes,
    )


_pipeline_final_orig = run_market_policy_pipeline
_refresh_pipeline_final_orig = refresh_market_policy_pipeline


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

    profile, profile_id = _safe_profile_identity_or_lookup(
        db,
        profile=profile,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )
    if profile is None:
        result["profile_bootstrap_error"] = "profile_not_refetchable_after_forced_bootstrap"
        recompute = dict(result.get("recompute") or {})
        recompute["jurisdiction_profile_id"] = profile_id
        result["recompute"] = recompute
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
        _, fresh_profile_id = _safe_profile_identity_or_lookup(
            db,
            profile=None,
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
        )
        recompute = dict(result.get("recompute") or {})
        recompute["jurisdiction_profile_id"] = fresh_profile_id
        result["recompute"] = recompute
        return result

    profile, profile_id = _safe_profile_identity_or_lookup(
        db,
        profile=profile,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )

    recompute = dict(result.get("recompute") or {})
    recompute["jurisdiction_profile_id"] = profile_id
    recompute["profile"] = finalized.get("completeness")
    recompute["coverage"] = finalized.get("coverage")
    result["recompute"] = recompute
    result["health"] = finalized.get("health")
    result["lockout"] = finalized.get("lockout")
    result["sla_summary"] = finalized.get("sla_summary")
    result["requirements"] = finalized.get("requirements")
    result["profile_bootstrap_error"] = None
    return result


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
        profile, profile_id = _safe_profile_identity_or_lookup(
            db,
            profile=profile,
            org_id=org_id,
            state=_norm_state(state),
            county=_norm_lower(county),
            city=_norm_lower(city),
            pha_name=_norm_text(pha_name),
        )
        if profile is not None and profile_id is not None:
            try:
                pipeline_result["health"] = _chunk3_pipeline_get_jurisdiction_health(db, profile_id=int(profile_id))
            except Exception:
                pass
            recompute = dict(pipeline_result.get("recompute") or {})
            recompute["jurisdiction_profile_id"] = profile_id
            pipeline_result["recompute"] = recompute
    result["pipeline_result"] = pipeline_result
    return result

# --- tier-one evidence-first final overrides ---


def _pipeline_refresh_rows(result: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in list((result or {}).get("source_runs") or []):
        if not isinstance(row, dict):
            continue
        refresh = row.get("refresh") if isinstance(row.get("refresh"), dict) else None
        if refresh is not None:
            rows.append(dict(refresh))
        else:
            compact = {k: v for k, v in row.items() if k in {"source_id", "ok", "changed", "reason", "change_detected"}}
            rows.append(compact)
    return rows


def _finalize_pipeline_market_state(
    db: Session,
    *,
    result: dict[str, Any],
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
    profile = _refetch_profile_by_scope(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )
    if profile is None:
        profile = _create_empty_market_profile(
            db,
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
            notes="Bootstrapped during evidence-first pipeline finalization.",
        )
    if profile is None:
        return result

    finalized = _chunk3_finalize_jurisdiction_profile_lifecycle(
        db,
        profile=profile,
        refresh_results=_pipeline_refresh_rows(result),
        discovery_result=result.get("discovery_result") if isinstance(result.get("discovery_result"), dict) else None,
        governance_result=result.get("lifecycle_result") if isinstance(result.get("lifecycle_result"), dict) else None,
    )
    recompute = dict(result.get("recompute") or {})
    recompute["jurisdiction_profile_id"] = (finalized.get("profile") or {}).get("id") or (recompute.get("jurisdiction_profile_id"))
    recompute["profile"] = finalized.get("completeness")
    recompute["coverage"] = finalized.get("coverage")
    result["recompute"] = recompute
    result["health"] = finalized.get("health")
    result["lockout"] = finalized.get("lockout")
    result["sla_summary"] = finalized.get("sla_summary")
    result["requirements"] = finalized.get("requirements")
    result["evidence_state"] = (finalized.get("requirements") or {}).get("evidence_state")
    return result


_tier1_pipeline_base_run_market_policy_pipeline = run_market_policy_pipeline
_tier1_pipeline_base_refresh_market_policy_pipeline = refresh_market_policy_pipeline


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
    result = _tier1_pipeline_base_run_market_policy_pipeline(
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
    try:
        return _finalize_pipeline_market_state(
            db,
            result=dict(result),
            org_id=org_id,
            state=state,
            county=county,
            city=city,
            pha_name=pha_name,
        )
    except Exception as exc:
        db.rollback()
        result = dict(result)
        result["pipeline_finalization_error"] = str(exc)
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
    result = _tier1_pipeline_base_refresh_market_policy_pipeline(
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
        try:
            pipeline_result = _finalize_pipeline_market_state(
                db,
                result=pipeline_result,
                org_id=org_id,
                state=state,
                county=county,
                city=city,
                pha_name=pha_name,
            )
        except Exception as exc:
            db.rollback()
            pipeline_result["pipeline_finalization_error"] = str(exc)
    result["pipeline_result"] = pipeline_result
    return result


# === targeted backfill pipeline overlay (current-architecture preserving) ===
_original_run_market_policy_pipeline = run_market_policy_pipeline

def _source_can_backfill_missing_categories(source: PolicySource, missing_categories: list[str]) -> bool:
    if not missing_categories:
        return False
    text = " ".join([
        str(getattr(source, "url", "") or ""),
        str(getattr(source, "title", "") or ""),
        str(getattr(source, "publisher", "") or ""),
        str(getattr(source, "notes", "") or ""),
        str(getattr(source, "normalized_categories_json", "") or ""),
    ]).lower()
    category_hints = set()
    for cat in missing_categories:
        raw = str(cat or "").strip().lower()
        if not raw:
            continue
        category_hints.add(raw)
    synonyms = {
        "rental_license": ["license", "registration", "certificate"],
        "source_of_income": ["source of income", "fair housing", "civil rights", "discrimination"],
        "permits": ["permit", "building"],
        "documents": ["document", "application", "packet", "submit", "form"],
        "contacts": ["contact", "department", "division", "office"],
        "fees": ["fee", "payment"],
        "program_overlay": ["voucher", "hcv", "hap", "section 8", "overlay", "nspire"],
        "lead": ["lead", "lead-safe", "lead paint"],
    }
    for cat in category_hints:
        if cat in text:
            return True
        for s in synonyms.get(cat, []):
            if s in text:
                return True
    return False

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
    missing_categories = _discovery_missing_categories(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )
    source_runs: list[dict[str, Any]] = []
    total_changed_rules = 0
    total_new_rules = 0
    total_missing_rules = 0
    changed_source_ids: list[int] = []
    failed_source_ids: list[int] = []

    for source in sources:
        fetch_result = refresh_policy_source_and_detect_changes(db, source=source)
        inventory_result = sync_crawl_result_to_inventory(db, source=source, fetch_result=fetch_result)
        content_changed = bool(fetch_result.get("change_detected") or fetch_result.get("changed"))
        should_backfill = (not content_changed) and _source_can_backfill_missing_categories(source, missing_categories)
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
            extract_result = {"ok": False, "created_count": 0, "assertion_ids": [], "stale_mark_result": stale_mark_result, "reason": "fetch_failed"}
            diff_result = {"ok": False, "changed_count": 0, "new_count": 0, "missing_count": 0, "reason": "fetch_failed"}
        else:
            raw_candidates: list[dict[str, Any]] = []
            if content_changed or should_backfill:
                if content_changed:
                    changed_source_ids.append(int(source.id))
                    mark_assertions_stale_for_source(db, source_id=int(source.id), reason="source_changed")
                extracted = extract_assertions_for_source(db, source=source, org_id=org_id, org_scope=(org_id is not None))
                raw_candidates = _extract_raw_candidates(extracted)
                extract_result = {
                    "ok": True,
                    "created_count": len(extracted),
                    "assertion_ids": [int(a.id) for a in extracted if getattr(a, "id", None) is not None],
                    "reason": "content_change" if content_changed else "backfill_missing_categories",
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
                extract_result = {"ok": True, "created_count": 0, "assertion_ids": [], "reason": "no_content_change"}
                diff_result = {"ok": True, "changed_count": 0, "new_count": 0, "missing_count": 0, "reason": "no_content_change"}

        total_changed_rules += int(diff_result.get("changed_count", 0) or 0)
        total_new_rules += int(diff_result.get("new_count", 0) or 0)
        total_missing_rules += int(diff_result.get("missing_count", 0) or 0)
        source_runs.append(
            {
                "source_id": int(source.id),
                "refresh": fetch_result,
                "inventory": inventory_result,
                "changed": bool(content_changed),
                "comparison_state": fetch_result.get("comparison_state"),
                "change_kind": fetch_result.get("change_kind"),
                "revalidation_required": bool(fetch_result.get("requires_revalidation") or fetch_result.get("revalidation_required")),
                "source_version_id": fetch_result.get("source_version_id"),
                "previous_version_id": fetch_result.get("previous_version_id"),
                "extract_result": extract_result,
                "diff": diff_result,
                "normalized": normalize_result,
                "validation": validation_result or {"validated_count": 0, "weak_support_count": 0, "ambiguous_count": 0, "conflicting_count": 0, "unsupported_count": 0, "updated_ids": []},
                "requires_revalidation": bool(fetch_result.get("requires_revalidation") or fetch_result.get("revalidation_required")),
            }
        )

    summary = {
        "changed_source_count": len(changed_source_ids),
        "failed_source_count": len(failed_source_ids),
        "changed_rule_count": total_changed_rules,
        "new_rule_count": total_new_rules,
        "missing_rule_count": total_missing_rules,
    }
    return {
        "ok": True,
        "sources_processed": len(sources),
        "total_changed_rules": total_changed_rules,
        "total_new_rules": total_new_rules,
        "total_missing_rules": total_missing_rules,
        "changed_source_count": len(changed_source_ids),
        "changed_source_ids": sorted(set(changed_source_ids)),
        "failed_source_count": len(failed_source_ids),
        "failed_source_ids": sorted(set(failed_source_ids)),
        "source_runs": source_runs,
        "summary": summary,
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
    result = dict(
        _original_run_market_policy_pipeline(
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
    )
    # Always project verified assertions after pipeline backfill so coverage can advance on unchanged sources.
    try:
        project_verified_assertions_to_profile(
            db,
            org_id=org_id,
            state=state,
            county=county,
            city=city,
            pha_name=pha_name,
            notes="post_pipeline_backfill_projection",
        )
        db.commit()
    except Exception:
        db.rollback()
    return result


# === FINAL INCREMENTAL PIPELINE OVERRIDES ===

def _live_projectable_categories_for_market(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> list[str]:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    stmt = select(PolicyAssertion).where(PolicyAssertion.state == st)
    if org_id is None:
        stmt = stmt.where(PolicyAssertion.org_id.is_(None))
    else:
        stmt = stmt.where(or_(PolicyAssertion.org_id == org_id, PolicyAssertion.org_id.is_(None)))

    rows = list(db.scalars(stmt).all())
    categories: set[str] = set()
    for row in rows:
        row_county = _norm_lower(getattr(row, "county", None))
        row_city = _norm_lower(getattr(row, "city", None))
        row_pha = _norm_text(getattr(row, "pha_name", None))
        if row_county not in {None, cnty}:
            continue
        if row_city not in {None, cty}:
            continue
        if pha is not None and row_pha not in {None, pha}:
            continue

        validation_state = str(getattr(row, "validation_state", "") or "").strip().lower()
        trust_state = str(getattr(row, "trust_state", "") or "").strip().lower()
        governance_state = str(getattr(row, "governance_state", "") or "").strip().lower()
        review_status = str(getattr(row, "review_status", "") or "").strip().lower()
        rule_status = str(getattr(row, "rule_status", "") or "").strip().lower()
        coverage_status = str(getattr(row, "coverage_status", "") or "").strip().lower()

        if validation_state != "validated":
            continue
        if trust_state not in {"validated", "trusted"}:
            continue
        if coverage_status in {"conflicting", "unsupported", "superseded", "stale"}:
            continue
        if governance_state not in {"active", "approved", "draft", ""}:
            continue
        if review_status not in {"verified", "accepted", "approved", "projected", "extracted", ""}:
            continue
        if rule_status not in {"active", "candidate", "draft", ""}:
            continue

        category = str(getattr(row, "normalized_category", None) or getattr(row, "rule_category", None) or "").strip().lower()
        if category:
            categories.add(category)

    return sorted(categories)


def _effective_missing_categories(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> list[str]:
    expected = expected_inventory_hints(
        state=_norm_state(state),
        county=_norm_lower(county),
        city=_norm_lower(city),
        pha_name=_norm_text(pha_name),
        include_section8=True,
    )
    expected_categories = [str(x).strip().lower() for x in (expected.get("expected_categories") or []) if str(x).strip()]
    live_categories = set(
        _live_projectable_categories_for_market(
            db,
            org_id=org_id,
            state=state,
            county=county,
            city=city,
            pha_name=pha_name,
        )
    )
    return [cat for cat in expected_categories if cat not in live_categories]


_final_incremental_original_run_source_refresh_batch = _run_source_refresh_batch
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
        fetch_result = refresh_policy_source_and_detect_changes(db, source=source, force=False)
        inventory_result = sync_crawl_result_to_inventory(db, source=source, fetch_result=fetch_result)
        skipped = bool(fetch_result.get("skipped"))
        content_changed = bool(fetch_result.get("change_detected") or fetch_result.get("changed"))
        needs_revalidation = bool(fetch_result.get("requires_revalidation") or fetch_result.get("revalidation_required"))

        extract_result: dict[str, Any] = {"ok": True, "created_count": 0, "assertion_ids": []}
        diff_result: dict[str, Any] = {"ok": True, "changed_count": 0, "new_count": 0, "missing_count": 0}
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
        elif skipped and not needs_revalidation:
            extract_result = {
                "ok": True,
                "created_count": 0,
                "assertion_ids": [],
                "reason": "refresh_skipped_not_due",
            }
            diff_result = {
                "ok": True,
                "changed_count": 0,
                "new_count": 0,
                "missing_count": 0,
                "reason": "refresh_skipped_not_due",
            }
        elif not content_changed:
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
            if needs_revalidation:
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
                "reason": "content_changed",
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

        total_changed_rules += int(diff_result.get("changed_count") or 0)
        total_new_rules += int(diff_result.get("new_count") or 0)
        total_missing_rules += int(diff_result.get("missing_count") or 0)

        source_runs.append(
            {
                "source_id": int(source.id),
                "refresh": fetch_result,
                "inventory": inventory_result,
                "changed": bool(content_changed),
                "comparison_state": fetch_result.get("comparison_state"),
                "change_kind": fetch_result.get("change_kind"),
                "revalidation_required": needs_revalidation,
                "source_version_id": fetch_result.get("source_version_id"),
                "previous_version_id": fetch_result.get("previous_version_id"),
                "extract_result": extract_result,
                "diff": diff_result,
                "normalized": normalize_result,
                "validation": validation_result or {
                    "validated_count": 0,
                    "weak_support_count": 0,
                    "ambiguous_count": 0,
                    "conflicting_count": 0,
                    "unsupported_count": 0,
                    "updated_ids": [],
                },
                "requires_revalidation": needs_revalidation,
            }
        )

    summary = {
        "changed_source_count": len(changed_source_ids),
        "failed_source_count": len(failed_source_ids),
        "changed_rule_count": total_changed_rules,
        "new_rule_count": total_new_rules,
        "missing_rule_count": total_missing_rules,
    }
    return {
        "ok": True,
        "sources_processed": len(sources),
        "total_changed_rules": total_changed_rules,
        "total_new_rules": total_new_rules,
        "total_missing_rules": total_missing_rules,
        "changed_source_count": len(changed_source_ids),
        "changed_source_ids": sorted(set(changed_source_ids)),
        "failed_source_count": len(failed_source_ids),
        "failed_source_ids": sorted(set(failed_source_ids)),
        "source_runs": source_runs,
        "summary": summary,
    }


_final_incremental_original_run_market_policy_pipeline = run_market_policy_pipeline
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
    missing_categories = _effective_missing_categories(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )
    existing_sources = _market_sources_from_catalog(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        focus=focus,
    )

    if missing_categories:
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
    else:
        discovery_result = {
            "ok": True,
            "discovery_triggered": False,
            "reason": "live_projectable_categories_cover_expected_scope",
            "mode": "catalog_selection_only",
            "status": "completed",
            "state": st,
            "county": cnty,
            "city": cty,
            "pha_name": pha,
            "missing_categories": [],
            "candidate_count": 0,
            "created_count": 0,
            "existing_count": len(existing_sources),
            "created_source_ids": [],
            "curated_candidate_count": 0,
            "validated_candidate_count": 0,
            "rejected_candidate_count": 0,
            "guessed_candidate_count": 0,
            "candidates": [],
            "results": [],
        }

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
    try:
        project_verified_assertions_to_profile(
            db,
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
            notes="post_pipeline_incremental_projection",
        )
        db.commit()
    except Exception:
        db.rollback()
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
            "inventory_sync_failed_source_ids": [
                row["source_id"] for row in refresh_batch["source_runs"]
                if not bool((row.get("inventory") or {}).get("ok"))
            ],
            "revalidation_required_source_ids": [
                row["source_id"] for row in refresh_batch["source_runs"]
                if bool((row.get("refresh") or {}).get("revalidation_required") or row.get("requires_revalidation"))
            ],
            "refresh_state_summary": refresh_state_summary,
        },
        "inventory_summary": inventory_summary,
        "lifecycle_result": lifecycle_result,
        "recompute": recompute,
        "review_queue": review_queue,
        "gap_escalations": gap_escalations,
        "governed": governed_assertions_for_scope(
            db,
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
        ),
        "brief": _build_market_compliance_brief(
            db,
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
        ),
    }


# === Cleanup / optimization pipeline overrides ===

def _is_monitor_noop_source_run(row: dict[str, Any]) -> bool:
    refresh = dict(row.get('refresh') or {})
    return (
        str(refresh.get('comparison_state') or row.get('comparison_state') or '').lower() == 'unchanged'
        and str(refresh.get('change_kind') or row.get('change_kind') or '').lower() == 'unchanged'
        and not bool(refresh.get('requires_revalidation') or row.get('revalidation_required'))
        and str((row.get('extract_result') or {}).get('reason') or '').lower() in {'no_content_change', 'monitor_skip_unchanged'}
    )


def _compact_monitor_summary(source_runs: list[dict[str, Any]]) -> dict[str, Any]:
    unchanged_ids = []
    changed_ids = []
    degraded_ids = []
    failed_ids = []
    for row in source_runs:
        sid = int(row.get('source_id') or 0)
        refresh = dict(row.get('refresh') or {})
        if not refresh.get('ok', True):
            failed_ids.append(sid)
            continue
        if str(refresh.get('refresh_state') or '').lower() == 'degraded':
            degraded_ids.append(sid)
        if _is_monitor_noop_source_run(row):
            unchanged_ids.append(sid)
        else:
            changed_ids.append(sid)
    return {
        'mode': 'monitor',
        'all_sources_unchanged': len(changed_ids) == 0 and len(failed_ids) == 0,
        'unchanged_source_ids': unchanged_ids,
        'changed_source_ids': changed_ids,
        'degraded_nonblocking_source_ids': degraded_ids,
        'failed_source_ids': failed_ids,
    }


_run_source_refresh_batch_cleanup_base = _run_source_refresh_batch

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
    monitor_skipped_source_ids: list[int] = []

    for source in sources:
        fetch_result = refresh_policy_source_and_detect_changes(db, source=source)
        inventory_result = sync_crawl_result_to_inventory(db, source=source, fetch_result=fetch_result)
        content_changed = bool(fetch_result.get('change_detected') or fetch_result.get('changed'))
        monitor_skip = bool(fetch_result.get('ok')) and not content_changed and not bool(fetch_result.get('requires_revalidation'))
        if not fetch_result.get('ok'):
            failed_source_ids.append(int(source.id))
            stale_mark_result = mark_assertions_stale_for_source(db, source_id=int(source.id), reason='source_fetch_failed')
            extract_result = {'ok': False, 'created_count': 0, 'assertion_ids': [], 'stale_mark_result': stale_mark_result, 'reason': 'fetch_failed'}
            diff_result = {'ok': False, 'changed_count': 0, 'new_count': 0, 'missing_count': 0, 'reason': 'fetch_failed'}
            normalize_result = None
            validation_result = None
        elif monitor_skip:
            monitor_skipped_source_ids.append(int(source.id))
            extract_result = {'ok': True, 'created_count': 0, 'assertion_ids': [], 'reason': 'monitor_skip_unchanged'}
            diff_result = {'ok': True, 'changed_count': 0, 'new_count': 0, 'missing_count': 0, 'reason': 'monitor_skip_unchanged'}
            normalize_result = None
            validation_result = {'validated_count': 0, 'weak_support_count': 0, 'ambiguous_count': 0, 'conflicting_count': 0, 'unsupported_count': 0, 'updated_ids': [], 'reason': 'monitor_skip_unchanged'}
        else:
            raw_candidates: list[dict[str, Any]] = []
            if content_changed:
                changed_source_ids.append(int(source.id))
                mark_assertions_stale_for_source(db, source_id=int(source.id), reason='source_changed')
                extracted = extract_assertions_for_source(db, source=source, org_id=org_id, org_scope=(org_id is not None))
                raw_candidates = _extract_raw_candidates(extracted)
                extract_result = {'ok': True, 'created_count': len(extracted), 'assertion_ids': [int(a.id) for a in extracted if getattr(a, 'id', None) is not None]}
                diff_result = diff_active_rules_for_source(db, org_id=org_id, source_id=int(source.id), state=state, county=county, city=city, pha_name=pha_name, raw_candidates=raw_candidates)
                normalize_result = normalize_market_assertions(db, org_id=org_id, state=state, county=county, city=city, pha_name=pha_name, reviewer_user_id=reviewer_user_id, source_id=int(source.id), raw_candidates=raw_candidates)
                validation_result = validate_market_assertions(db, org_id=org_id, state=state, county=county, city=city, pha_name=pha_name, source_id=int(source.id))
            else:
                extract_result = {'ok': True, 'created_count': 0, 'assertion_ids': [], 'reason': 'no_content_change'}
                diff_result = {'ok': True, 'changed_count': 0, 'new_count': 0, 'missing_count': 0, 'reason': 'no_content_change'}
                normalize_result = None
                validation_result = {'validated_count': 0, 'weak_support_count': 0, 'ambiguous_count': 0, 'conflicting_count': 0, 'unsupported_count': 0, 'updated_ids': []}
        total_changed_rules += int(diff_result.get('changed_count') or 0)
        total_new_rules += int(diff_result.get('new_count') or 0)
        total_missing_rules += int(diff_result.get('missing_count') or 0)
        source_runs.append({
            'source_id': int(source.id),
            'refresh': fetch_result,
            'inventory': inventory_result,
            'changed': content_changed,
            'comparison_state': fetch_result.get('comparison_state'),
            'change_kind': fetch_result.get('change_kind'),
            'revalidation_required': bool(fetch_result.get('revalidation_required')),
            'source_version_id': fetch_result.get('source_version_id'),
            'previous_version_id': fetch_result.get('previous_version_id'),
            'extract_result': extract_result,
            'diff': diff_result,
            'normalized': normalize_result,
            'validation': validation_result,
            'requires_revalidation': bool(fetch_result.get('requires_revalidation') or fetch_result.get('revalidation_required')),
            'monitor_skipped': monitor_skip,
        })
    return {
        'source_runs': source_runs,
        'summary': {
            'changed_rule_count': total_changed_rules,
            'new_rule_count': total_new_rules,
            'missing_rule_count': total_missing_rules,
            'changed_source_count': len(changed_source_ids),
            'failed_source_count': len(failed_source_ids),
            'changed_source_ids': sorted(set(changed_source_ids)),
            'failed_source_ids': sorted(set(failed_source_ids)),
            'monitor_skipped_source_ids': sorted(set(monitor_skipped_source_ids)),
            'monitor_skipped_source_count': len(set(monitor_skipped_source_ids)),
        },
    }


_pipeline_cleanup_base = run_market_policy_pipeline

def run_market_policy_pipeline(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = 'se_mi_extended',
    reviewer_user_id: int | None = None,
    auto_activate: bool = True,
) -> dict[str, Any]:
    result = dict(_pipeline_cleanup_base(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
        reviewer_user_id=reviewer_user_id,
        auto_activate=auto_activate,
    ) or {})
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)
    inventory_hints = expected_inventory_hints(state=st, county=cnty, city=cty, pha_name=pha, include_section8=True)
    live_inventory = inventory_summary_for_market(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        program_type='section8' if 'section8' in set(inventory_hints.get('expected_categories') or []) else None,
    )
    discovery_result = dict(result.get('discovery_result') or {})
    discovery_result['inventory_summary'] = live_inventory
    result['discovery_result'] = discovery_result

    cleanup_market_stale_assertions(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        reviewer_user_id=reviewer_user_id,
    )
    cleanup_non_projectable_assertions_for_market(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )
    archive_stale_market_sources(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        focus=focus,
    )

    source_runs = list(result.get('source_runs') or [])
    monitor_summary = _compact_monitor_summary(source_runs)
    if (
        not list(result.get('missing_categories') or [])
        and int(result.get('total_changed_rules') or 0) == 0
        and int(result.get('changed_source_count') or 0) == 0
        and int(result.get('failed_source_count') or 0) == 0
        and monitor_summary.get('all_sources_unchanged')
    ):
        result['response_mode'] = 'monitor'
        result['monitor_summary'] = monitor_summary
    else:
        result['response_mode'] = result.get('response_mode') or 'full_pipeline'
    return result


# === FINAL RESPONSE TRUTH PATCH ===

def _pipeline_fresh_coverage_payload(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
    focus: str = "se_mi_extended",
) -> dict[str, Any]:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    payload = dict(
        compute_coverage_status(
            db,
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
            focus=focus,
        )
        or {}
    )

    # Persist, but do not trust the row as the response source of truth.
    try:
        upsert_coverage_status(
            db,
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
            focus=focus,
        )
    except Exception:
        db.rollback()

    return payload


def _pipeline_compact_coverage_view(payload: dict[str, Any]) -> dict[str, Any]:
    payload = dict(payload or {})
    return {
        "id": payload.get("id"),
        "coverage_status": payload.get("coverage_status"),
        "production_readiness": payload.get("production_readiness"),
        "completeness_status": payload.get("completeness_status"),
        "is_stale": bool(payload.get("is_stale", False)),
        "safe_to_rely_on": bool(payload.get("safe_to_rely_on")),
        "safe_for_user_reliance": bool(payload.get("safe_for_user_reliance")),
        "safe_for_projection": bool(payload.get("safe_for_projection")),
        "blocking_categories": list(payload.get("blocking_categories") or []),
        "legal_lockout_categories": list(payload.get("legal_lockout_categories") or []),
        "critical_fetch_failure_categories": list(payload.get("critical_fetch_failure_categories") or []),
        "source_of_truth_strategy": dict(payload.get("source_of_truth_strategy") or {}),
    }


def _pipeline_apply_fresh_coverage_to_result(
    result: dict[str, Any],
    *,
    coverage_payload: dict[str, Any],
    db: Session,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> dict[str, Any]:
    result = dict(result or {})
    coverage_payload = dict(coverage_payload or {})

    # Top-level response should use fresh computed truth, not stale persisted row snapshots.
    result["coverage"] = coverage_payload

    recompute = dict(result.get("recompute") or {})
    recompute["coverage"] = _pipeline_compact_coverage_view(coverage_payload)
    recompute["coverage"]["safe_to_rely_on"] = bool(coverage_payload.get("safe_to_rely_on"))
    recompute["coverage"]["safe_for_user_reliance"] = bool(coverage_payload.get("safe_for_user_reliance"))
    recompute["coverage"]["safe_for_projection"] = bool(coverage_payload.get("safe_for_projection"))
    result["recompute"] = recompute

    # Keep brief aligned with fresh coverage truth.
    try:
        brief = _build_market_compliance_brief(
            db,
            org_id=org_id,
            state=state,
            county=county,
            city=city,
            pha_name=pha_name,
        )
        if isinstance(brief, dict):
            brief = dict(brief)
            brief["coverage"] = coverage_payload
            result["brief"] = brief
    except Exception:
        pass

    # Effective missing categories should come from fresh coverage truth.
    safe = bool(
        coverage_payload.get("safe_to_rely_on")
        or coverage_payload.get("safe_for_user_reliance")
        or (coverage_payload.get("trust_decision") or {}).get("safe_for_user_reliance")
    )
    if safe:
        result["missing_categories"] = []
    else:
        missing = list(coverage_payload.get("missing_categories") or [])
        critical_missing = list(coverage_payload.get("critical_missing_categories") or [])
        authority_unmet = list(coverage_payload.get("authority_unmet_categories") or [])
        weak_support = list(coverage_payload.get("weak_support_categories") or [])
        result["missing_categories"] = sorted(
            set(
                str(x).strip().lower()
                for x in (missing + critical_missing + authority_unmet + weak_support)
                if str(x).strip()
            )
        )

    # Normalize discovery payload after recompute.
    if isinstance(result.get("discovery_result"), dict):
        discovery_result = dict(result["discovery_result"])
        discovery_result["missing_categories"] = list(result.get("missing_categories") or [])
        if not discovery_result["missing_categories"]:
            discovery_result["discovery_triggered"] = False
            discovery_result["status"] = "skipped"
            discovery_result["reason"] = "no_missing_categories_after_recompute"
        result["discovery_result"] = discovery_result

    return result


_pipeline_response_truth_base_run_market_policy_pipeline = run_market_policy_pipeline

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

    result = dict(
        _pipeline_response_truth_base_run_market_policy_pipeline(
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
        or {}
    )
    if not result.get("ok"):
        return result

    coverage_payload = _pipeline_fresh_coverage_payload(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        focus=focus,
    )

    result = _pipeline_apply_fresh_coverage_to_result(
        result,
        coverage_payload=coverage_payload,
        db=db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )
    return result


_pipeline_response_truth_base_refresh_market_policy_pipeline = refresh_market_policy_pipeline

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
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    result = dict(
        _pipeline_response_truth_base_refresh_market_policy_pipeline(
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
        or {}
    )

    pipeline_result = dict(result.get("pipeline_result") or {})
    if pipeline_result.get("ok"):
        coverage_payload = _pipeline_fresh_coverage_payload(
            db,
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
            focus=focus,
        )
        pipeline_result = _pipeline_apply_fresh_coverage_to_result(
            pipeline_result,
            coverage_payload=coverage_payload,
            db=db,
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
        )
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
