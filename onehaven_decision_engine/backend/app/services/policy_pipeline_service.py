from __future__ import annotations

from typing import Optional

from sqlalchemy.orm import Session

from app.policy_models import PolicyAssertion, PolicySource
from app.services.jurisdiction_completeness_service import (
    profile_completeness_payload,
    recompute_profile_and_coverage,
)
from app.services.policy_cleanup_service import (
    ARCHIVE_MARKER,
    archive_stale_market_sources,
)
from app.services.policy_coverage_service import (
    compute_coverage_status,
    upsert_coverage_status,
)
from app.services.policy_extractor_service import extract_assertions_for_source
from app.services.policy_projection_service import (
    build_property_compliance_brief,
    project_verified_assertions_to_profile,
)
from app.services.policy_review_service import (
    auto_verify_market_assertions,
    cleanup_market_stale_assertions,
    supersede_replaced_assertions,
)


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
    from app.services.policy_catalog_admin_service import merged_catalog_for_market

    items = merged_catalog_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
    )
    urls = [i.url.strip() for i in items if i.url and i.url.strip()]
    if not urls:
        return []

    q = db.query(PolicySource).filter(PolicySource.url.in_(urls))
    if org_id is None:
        q = q.filter(PolicySource.org_id.is_(None))
    else:
        q = q.filter(
            (PolicySource.org_id == org_id) | (PolicySource.org_id.is_(None))
        )

    rows = q.order_by(PolicySource.id.asc()).all()

    by_url: dict[str, PolicySource] = {}
    for row in rows:
        if _is_archived_source(row):
            continue
        url = (row.url or "").strip()
        if not url:
            continue

        existing = by_url.get(url)
        if existing is None:
            by_url[url] = row
            continue

        existing_sort = (
            existing.retrieved_at.isoformat() if existing.retrieved_at else "",
            existing.id or 0,
        )
        row_sort = (
            row.retrieved_at.isoformat() if row.retrieved_at else "",
            row.id or 0,
        )
        if row_sort > existing_sort:
            by_url[url] = row

    ordered = [by_url[url] for url in urls if url in by_url]
    return ordered


def _market_assertions(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> list[PolicyAssertion]:
    q = db.query(PolicyAssertion).filter(PolicyAssertion.state == state)
    if org_id is None:
        q = q.filter(PolicyAssertion.org_id.is_(None))
    else:
        q = q.filter(
            (PolicyAssertion.org_id == org_id) | (PolicyAssertion.org_id.is_(None))
        )

    rows = q.all()
    out: list[PolicyAssertion] = []
    for row in rows:
        if row.county is not None and row.county != county:
            continue
        if row.city is not None and row.city != city:
            continue
        if row.pha_name is not None and row.pha_name != pha_name:
            continue
        out.append(row)
    return out


def _issues_remaining(coverage_payload: dict, brief: dict) -> list[str]:
    issues: list[str] = []

    if not coverage_payload.get("municipal_core_ok"):
        issues.append("Municipal core is incomplete.")
    if not coverage_payload.get("state_federal_core_ok"):
        issues.append("State/federal inherited core is incomplete.")
    if coverage_payload.get("stale_warning_count", 0) > 0:
        issues.append("There are still unresolved stale/recheck assertions.")
    if brief.get("compliance", {}).get("certificate_required_before_occupancy") == "unknown":
        issues.append("Certificate-before-occupancy status is still unknown.")
    if coverage_payload.get("completeness_status") != "complete":
        issues.append("Jurisdiction category coverage is incomplete.")
    if coverage_payload.get("is_stale"):
        issues.append("Jurisdiction source freshness is stale.")

    return issues


def _unresolved_rule_gaps(coverage_payload: dict, brief: dict) -> list[str]:
    gaps: list[str] = []
    verified_keys = set(coverage_payload.get("verified_rule_keys") or [])

    if "rental_registration_required" not in verified_keys:
        gaps.append("rental_registration_required")
    if "inspection_program_exists" not in verified_keys:
        gaps.append("inspection_program_exists")
    if "federal_hcv_regulations_anchor" not in verified_keys:
        gaps.append("federal_hcv_regulations_anchor")
    if "federal_nspire_anchor" not in verified_keys:
        gaps.append("federal_nspire_anchor")
    if "mi_statute_anchor" not in verified_keys:
        gaps.append("mi_statute_anchor")

    if brief.get("compliance", {}).get("certificate_required_before_occupancy") == "unknown":
        gaps.append("certificate_required_before_occupancy")

    for missing_category in coverage_payload.get("missing_categories") or []:
        gaps.append(f"category:{missing_category}")

    seen: set[str] = set()
    out: list[str] = []
    for gap in gaps:
        if gap in seen:
            continue
        seen.add(gap)
        out.append(gap)
    return out


def _stabilize_review_state(
    db: Session,
    *,
    org_id: Optional[int],
    reviewer_user_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
    archive_extracted_duplicates: bool,
) -> dict:
    verify_1 = auto_verify_market_assertions(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        reviewer_user_id=reviewer_user_id,
    )
    supersede_1 = supersede_replaced_assertions(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        reviewer_user_id=reviewer_user_id,
    )
    cleanup = cleanup_market_stale_assertions(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        reviewer_user_id=reviewer_user_id,
        archive_extracted_duplicates=archive_extracted_duplicates,
    )
    verify_2 = auto_verify_market_assertions(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        reviewer_user_id=reviewer_user_id,
    )
    supersede_2 = supersede_replaced_assertions(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        reviewer_user_id=reviewer_user_id,
    )

    return {
        "verify_result": {
            "updated_count": int(verify_1.get("updated_count", 0))
            + int(verify_2.get("updated_count", 0)),
            "updated_ids": (verify_1.get("updated_ids", []) or [])
            + (verify_2.get("updated_ids", []) or []),
        },
        "supersede_result": {
            "superseded_count": int(supersede_1.get("superseded_count", 0))
            + int(supersede_2.get("superseded_count", 0)),
            "superseded_ids": (supersede_1.get("superseded_ids", []) or [])
            + (supersede_2.get("superseded_ids", []) or []),
        },
        "cleanup_result": cleanup,
    }


def run_market_pipeline(
    db: Session,
    *,
    org_id: Optional[int],
    reviewer_user_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = "se_mi_extended",
) -> dict:
    from app.services.policy_source_service import collect_catalog_for_market

    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    collect_results = collect_catalog_for_market(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        focus=focus,
    )

    source_rows = _market_sources_from_catalog(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        focus=focus,
    )

    extracted_ids: list[int] = []
    extract_results: list[dict] = []

    for src in source_rows:
        created = extract_assertions_for_source(
            db,
            source=src,
            org_id=org_id,
            org_scope=(org_id is not None),
        )
        created_ids = [a.id for a in created]
        extracted_ids.extend(created_ids)
        extract_results.append(
            {
                "source_id": src.id,
                "url": src.url,
                "created": len(created_ids),
                "assertion_ids": created_ids,
                "normalized_categories": sorted(
                    {
                        a.normalized_category
                        for a in created
                        if getattr(a, "normalized_category", None)
                    }
                ),
            }
        )

    review_bundle = _stabilize_review_state(
        db,
        org_id=org_id,
        reviewer_user_id=reviewer_user_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        archive_extracted_duplicates=True,
    )
    verify_result = review_bundle["verify_result"]
    supersede_result = review_bundle["supersede_result"]

    profile = project_verified_assertions_to_profile(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        notes=f"Projected from verified assertions for {cty or cnty or st}.",
    )
    profile, coverage = recompute_profile_and_coverage(db, profile, commit=True)

    coverage_payload = compute_coverage_status(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )

    brief = build_property_compliance_brief(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )

    issues_remaining = _issues_remaining(coverage_payload, brief)

    return {
        "ok": True,
        "market": {
            "state": st,
            "county": cnty,
            "city": cty,
            "pha_name": pha,
        },
        "collect": {
            "count": len(collect_results),
            "ok_count": sum(1 for r in collect_results if r.fetch_ok),
            "failed_count": sum(1 for r in collect_results if not r.fetch_ok),
            "source_ids": [r.source.id for r in collect_results],
        },
        "extract": {
            "source_count": len(source_rows),
            "assertion_count_created": len(extracted_ids),
            "results": extract_results,
        },
        "review": {
            **verify_result,
            **supersede_result,
        },
        "profile": {
            "id": profile.id,
            "org_id": profile.org_id,
            "state": profile.state,
            "county": profile.county,
            "city": profile.city,
            "pha_name": profile.pha_name,
            "friction_multiplier": profile.friction_multiplier,
            "notes": profile.notes,
            "completeness": profile_completeness_payload(db, profile),
        },
        "coverage": {
            "id": coverage.id,
            **coverage_payload,
        },
        "brief": brief,
        "issues_remaining": issues_remaining,
    }


def cleanup_market(
    db: Session,
    *,
    org_id: Optional[int],
    reviewer_user_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    archive_extracted_duplicates: bool = True,
    focus: str = "se_mi_extended",
) -> dict:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    review_bundle = _stabilize_review_state(
        db,
        org_id=org_id,
        reviewer_user_id=reviewer_user_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        archive_extracted_duplicates=archive_extracted_duplicates,
    )
    cleanup = review_bundle["cleanup_result"]

    source_cleanup = archive_stale_market_sources(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        focus=focus,
    )

    coverage = upsert_coverage_status(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        notes=f"Coverage refreshed after stale cleanup for {cty or cnty or st}.",
    )

    coverage_payload = compute_coverage_status(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )

    return {
        "ok": True,
        "market": {
            "state": st,
            "county": cnty,
            "city": cty,
            "pha_name": pha,
        },
        "cleanup": cleanup,
        "source_cleanup": source_cleanup,
        "coverage": {
            "id": coverage.id,
            **coverage_payload,
        },
    }


def repair_market(
    db: Session,
    *,
    org_id: Optional[int],
    reviewer_user_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = "se_mi_extended",
    archive_extracted_duplicates: bool = True,
) -> dict:
    from app.services.policy_source_service import collect_catalog_for_market

    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    collect_results = collect_catalog_for_market(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        focus=focus,
    )

    source_rows = _market_sources_from_catalog(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        focus=focus,
    )

    extracted_ids: list[int] = []
    extract_results: list[dict] = []

    for src in source_rows:
        created = extract_assertions_for_source(
            db,
            source=src,
            org_id=org_id,
            org_scope=(org_id is not None),
        )
        created_ids = [a.id for a in created]
        extracted_ids.extend(created_ids)
        extract_results.append(
            {
                "source_id": src.id,
                "url": src.url,
                "created": len(created_ids),
                "assertion_ids": created_ids,
                "normalized_categories": sorted(
                    {
                        a.normalized_category
                        for a in created
                        if getattr(a, "normalized_category", None)
                    }
                ),
            }
        )

    review_bundle = _stabilize_review_state(
        db,
        org_id=org_id,
        reviewer_user_id=reviewer_user_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        archive_extracted_duplicates=archive_extracted_duplicates,
    )
    verify_result = review_bundle["verify_result"]
    supersede_result = review_bundle["supersede_result"]
    cleanup_result = review_bundle["cleanup_result"]

    source_cleanup = archive_stale_market_sources(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        focus=focus,
    )

    profile = project_verified_assertions_to_profile(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        notes=f"Repaired and projected from verified assertions for {cty or cnty or st}.",
    )
    profile, coverage = recompute_profile_and_coverage(db, profile, commit=True)

    coverage_payload = compute_coverage_status(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )

    brief = build_property_compliance_brief(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )

    issues_remaining = _issues_remaining(coverage_payload, brief)
    unresolved_rule_gaps = _unresolved_rule_gaps(coverage_payload, brief)

    return {
        "ok": True,
        "market": {
            "state": st,
            "county": cnty,
            "city": cty,
            "pha_name": pha,
        },
        "sources_refreshed": {
            "count": len(collect_results),
            "ok_count": sum(1 for r in collect_results if r.fetch_ok),
            "failed_count": sum(1 for r in collect_results if not r.fetch_ok),
            "source_ids": [r.source.id for r in collect_results],
        },
        "assertions_created": {
            "count": len(extracted_ids),
            "results": extract_results,
        },
        "assertions_verified": {
            "updated_count": int(verify_result.get("updated_count", 0)),
            "updated_ids": verify_result.get("updated_ids", []),
        },
        "duplicates_superseded": {
            "superseded_count": int(supersede_result.get("superseded_count", 0)),
            "superseded_ids": supersede_result.get("superseded_ids", []),
        },
        "cleanup": cleanup_result,
        "source_cleanup": source_cleanup,
        "profile": {
            "id": profile.id,
            "org_id": profile.org_id,
            "state": profile.state,
            "county": profile.county,
            "city": profile.city,
            "pha_name": profile.pha_name,
            "friction_multiplier": profile.friction_multiplier,
            "notes": profile.notes,
            "completeness": profile_completeness_payload(db, profile),
        },
        "coverage": {
            "id": coverage.id,
            **coverage_payload,
        },
        "brief": brief,
        "stale_items_remaining": int(cleanup_result.get("stale_items_remaining", 0)),
        "unresolved_rule_gaps": unresolved_rule_gaps,
        "issues_remaining": issues_remaining,
    }


# ---- Chunk 5 pipeline enrichments ----
_base_run_market_pipeline = run_market_pipeline
_base_repair_market = repair_market


def run_market_pipeline(
    db: Session,
    *,
    org_id: Optional[int],
    reviewer_user_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = 'se_mi_extended',
) -> dict:
    result = _base_run_market_pipeline(
        db,
        org_id=org_id,
        reviewer_user_id=reviewer_user_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
    )
    coverage = dict(result.get('coverage') or {})
    result['jurisdiction_rule_engine'] = {
        'resolution_order': [
            'michigan_statewide_baseline',
            'county_rules',
            'city_rules',
            'housing_authority_overlays',
            'org_overrides',
        ],
        'coverage_confidence': coverage.get('coverage_confidence'),
        'confidence_score': coverage.get('confidence_score'),
        'missing_local_rule_areas': coverage.get('missing_local_rule_areas') or coverage.get('missing_categories') or [],
        'missing_rule_keys': coverage.get('missing_rule_keys') or [],
        'stale_warning': coverage.get('stale_warning', coverage.get('is_stale')),
    }
    return result


def repair_market(
    db: Session,
    *,
    org_id: Optional[int],
    reviewer_user_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = 'se_mi_extended',
    archive_extracted_duplicates: bool = True,
) -> dict:
    result = _base_repair_market(
        db,
        org_id=org_id,
        reviewer_user_id=reviewer_user_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
        archive_extracted_duplicates=archive_extracted_duplicates,
    )
    coverage = dict(result.get('coverage') or {})
    result['jurisdiction_rule_engine'] = {
        'resolution_order': [
            'michigan_statewide_baseline',
            'county_rules',
            'city_rules',
            'housing_authority_overlays',
            'org_overrides',
        ],
        'coverage_confidence': coverage.get('coverage_confidence'),
        'confidence_score': coverage.get('confidence_score'),
        'missing_local_rule_areas': coverage.get('missing_local_rule_areas') or coverage.get('missing_categories') or [],
        'missing_rule_keys': coverage.get('missing_rule_keys') or [],
        'stale_warning': coverage.get('stale_warning', coverage.get('is_stale')),
    }
    return result
