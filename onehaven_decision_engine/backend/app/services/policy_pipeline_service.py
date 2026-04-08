# backend/app/services/policy_pipeline_service.py
from __future__ import annotations

from typing import Any, Optional

from sqlalchemy.orm import Session

from app.policy_models import PolicyAssertion, PolicySource
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
    normalize_market_assertions,
    supersede_replaced_assertions,
)
from app.services.policy_source_service import collect_catalog_for_market, fetch_policy_source


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
    source_rows = collect_catalog_for_market(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
    )
    out: list[PolicySource] = []
    seen: set[int] = set()
    for row in source_rows:
        if _is_archived_source(row):
            continue
        if int(row.id) in seen:
            continue
        seen.add(int(row.id))
        out.append(row)
    return out


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
        q = q.filter((PolicyAssertion.org_id == org_id) | (PolicyAssertion.org_id.is_(None)))

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


def _issues_remaining(coverage_payload: dict[str, Any], brief: dict[str, Any]) -> list[str]:
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


def _unresolved_rule_gaps(coverage_payload: dict[str, Any], brief: dict[str, Any]) -> list[str]:
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

    out: list[str] = []
    seen: set[str] = set()
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
) -> dict[str, Any]:
    normalized = normalize_market_assertions(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )
    verify_1 = auto_verify_market_assertions(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        reviewer_user_id=reviewer_user_id,
    )
    lifecycle_1 = apply_governance_lifecycle(
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
    lifecycle_2 = apply_governance_lifecycle(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        reviewer_user_id=reviewer_user_id,
    )
    supersede_legacy = supersede_replaced_assertions(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        reviewer_user_id=reviewer_user_id,
    )

    return {
        "normalized_result": normalized,
        "verify_result": {
            "updated_count": int(verify_1.get("updated_count", 0)) + int(verify_2.get("updated_count", 0)),
            "updated_ids": (verify_1.get("updated_ids", []) or []) + (verify_2.get("updated_ids", []) or []),
        },
        "governance_result": {
            "activated_count": int(lifecycle_1.get("activated_count", 0)) + int(lifecycle_2.get("activated_count", 0)),
            "activated_ids": (lifecycle_1.get("activated_ids", []) or []) + (lifecycle_2.get("activated_ids", []) or []),
            "approved_count": int(lifecycle_1.get("approved_count", 0)) + int(lifecycle_2.get("approved_count", 0)),
            "approved_ids": (lifecycle_1.get("approved_ids", []) or []) + (lifecycle_2.get("approved_ids", []) or []),
            "replaced_count": int(lifecycle_1.get("replaced_count", 0)) + int(lifecycle_2.get("replaced_count", 0)),
            "replaced_ids": (lifecycle_1.get("replaced_ids", []) or []) + (lifecycle_2.get("replaced_ids", []) or []),
        },
        "supersede_result": supersede_legacy,
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
    force_refresh: bool = False,
    publish_active: bool = True,
) -> dict[str, Any]:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    source_rows = _market_sources_from_catalog(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        focus=focus,
    )

    refresh_results: list[dict[str, Any]] = []
    extracted_ids: list[int] = []
    extract_results: list[dict[str, Any]] = []

    for src in source_rows:
        refresh_result = fetch_policy_source(
            db,
            source=src,
            force=force_refresh,
        )
        refresh_results.append(
            {
                "source_id": int(src.id),
                "url": src.url,
                **refresh_result,
            }
        )

        if not refresh_result.get("ok"):
            stale_update = mark_assertions_stale_for_source(db, source_id=int(src.id))
            extract_results.append(
                {
                    "source_id": int(src.id),
                    "url": src.url,
                    "refresh_ok": False,
                    "changed": False,
                    "created": 0,
                    "assertion_ids": [],
                    "stale_update": stale_update,
                }
            )
            continue

        if bool(refresh_result.get("changed")) or force_refresh:
            created = extract_assertions_for_source(
                db,
                source=src,
                org_id=org_id,
                org_scope=(org_id is not None),
            )
            created_ids = [int(a.id) for a in created]
            extracted_ids.extend(created_ids)
            extract_results.append(
                {
                    "source_id": int(src.id),
                    "url": src.url,
                    "refresh_ok": True,
                    "changed": bool(refresh_result.get("changed")),
                    "created": len(created_ids),
                    "assertion_ids": created_ids,
                    "normalized_categories": sorted(
                        {
                            getattr(a, "normalized_category", None) or getattr(a, "rule_category", None)
                            for a in created
                            if getattr(a, "normalized_category", None) or getattr(a, "rule_category", None)
                        }
                    ),
                }
            )
        else:
            extract_results.append(
                {
                    "source_id": int(src.id),
                    "url": src.url,
                    "refresh_ok": True,
                    "changed": False,
                    "created": 0,
                    "assertion_ids": [],
                    "normalized_categories": [],
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

    if not publish_active:
        review_bundle["governance_result"] = {
            "activated_count": 0,
            "activated_ids": [],
            "approved_count": review_bundle.get("verify_result", {}).get("updated_count", 0),
            "approved_ids": review_bundle.get("verify_result", {}).get("updated_ids", []),
            "replaced_count": 0,
            "replaced_ids": [],
        }

    archived_source_result = archive_stale_market_sources(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )

    profile = project_verified_assertions_to_profile(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )
    coverage_row = upsert_coverage_status(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        focus=focus,
        notes="policy pipeline recompute",
    )
    coverage_payload = compute_coverage_status(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        focus=focus,
    )

    recompute_payload = recompute_profile_and_coverage(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )
    completeness_payload = profile_completeness_payload(
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
        property_id=None,
        property_row=None,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )

    assertions = _market_assertions(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )

    draft_count = sum(1 for a in assertions if (a.governance_state or "").lower() == "draft")
    approved_count = sum(1 for a in assertions if (a.governance_state or "").lower() == "approved")
    active_count = sum(1 for a in assertions if (a.governance_state or "").lower() == "active")
    replaced_count = sum(1 for a in assertions if (a.governance_state or "").lower() == "replaced")

    review_queue = build_review_queue_payload(
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        assertions=assertions,
    )

    result = {
        "ok": True,
        "market": {
            "state": st,
            "county": cnty,
            "city": cty,
            "pha_name": pha,
        },
        "source_count": len(source_rows),
        "refresh_results": refresh_results,
        "extract_results": extract_results,
        "extracted_count": len(extracted_ids),
        "extracted_ids": extracted_ids,
        "review_bundle": review_bundle,
        "review_queue": review_queue,
        "archived_source_result": archived_source_result,
        "profile_id": getattr(profile, "id", None),
        "coverage_status_id": getattr(coverage_row, "id", None),
        "coverage": coverage_payload,
        "recompute_payload": recompute_payload,
        "completeness_payload": completeness_payload,
        "brief": brief,
        "governance_counts": {
            "draft": draft_count,
            "approved": approved_count,
            "active": active_count,
            "replaced": replaced_count,
        },
        "issues_remaining": _issues_remaining(coverage_payload, brief),
        "unresolved_rule_gaps": _unresolved_rule_gaps(coverage_payload, brief),
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
    focus: str = "se_mi_extended",
    archive_extracted_duplicates: bool = True,
    force_refresh: bool = False,
) -> dict[str, Any]:
    return run_market_pipeline(
        db,
        org_id=org_id,
        reviewer_user_id=reviewer_user_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
        force_refresh=force_refresh,
        publish_active=True,
    )