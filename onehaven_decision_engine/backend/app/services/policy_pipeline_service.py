from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import select
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
    diff_active_rules_for_source,
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

    catalog_sources = _market_sources_from_catalog(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        focus=focus,
    )

    source_runs: list[dict[str, Any]] = []
    total_extracted = 0
    total_normalized = 0
    total_changed = 0
    total_unchanged = 0
    total_new = 0
    total_missing = 0

    for source in catalog_sources:
        fetch_result = fetch_policy_source(db, source=source)
        if bool(fetch_result.get("changed")):
            stale_mark = mark_assertions_stale_for_source(
                db,
                source_id=int(source.id),
                reason="source_content_changed",
            )
        else:
            stale_mark = {
                "ok": True,
                "source_id": int(source.id),
                "stale_count": 0,
                "stale_ids": [],
                "reason": "no_source_change",
            }

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

        extracted_count = len(extract_result_rows)
        total_extracted += extracted_count
        total_normalized += int(normalize_result.get("normalized_count") or 0)
        total_changed += int(diff_result.get("changed_count") or 0)
        total_unchanged += int(diff_result.get("unchanged_count") or 0)
        total_new += int(diff_result.get("new_count") or 0)
        total_missing += int(diff_result.get("missing_count") or 0)

        source_runs.append(
            {
                "source_id": int(source.id),
                "source_name": getattr(source, "source_name", None) or getattr(source, "title", None),
                "fetch_result": fetch_result,
                "stale_mark_result": stale_mark,
                "extract_result": {
                    "created_count": extracted_count,
                    "assertion_ids": [int(a.id) for a in extract_result_rows if getattr(a, "id", None) is not None],
                    "assertions": raw_candidates,
                },
                "diff_result": diff_result,
                "normalize_result": normalize_result,
            }
        )

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
        coverage=coverage_payload,
    )
    recompute_payload = recompute_profile_and_coverage(
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
    project_verified_assertions_to_profile(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        notes=f"Pipeline refresh for {state}/{county or '-'} / {city or '-'}",
    )

    completeness = profile_completeness_payload(
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
        "market": {
            "state": st,
            "county": cnty,
            "city": cty,
            "pha_name": pha,
        },
        "sources_processed": len(source_runs),
        "source_runs": source_runs,
        "summary": {
            "extracted_count": total_extracted,
            "normalized_count": total_normalized,
            "changed_count": total_changed,
            "unchanged_count": total_unchanged,
            "new_count": total_new,
            "missing_count": total_missing,
        },
        "auto_verify_result": auto_verify_result,
        "lifecycle_result": lifecycle_result,
        "supersede_result": supersede_result,
        "cleanup_result": cleanup_result,
        "coverage": coverage_payload,
        "recompute": recompute_payload,
        "brief": brief,
        "completeness": completeness,
        "review_queue": review_queue,
        "governance": _governance_summary(
            db,
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
        ),
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

    fetch_result = fetch_policy_source(db, source=source, force=True)
    if bool(fetch_result.get("changed")):
        stale_mark_result = mark_assertions_stale_for_source(
            db,
            source_id=int(source.id),
            reason="source_content_changed",
        )
    else:
        stale_mark_result = {
            "ok": True,
            "source_id": int(source.id),
            "stale_count": 0,
            "stale_ids": [],
            "reason": "no_source_change",
        }

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
        coverage=coverage_payload,
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