from __future__ import annotations

from typing import Optional

from sqlalchemy.orm import Session

from app.policy_models import PolicySource
from app.services.policy_catalog import catalog_for_market
from app.services.policy_coverage_service import compute_coverage_status, upsert_coverage_status
from app.services.policy_extractor_service import extract_assertions_for_source
from app.services.policy_projection_service import (
    build_property_compliance_brief,
    project_verified_assertions_to_profile,
)
from app.services.policy_review_service import (
    auto_verify_market_assertions,
    supersede_replaced_assertions,
)
from app.services.policy_source_service import collect_catalog_for_market


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


def _market_sources_from_catalog(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    focus: str,
) -> list[PolicySource]:
    items = catalog_for_market(
        state=state,
        county=county,
        city=city,
        focus=focus,
    )
    urls = [i.url.strip() for i in items]
    if not urls:
        return []

    q = db.query(PolicySource).filter(PolicySource.url.in_(urls))
    if org_id is None:
        q = q.filter(PolicySource.org_id.is_(None))
    else:
        q = q.filter(PolicySource.org_id == org_id)

    return q.order_by(PolicySource.id.asc()).all()


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
        focus=focus,
    )

    extracted_ids: list[int] = []
    extract_results: list[dict] = []

    for src in source_rows:
        created = extract_assertions_for_source(
            db,
            source=src,
            org_id=reviewer_user_id,
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
            }
        )

    verify_result = auto_verify_market_assertions(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        reviewer_user_id=reviewer_user_id,
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

    profile = project_verified_assertions_to_profile(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        notes=f"Projected from verified assertions for {cty or cnty or st}.",
    )

    coverage = upsert_coverage_status(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        notes=f"Coverage refreshed after market pipeline run for {cty or cnty or st}.",
    )

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

    issues_remaining: list[str] = []
    if not coverage_payload.get("municipal_core_ok"):
        issues_remaining.append("Municipal core is incomplete.")
    if not coverage_payload.get("state_federal_core_ok"):
        issues_remaining.append("State/federal inherited core is incomplete.")
    if coverage_payload.get("stale_warning_count", 0) > 0:
        issues_remaining.append("There are still unresolved stale/recheck assertions.")
    if brief.get("compliance", {}).get("certificate_required_before_occupancy") == "unknown":
        issues_remaining.append("Certificate-before-occupancy status is still unknown.")

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
        },
        "coverage": {
            "id": coverage.id,
            **coverage_payload,
        },
        "brief": brief,
        "issues_remaining": issues_remaining,
    }
