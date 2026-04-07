from __future__ import annotations

import json
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.domain.jurisdiction_categories import compute_completeness_score, normalize_categories
from app.services.jurisdiction_completeness_service import (
    collect_covered_categories_for_scope,
    compute_scope_freshness_summary,
)
from app.policy_models import JurisdictionCoverageStatus, PolicyAssertion, PolicySource
from app.services.policy_cleanup_service import ARCHIVE_MARKER
from app.services.policy_catalog_admin_service import merged_catalog_for_market


IMPORTANT_RULE_KEYS = {
    "rental_registration_required",
    "inspection_program_exists",
    "certificate_required_before_occupancy",
    "pha_landlord_packet_required",
    "hap_contract_and_tenancy_addendum_required",
    "federal_hcv_regulations_anchor",
    "federal_nspire_anchor",
    "mi_statute_anchor",
    "mshda_program_anchor",
    "pha_admin_plan_anchor",
    "pha_administrator_changed",
}


REQUIRED_CATEGORY_BASELINE = [
    "registration",
    "inspection",
    "section8",
    "safety",
]


def _dumps(v: Any) -> str:
    try:
        return json.dumps(v, ensure_ascii=False, sort_keys=True)
    except Exception:
        return "{}"


def _norm_state(v: Optional[str]) -> str:
    return (v or "MI").strip().upper()


def _norm_lower(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = v.strip().lower()
    return s or None


def _norm_text(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = v.strip()
    return s or None


def _is_archived_source(src: PolicySource) -> bool:
    return ARCHIVE_MARKER in (src.notes or "").lower()


def _market_sources(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> list[PolicySource]:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    q = db.query(PolicySource).filter(PolicySource.state == st)
    if org_id is None:
        q = q.filter(PolicySource.org_id.is_(None))
    else:
        q = q.filter(
            (PolicySource.org_id == org_id) | (PolicySource.org_id.is_(None))
        )

    rows = q.all()
    out: list[PolicySource] = []
    for row in rows:
        if row.county is not None and row.county != cnty:
            continue
        if row.city is not None and row.city != cty:
            continue
        if row.pha_name is not None and row.pha_name != pha:
            continue
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
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    q = db.query(PolicyAssertion).filter(PolicyAssertion.state == st)
    if org_id is None:
        q = q.filter(PolicyAssertion.org_id.is_(None))
    else:
        q = q.filter(
            (PolicyAssertion.org_id == org_id) | (PolicyAssertion.org_id.is_(None))
        )

    rows = q.all()
    out: list[PolicyAssertion] = []
    for row in rows:
        if row.county is not None and row.county != cnty:
            continue
        if row.city is not None and row.city != cty:
            continue
        if row.pha_name is not None and row.pha_name != pha:
            continue
        out.append(row)
    return out


def _latest_active_source_by_url(
    rows: list[PolicySource],
    *,
    active_urls: set[str],
) -> dict[str, PolicySource]:
    out: dict[str, PolicySource] = {}

    for row in rows:
        url = (row.url or "").strip()
        if not url or url not in active_urls:
            continue
        if _is_archived_source(row):
            continue

        existing = out.get(url)
        if existing is None:
            out[url] = row
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
            out[url] = row

    return out


def _effective_stale_assertions(
    assertions: list[PolicyAssertion],
    *,
    active_source_ids: set[int],
    verified_rule_keys: set[str],
) -> list[PolicyAssertion]:
    out: list[PolicyAssertion] = []

    for a in assertions:
        if a.review_status not in {"stale", "needs_recheck"}:
            continue

        if a.superseded_by_assertion_id is not None:
            continue

        if a.source_id is not None and a.source_id not in active_source_ids:
            continue

        rule_key = (a.rule_key or "").strip()

        if rule_key and rule_key in verified_rule_keys:
            continue

        if rule_key and rule_key not in IMPORTANT_RULE_KEYS:
            continue

        out.append(a)

    return out


def _required_categories_for_market(
    *,
    city: Optional[str],
    county: Optional[str],
    pha_name: Optional[str],
) -> list[str]:
    categories = list(REQUIRED_CATEGORY_BASELINE)
    if city:
        categories.extend(["occupancy", "permits"])
    if pha_name:
        categories.append("section8")
    if county:
        categories.append("registration")
    return normalize_categories(categories)


def compute_coverage_status(
    db: Session,
    *,
    org_id: Optional[int],
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

    active_catalog_items = merged_catalog_for_market(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        focus=focus,
    )
    active_urls = {
        item.url.strip()
        for item in active_catalog_items
        if item.url and item.url.strip()
    }

    all_sources = _market_sources(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )
    latest_sources_by_url = _latest_active_source_by_url(
        all_sources,
        active_urls=active_urls,
    )
    latest_sources = list(latest_sources_by_url.values())
    source_ids_active = {src.id for src in latest_sources}

    assertions = _market_assertions(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )

    verified_assertions = [
        a
        for a in assertions
        if a.review_status == "verified"
        and a.superseded_by_assertion_id is None
    ]
    verified_rule_keys = sorted({a.rule_key for a in verified_assertions if a.rule_key})
    verified_rule_keys_set = set(verified_rule_keys)

    stale_assertions = _effective_stale_assertions(
        assertions,
        active_source_ids=source_ids_active,
        verified_rule_keys=verified_rule_keys_set,
    )

    fetch_failures = 0
    for src in latest_sources:
        status = src.http_status
        if status is None:
            fetch_failures += 1
            continue
        try:
            code = int(status)
        except Exception:
            fetch_failures += 1
            continue
        if code < 200 or code >= 400:
            fetch_failures += 1

    source_kind_counts: dict[str, int] = {}
    for item in active_catalog_items:
        kind = (item.source_kind or "unknown").strip()
        source_kind_counts[kind] = source_kind_counts.get(kind, 0) + 1

    municipal_core_ok = (
        source_kind_counts.get("municipal_registration", 0) > 0
        and source_kind_counts.get("municipal_inspection", 0) > 0
    )

    state_federal_core_ok = (
        (
            "federal_hcv_regulations_anchor" in verified_rule_keys_set
            or source_kind_counts.get("federal_anchor", 0) > 0
        )
        and (
            "federal_nspire_anchor" in verified_rule_keys_set
            or source_kind_counts.get("federal_anchor", 0) > 0
        )
        and (
            "mi_statute_anchor" in verified_rule_keys_set
            or source_kind_counts.get("state_anchor", 0) > 0
        )
    )

    pha_core_ok = (
        source_kind_counts.get("pha_plan", 0) > 0
        or source_kind_counts.get("pha_guidance", 0) > 0
        or source_kind_counts.get("state_hcv_anchor", 0) > 0
        or "mshda_program_anchor" in verified_rule_keys_set
        or "pha_admin_plan_anchor" in verified_rule_keys_set
    )

    has_sources = len(latest_sources) > 0
    has_extracted = len(assertions) > 0
    verified_rule_count = len(verified_rule_keys)

    if municipal_core_ok and state_federal_core_ok and verified_rule_count >= 5 and fetch_failures == 0:
        production_readiness = "ready"
    elif has_sources or has_extracted:
        production_readiness = "partial"
    else:
        production_readiness = "needs_review"

    if verified_rule_count >= 8 and fetch_failures == 0 and len(stale_assertions) == 0:
        confidence_label = "high"
    elif verified_rule_count >= 4:
        confidence_label = "medium"
    else:
        confidence_label = "low"

    if verified_rule_count == 0 and not has_sources:
        coverage_status = "no_sources"
    elif has_sources and not has_extracted:
        coverage_status = "sources_collected"
    elif has_extracted and verified_rule_count == 0:
        coverage_status = "assertions_extracted"
    elif verified_rule_count > 0:
        coverage_status = "verified_extended"
    else:
        coverage_status = "needs_review"

    required_categories = _required_categories_for_market(
        city=cty,
        county=cnty,
        pha_name=pha,
    )
    covered_categories = collect_covered_categories_for_scope(
        db,
        state=st,
        county=cnty,
        city=cty,
    )
    completeness = compute_completeness_score(
        required_categories=required_categories,
        covered_categories=covered_categories,
    )
    freshness = compute_scope_freshness_summary(
        db,
        state=st,
        county=cnty,
        city=cty,
    )

    return {
        "state": st,
        "county": cnty,
        "city": cty,
        "pha_name": pha,
        "coverage_status": coverage_status,
        "production_readiness": production_readiness,
        "confidence_label": confidence_label,
        "verified_rule_count": verified_rule_count,
        "source_count": len(latest_sources),
        "fetch_failure_count": fetch_failures,
        "stale_warning_count": len(stale_assertions),
        "has_sources": has_sources,
        "has_extracted": has_extracted,
        "verified_rule_keys": verified_rule_keys,
        "municipal_core_ok": municipal_core_ok,
        "state_federal_core_ok": state_federal_core_ok,
        "pha_core_ok": pha_core_ok,
        "required_categories": completeness.required_categories,
        "covered_categories": completeness.covered_categories,
        "missing_categories": completeness.missing_categories,
        "completeness_score": completeness.completeness_score,
        "completeness_status": completeness.completeness_status,
        "is_stale": freshness.is_stale,
        "stale_reason": freshness.stale_reason,
        "freshest_source_at": freshness.freshest_source_at.isoformat() if freshness.freshest_source_at else None,
        "oldest_source_at": freshness.oldest_source_at.isoformat() if freshness.oldest_source_at else None,
        "authoritative_source_count": freshness.authoritative_source_count,
        "source_freshness_json": freshness.freshness_payload,
    }


def upsert_coverage_status(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    notes: Optional[str] = None,
    focus: str = "se_mi_extended",
) -> JurisdictionCoverageStatus:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    payload = compute_coverage_status(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        focus=focus,
    )

    q = db.query(JurisdictionCoverageStatus).filter(
        JurisdictionCoverageStatus.state == st,
        JurisdictionCoverageStatus.county == cnty,
        JurisdictionCoverageStatus.city == cty,
        JurisdictionCoverageStatus.pha_name == pha,
    )

    if org_id is None:
        q = q.filter(JurisdictionCoverageStatus.org_id.is_(None))
    else:
        q = q.filter(
            (JurisdictionCoverageStatus.org_id == org_id)
            | (JurisdictionCoverageStatus.org_id.is_(None))
        )

    row = q.order_by(JurisdictionCoverageStatus.id.desc()).first()

    if row is None:
        row = JurisdictionCoverageStatus(
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
        )
        db.add(row)

    row.coverage_status = payload["coverage_status"]
    row.production_readiness = payload["production_readiness"]
    row.verified_rule_count = payload["verified_rule_count"]
    row.source_count = payload["source_count"]
    row.fetch_failure_count = payload["fetch_failure_count"]
    row.stale_warning_count = payload["stale_warning_count"]
    row.last_source_refresh_at = None if not payload["freshest_source_at"] else payload["freshest_source_at"]
    row.last_reviewed_at = datetime.utcnow() if payload["verified_rule_count"] > 0 else row.last_reviewed_at

    row.required_categories_json = _dumps(payload["required_categories"])
    row.covered_categories_json = _dumps(payload["covered_categories"])
    row.missing_categories_json = _dumps(payload["missing_categories"])
    row.completeness_score = payload["completeness_score"]
    row.completeness_status = payload["completeness_status"]
    row.category_norm_version = "v1"
    row.last_verified_at = datetime.utcnow() if payload["completeness_status"] == "complete" and not payload["is_stale"] else row.last_verified_at
    row.is_stale = bool(payload["is_stale"])
    row.stale_reason = payload["stale_reason"]
    row.freshest_source_at = None
    row.oldest_source_at = None
    row.source_freshness_json = _dumps(payload["source_freshness_json"])
    row.notes = notes

    # Back-compat assignments for repos that still have these columns.
    if hasattr(row, "confidence_label"):
        setattr(row, "confidence_label", payload["confidence_label"])
    if hasattr(row, "has_sources"):
        setattr(row, "has_sources", payload["has_sources"])
    if hasattr(row, "has_extracted"):
        setattr(row, "has_extracted", payload["has_extracted"])
    if hasattr(row, "verified_rule_keys"):
        setattr(row, "verified_rule_keys", payload["verified_rule_keys"])
    if hasattr(row, "municipal_core_ok"):
        setattr(row, "municipal_core_ok", payload["municipal_core_ok"])
    if hasattr(row, "state_federal_core_ok"):
        setattr(row, "state_federal_core_ok", payload["state_federal_core_ok"])
    if hasattr(row, "pha_core_ok"):
        setattr(row, "pha_core_ok", payload["pha_core_ok"])
    if hasattr(row, "authoritative_source_count"):
        setattr(row, "authoritative_source_count", payload["authoritative_source_count"])

    db.commit()
    db.refresh(row)
    return row


# ---- Chunk 5 coverage enrichments ----
_base_compute_coverage_status = compute_coverage_status
_base_upsert_coverage_status = upsert_coverage_status


def _coverage_confidence_label(score: float) -> str:
    return 'high' if score >= 0.75 else ('medium' if score >= 0.45 else 'low')


def compute_coverage_status(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = 'se_mi_extended',
) -> dict:
    payload = _base_compute_coverage_status(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
    )

    verified_rule_keys = list(payload.get('verified_rule_keys') or [])
    required_categories = list(payload.get('required_categories') or [])
    missing_categories = list(payload.get('missing_categories') or [])
    source_count = int(payload.get('source_count') or 0)
    authoritative_count = int(payload.get('authoritative_source_count') or 0)
    verified_count = int(payload.get('verified_rule_count') or len(verified_rule_keys))
    stale_warning_count = int(payload.get('stale_warning_count') or 0)
    fetch_failure_count = int(payload.get('fetch_failure_count') or 0)
    completeness_score = float(payload.get('completeness_score') or 0.0)

    confidence_score = 0.0
    if source_count:
        confidence_score += min(0.25, 0.05 * source_count)
    if authoritative_count and source_count:
        confidence_score += min(0.25, 0.25 * (authoritative_count / max(1, source_count)))
    if verified_count:
        confidence_score += min(0.25, 0.03 * verified_count)
    confidence_score += min(0.25, completeness_score * 0.25)
    confidence_score -= min(0.20, stale_warning_count * 0.05)
    confidence_score -= min(0.20, fetch_failure_count * 0.05)
    confidence_score = max(0.0, min(1.0, round(confidence_score, 3)))

    important_missing = []
    if 'rental_registration_required' not in verified_rule_keys:
        important_missing.append('rental_registration_required')
    if 'inspection_program_exists' not in verified_rule_keys:
        important_missing.append('inspection_program_exists')
    if city and 'certificate_required_before_occupancy' not in verified_rule_keys:
        important_missing.append('certificate_required_before_occupancy')
    if pha_name:
        if 'pha_landlord_packet_required' not in verified_rule_keys:
            important_missing.append('pha_landlord_packet_required')
        if 'hap_contract_and_tenancy_addendum_required' not in verified_rule_keys:
            important_missing.append('hap_contract_and_tenancy_addendum_required')

    payload['coverage_confidence'] = _coverage_confidence_label(confidence_score)
    payload['confidence_score'] = confidence_score
    payload['authoritative_ratio'] = round(authoritative_count / max(1, source_count), 3) if source_count else 0.0
    payload['verified_ratio'] = round(verified_count / max(1, max(len(verified_rule_keys), len(required_categories), 1)), 3)
    payload['missing_rule_keys'] = important_missing
    payload['missing_local_rule_areas'] = missing_categories
    payload['stale_warning'] = bool(payload.get('is_stale')) or stale_warning_count > 0
    payload['resolution_order'] = [
        'michigan_statewide_baseline',
        'county_rules',
        'city_rules',
        'housing_authority_overlays',
        'org_overrides',
    ]
    return payload


def upsert_coverage_status(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    focus: str = 'se_mi_extended',
    notes: Optional[str] = None,
):
    row = _base_upsert_coverage_status(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
        notes=notes,
    )
    payload = compute_coverage_status(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        focus=focus,
    )
    if hasattr(row, 'notes'):
        suffix = f" | coverage_confidence={payload.get('coverage_confidence')} score={payload.get('confidence_score')}"
        row.notes = ((row.notes or '') + suffix).strip()
        db.commit()
        db.refresh(row)
    return row
