from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from app.policy_models import JurisdictionCoverageStatus, PolicyAssertion, PolicySource

CORE_MUNICIPAL_RULES = {
    "rental_registration_required",
    "inspection_program_exists",
    "certificate_required_before_occupancy",
}

CORE_STATE_FEDERAL_RULES = {
    "federal_hcv_regulations_anchor",
    "federal_nspire_anchor",
    "mi_statute_anchor",
}

CORE_PHA_RULES = {
    "pha_admin_plan_anchor",
    "pha_administrator_changed",
    "hap_contract_and_tenancy_addendum_required",
    "pha_landlord_packet_required",
}


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


def _in_scope_assertion(
    row: PolicyAssertion,
    *,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> bool:
    if row.county is not None and row.county != county:
        return False
    if row.city is not None and row.city != city:
        return False
    if row.pha_name is not None and row.pha_name != pha_name:
        return False
    return True


def _in_scope_source(
    row: PolicySource,
    *,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> bool:
    if row.county is not None and row.county != county:
        return False
    if row.city is not None and row.city != city:
        return False
    if row.pha_name is not None and row.pha_name != pha_name:
        return False
    return True


def _query_market_assertions(
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
    return [
        row
        for row in rows
        if _in_scope_assertion(
            row,
            county=county,
            city=city,
            pha_name=pha_name,
        )
    ]


def _query_market_sources(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> list[PolicySource]:
    q = db.query(PolicySource).filter(PolicySource.state == state)
    if org_id is None:
        q = q.filter(PolicySource.org_id.is_(None))
    else:
        q = q.filter(
            (PolicySource.org_id == org_id) | (PolicySource.org_id.is_(None))
        )

    rows = q.all()
    return [
        row
        for row in rows
        if _in_scope_source(
            row,
            county=county,
            city=city,
            pha_name=pha_name,
        )
    ]


def compute_coverage_status(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
) -> dict:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    sources = _query_market_sources(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )
    assertions = _query_market_assertions(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )

    verified = [a for a in assertions if a.review_status == "verified"]
    extracted = [a for a in assertions if a.review_status == "extracted"]
    staleish = [
        a
        for a in assertions
        if a.review_status in {"needs_recheck", "stale"}
        and a.superseded_by_assertion_id is None
    ]

    verified_rule_keys = {a.rule_key for a in verified}

    source_count = len(sources)
    fetch_failure_count = sum(
        1
        for s in sources
        if s.http_status is None
    )
    stale_warning_count = len(staleish)

    has_sources = source_count > 0
    has_extracted = bool(extracted or verified)

    municipal_verified_count = len(CORE_MUNICIPAL_RULES & verified_rule_keys)
    state_federal_verified_count = len(CORE_STATE_FEDERAL_RULES & verified_rule_keys)

    # Municipal can be ready with registration + inspection verified.
    municipal_ok = (
        "rental_registration_required" in verified_rule_keys
        and "inspection_program_exists" in verified_rule_keys
    )

    statefed_ok = state_federal_verified_count >= 2

    pha_required = pha is not None
    pha_ok = (not pha_required) or (len(CORE_PHA_RULES & verified_rule_keys) >= 1)

    coverage_status = "not_started"
    if has_sources:
        coverage_status = "sources_ingested"
    if has_extracted:
        coverage_status = "assertions_extracted"
    if verified:
        coverage_status = "review_in_progress"
    if municipal_ok and statefed_ok:
        coverage_status = "verified_core"
    if municipal_ok and statefed_ok and pha_ok:
        coverage_status = "verified_extended"

    production_readiness = "partial"
    if has_extracted and not verified:
        production_readiness = "needs_review"
    if municipal_ok and statefed_ok and pha_ok:
        production_readiness = "ready"
    elif municipal_ok or statefed_ok:
        production_readiness = "partial"

    if stale_warning_count > 0:
        production_readiness = "stale_warning"

    confidence_label = "low"
    if municipal_ok and statefed_ok:
        confidence_label = "medium"
    if production_readiness == "ready":
        confidence_label = "high"
    if production_readiness == "stale_warning":
        confidence_label = "medium"

    return {
        "state": st,
        "county": cnty,
        "city": cty,
        "pha_name": pha,
        "coverage_status": coverage_status,
        "production_readiness": production_readiness,
        "confidence_label": confidence_label,
        "verified_rule_count": len(verified),
        "source_count": source_count,
        "fetch_failure_count": fetch_failure_count,
        "stale_warning_count": stale_warning_count,
        "has_sources": has_sources,
        "has_extracted": has_extracted,
        "verified_rule_keys": sorted(verified_rule_keys),
        "municipal_core_ok": municipal_ok,
        "state_federal_core_ok": statefed_ok,
        "pha_core_ok": pha_ok,
        "municipal_verified_count": municipal_verified_count,
        "state_federal_verified_count": state_federal_verified_count,
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
) -> JurisdictionCoverageStatus:
    stats = compute_coverage_status(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )

    existing = (
        db.query(JurisdictionCoverageStatus)
        .filter(JurisdictionCoverageStatus.state == stats["state"])
        .filter(JurisdictionCoverageStatus.county == stats["county"])
        .filter(JurisdictionCoverageStatus.city == stats["city"])
        .filter(JurisdictionCoverageStatus.pha_name == stats["pha_name"])
        .filter(
            JurisdictionCoverageStatus.org_id.is_(None)
            if org_id is None
            else JurisdictionCoverageStatus.org_id == org_id
        )
        .first()
    )

    now = datetime.utcnow()

    if existing is None:
        row = JurisdictionCoverageStatus(
            org_id=org_id,
            state=stats["state"],
            county=stats["county"],
            city=stats["city"],
            pha_name=stats["pha_name"],
            coverage_status=stats["coverage_status"],
            production_readiness=stats["production_readiness"],
            last_reviewed_at=now if stats["verified_rule_count"] > 0 else None,
            last_source_refresh_at=now if stats["source_count"] > 0 else None,
            verified_rule_count=stats["verified_rule_count"],
            source_count=stats["source_count"],
            fetch_failure_count=stats["fetch_failure_count"],
            stale_warning_count=stats["stale_warning_count"],
            notes=notes,
            updated_at=now,
        )
        db.add(row)
    else:
        row = existing
        row.coverage_status = stats["coverage_status"]
        row.production_readiness = stats["production_readiness"]
        row.last_reviewed_at = (
            now if stats["verified_rule_count"] > 0 else row.last_reviewed_at
        )
        row.last_source_refresh_at = (
            now if stats["source_count"] > 0 else row.last_source_refresh_at
        )
        row.verified_rule_count = stats["verified_rule_count"]
        row.source_count = stats["source_count"]
        row.fetch_failure_count = stats["fetch_failure_count"]
        row.stale_warning_count = stats["stale_warning_count"]
        row.notes = notes or row.notes
        row.updated_at = now

    db.commit()
    db.refresh(row)
    return row
