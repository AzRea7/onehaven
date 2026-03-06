from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from app.policy_models import (
    JurisdictionCoverageStatus,
    PolicyAssertion,
    PolicySource,
)

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
}


def _norm_state(s: Optional[str]) -> str:
    return (s or "MI").strip().upper()


def _norm_lower(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    v = s.strip().lower()
    return v or None


def _apply_scope_filters(
    q,
    model,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
):
    q = q.filter(model.state == state)
    if org_id is None:
        q = q.filter(model.org_id.is_(None))
    else:
        q = q.filter((model.org_id == org_id) | (model.org_id.is_(None)))
    if county is not None:
        q = q.filter(model.county == county)
    if city is not None:
        q = q.filter(model.city == city)
    if pha_name is not None and hasattr(model, "pha_name"):
        q = q.filter(model.pha_name == pha_name)
    return q


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
    pha = pha_name.strip() if pha_name else None

    src_q = db.query(PolicySource)
    src_q = _apply_scope_filters(
        src_q,
        PolicySource,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )
    sources = src_q.all()

    assertion_q = db.query(PolicyAssertion)
    assertion_q = _apply_scope_filters(
        assertion_q,
        PolicyAssertion,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )
    assertions = assertion_q.all()

    verified = [a for a in assertions if a.review_status == "verified"]
    extracted = [a for a in assertions if a.review_status == "extracted"]
    staleish = [a for a in assertions if a.review_status in {"needs_recheck", "stale"}]

    verified_rule_keys = {a.rule_key for a in verified}
    source_count = len(sources)
    fetch_failure_count = sum(1 for s in sources if s.http_status is None)
    stale_warning_count = len(staleish)

    has_sources = source_count > 0
    has_extracted = len(extracted) > 0 or len(verified) > 0

    municipal_ok = len(CORE_MUNICIPAL_RULES & verified_rule_keys) >= 2
    statefed_ok = len(CORE_STATE_FEDERAL_RULES & verified_rule_keys) >= 2
    pha_ok = len(CORE_PHA_RULES & verified_rule_keys) >= 1

    coverage_status = "not_started"
    production_readiness = "partial"

    if has_sources:
        coverage_status = "sources_ingested"
    if has_extracted:
        coverage_status = "assertions_extracted"
    if len(verified) > 0:
        coverage_status = "review_in_progress"

    if municipal_ok and statefed_ok:
        coverage_status = "verified_core"
        production_readiness = "verified_core"

    if municipal_ok and statefed_ok and (pha_ok or pha is None):
        coverage_status = "verified_extended"
        production_readiness = "verified_extended"

    if stale_warning_count > 0:
        production_readiness = "stale_warning"

    if len(verified) == 0 and has_extracted:
        production_readiness = "needs_review"

    confidence_label = "low"
    if production_readiness == "verified_core":
        confidence_label = "medium"
    if production_readiness == "verified_extended":
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
