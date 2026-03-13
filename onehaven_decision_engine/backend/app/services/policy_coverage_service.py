from __future__ import annotations

from typing import Optional

from sqlalchemy.orm import Session

from app.policy_models import PolicyAssertion, PolicySource
from app.services.policy_cleanup_service import ARCHIVE_MARKER
from app.services.policy_catalog_admin_service import merged_catalog_for_market

# IMPORTANT:
# Adjust this import if your coverage ORM model lives elsewhere in your repo.
# Example alternatives:
# from app.models import PolicyCoverageStatus
# from app.models import PolicyCoverage as PolicyCoverageStatus
from app.policy_models import JurisdictionCoverageStatus


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
    """
    Only count stale warnings that are still meaningfully unresolved.

    Ignore:
    - superseded assertions
    - stale rows tied to inactive/archived sources
    - stale rows for rule_keys that already have a verified winner
    - stale rows for low-value non-operational rule keys
    """
    out: list[PolicyAssertion] = []

    for a in assertions:
        if a.review_status not in {"stale", "needs_recheck"}:
            continue

        if a.superseded_by_assertion_id is not None:
            continue

        if a.source_id is not None and a.source_id not in active_source_ids:
            continue

        rule_key = (a.rule_key or "").strip()

        # If this rule_key already has a verified winner, do not count its stale leftovers.
        if rule_key and rule_key in verified_rule_keys:
            continue

        # Ignore low-signal leftovers such as generic document references.
        if rule_key and rule_key not in IMPORTANT_RULE_KEYS:
            continue

        out.append(a)

    return out


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
    row.confidence_label = payload["confidence_label"]
    row.verified_rule_count = payload["verified_rule_count"]
    row.source_count = payload["source_count"]
    row.fetch_failure_count = payload["fetch_failure_count"]
    row.stale_warning_count = payload["stale_warning_count"]
    row.has_sources = payload["has_sources"]
    row.has_extracted = payload["has_extracted"]
    row.verified_rule_keys = payload["verified_rule_keys"]
    row.municipal_core_ok = payload["municipal_core_ok"]
    row.state_federal_core_ok = payload["state_federal_core_ok"]
    row.pha_core_ok = payload["pha_core_ok"]
    row.notes = notes

    db.commit()
    db.refresh(row)
    return row