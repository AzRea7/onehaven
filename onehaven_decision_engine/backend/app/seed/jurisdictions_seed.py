from __future__ import annotations
"""
Seed a realistic starter set of jurisdiction rules for Michigan cities.

Run example:
    python -m backend.app.seed.jurisdictions_seed
    (or run via your venv with PYTHONPATH set to repo root)
"""

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..db import SessionLocal
from ..models import JurisdictionRule
from ..domain.jurisdiction_defaults import defaults_for_michigan
from ..services.jurisdiction_profile_service import ensure_registry_source_mapping


def _notes_from_default(default) -> str:
    policy = default.to_profile_policy()
    coverage = policy.get("coverage") or {}
    discovery = (policy.get("discovery") or {}).get("search_hints") or {}
    thresholds = coverage.get("thresholds") or {}
    trust = ((policy.get("trust") or {}).get("projection") or {})
    freshness = ((policy.get("freshness") or {}).get("policy_sources") or {})
    universe = policy.get("expected_rule_universe") or {}
    rule_inventory = coverage.get("rule_family_inventory") or universe.get("rule_family_inventory") or {}
    legal_categories = coverage.get("legally_binding_categories") or universe.get("legally_binding_categories") or []
    operational_categories = coverage.get("operational_heuristic_categories") or universe.get("operational_heuristic_categories") or []
    property_proof_categories = coverage.get("property_proof_required_categories") or universe.get("property_proof_required_categories") or []
    source_families = coverage.get("required_source_families_by_category") or universe.get("required_source_families_by_category") or {}
    authority_scope = coverage.get("authority_scope_by_category") or universe.get("authority_scope_by_category") or {}

    note_parts = [
        default.notes or "",
        f"Expected categories: {', '.join(coverage.get('required_categories') or [])}." if coverage.get("required_categories") else "",
        f"Critical categories: {', '.join(coverage.get('critical_categories') or [])}." if coverage.get("critical_categories") else "",
        f"Legal rule families: {', '.join(legal_categories)}." if legal_categories else "",
        f"Operational heuristic families: {', '.join(operational_categories)}." if operational_categories else "",
        f"Property-proof families: {', '.join(property_proof_categories)}." if property_proof_categories else "",
        f"Discovery base terms: {', '.join(discovery.get('base_terms') or [])}." if discovery.get("base_terms") else "",
        f"Primary source hints: {', '.join(discovery.get('preferred_source_kinds') or [])}." if discovery.get("preferred_source_kinds") else "",
        f"Authority scope by category: {'; '.join(f'{k}:{v}' for k, v in authority_scope.items())}." if authority_scope else "",
        f"Required source families: {'; '.join(f'{k}->{', '.join(v)}' for k, v in source_families.items())}." if source_families else "",
        f"Authoritative source threshold: {thresholds.get('authoritative_source')}." if thresholds.get("authoritative_source") is not None else "",
        f"Default stale-days threshold: {freshness.get('stale_days')}." if freshness.get("stale_days") is not None else "",
        f"Projection trust minimum: {trust.get('min_completeness_score_for_trust')}." if trust.get("min_completeness_score_for_trust") is not None else "",
        f"Rule-family inventory count: {len(rule_inventory)}." if rule_inventory else "",
    ]
    return " ".join(part.strip() for part in note_parts if part and str(part).strip())


def _seed_payload_from_default(default) -> dict:
    return dict(
        city=default.city,
        state=default.state,
        require_rental_registration=bool(default.rental_license_required),
        require_city_inspection=bool(default.inspection_authority or default.inspection_frequency),
        lead_paint_affidavit_required=bool(
            "lead" in {str(x).strip().lower() for x in (default.required_categories(include_section8=True) or [])}
            or any("lead" in str(x).lower() for x in (default.typical_fail_points or []))
        ),
        criminal_background_policy="moderate",
        typical_days_to_approve=int(default.processing_days or 14),
        friction_weight=float(
            1.25 if default.coverage_confidence == "low" else 1.10 if default.coverage_confidence == "medium" else 1.00
        ),
        notes=_notes_from_default(default),
    )


SEED = [_seed_payload_from_default(default) for default in defaults_for_michigan()]


def upsert_rule(db: Session, org_id: int, payload: dict) -> JurisdictionRule:
    city = payload["city"]
    state = payload["state"]
    existing = db.execute(
        select(JurisdictionRule).where(
            JurisdictionRule.org_id == org_id,
            JurisdictionRule.city == city,
            JurisdictionRule.state == state,
        )
    ).scalar_one_or_none()

    if existing:
        for k, v in payload.items():
            setattr(existing, k, v)
        return existing

    rule = JurisdictionRule(org_id=org_id, **payload)
    db.add(rule)
    return rule


def seed_registry_and_sources(db: Session, org_id: int) -> list[dict]:
    synced: list[dict] = []
    for default in defaults_for_michigan():
        policy = default.to_profile_policy()
        synced.append(
            ensure_registry_source_mapping(
                db,
                org_id=org_id,
                state=default.state,
                county=getattr(default, 'county', None),
                city=default.city,
                pha_name=getattr(default, 'housing_authority', None),
                policy=policy,
            )
        )
    return synced


def main():
    org_id = 1
    db = SessionLocal()
    try:
        for payload in SEED:
            upsert_rule(db, org_id, payload)
        registry_synced = seed_registry_and_sources(db, org_id)
        db.commit()
        print(f"Seeded {len(SEED)} jurisdiction rules into org_id={org_id}.")
        print(f"Synced {len(registry_synced)} jurisdiction registry/source-family mappings.")
    finally:
        db.close()


if __name__ == "__main__":
    main()


from ..services.jurisdiction_registry_service import (
    JURISDICTION_TYPE_CITY,
    JURISDICTION_TYPE_COUNTY,
    JURISDICTION_TYPE_PHA,
    JURISDICTION_TYPE_STATE,
    ONBOARDING_SOURCE_MAPPED,
    get_or_create_jurisdiction_with_sources,
)


def _registry_source_links_from_default(default) -> dict:
    policy = default.to_profile_policy()
    coverage = policy.get('coverage') or {}
    discovery = (policy.get('discovery') or {}).get('search_hints') or {}
    evidence = list(getattr(default, 'source_evidence', None) or [])

    source_links: dict[str, dict[str, object]] = {}
    for item in evidence:
        if not isinstance(item, dict):
            continue
        url = item.get('url')
        label = str(item.get('label') or item.get('kind') or 'official_source').strip().lower().replace(' ', '_')
        if url:
            source_links[label] = {
                'url': url,
                'label': item.get('label') or label,
                'kind': item.get('kind') or 'official_source',
                'publisher': item.get('publisher'),
                'trusted': True,
            }

    # deterministic family placeholders are stored only when official URLs already exist
    for family, url in {
        'city_code': coverage.get('municipal_code_url'),
        'rental_license': coverage.get('rental_license_url'),
        'inspection_program': coverage.get('inspection_url'),
        'zoning': coverage.get('zoning_url'),
        'housing_authority': coverage.get('housing_authority_url'),
    }.items():
        if isinstance(url, str) and url.strip():
            source_links[family] = {'url': url.strip(), 'label': family.replace('_', ' ').title(), 'trusted': True}

    if not source_links and default.city:
        source_links['search_hints'] = {
            'label': f'{default.city} discovery hints',
            'preferred_source_kinds': discovery.get('preferred_source_kinds') or [],
            'base_terms': discovery.get('base_terms') or [],
            'trusted': False,
        }
    return source_links


def _registry_source_family_map_from_default(default) -> dict:
    policy = default.to_profile_policy()
    universe = policy.get('expected_rule_universe') or {}
    coverage = policy.get('coverage') or {}
    return dict(
        coverage.get('required_source_families_by_category')
        or universe.get('required_source_families_by_category')
        or {}
    )


def seed_jurisdiction_registry(db: Session, org_id: int) -> list[dict]:
    seeded: list[dict] = []
    state_record = get_or_create_jurisdiction_with_sources(
        db,
        org_id=org_id,
        jurisdiction_type=JURISDICTION_TYPE_STATE,
        state_code='MI',
        state_name='Michigan',
        official_website='https://www.michigan.gov',
        onboarding_status=ONBOARDING_SOURCE_MAPPED,
        source_confidence=1.0,
        source_links={
            'state_portal': {'url': 'https://www.michigan.gov', 'label': 'Michigan.gov', 'trusted': True},
            'legislature': {'url': 'https://www.legislature.mi.gov', 'label': 'Michigan Legislature', 'trusted': True},
            'courts': {'url': 'https://www.courts.michigan.gov', 'label': 'Michigan Courts', 'trusted': True},
        },
        validation_metadata={'registry_seed': 'statewide_curated', 'review_state': 'seeded'},
        notes='Curated statewide registry anchors for Michigan compliance discovery.',
    )
    seeded.append({'state': state_record.slug, 'jurisdiction_id': state_record.id})

    counties: dict[str, int] = {}
    for default in defaults_for_michigan():
        county = getattr(default, 'county', None)
        if county and county not in counties:
            county_record = get_or_create_jurisdiction_with_sources(
                db,
                org_id=org_id,
                jurisdiction_type=JURISDICTION_TYPE_COUNTY,
                state_code=default.state,
                county_name=county,
                parent_jurisdiction_id=state_record.id,
                onboarding_status=ONBOARDING_SOURCE_MAPPED,
                source_confidence=0.95,
                source_links={},
                validation_metadata={'registry_seed': 'county_scope_from_defaults'},
                notes=f'County registry record for {county.title()} County, {default.state}.',
            )
            counties[county] = county_record.id
            seeded.append({'county': county_record.slug, 'jurisdiction_id': county_record.id})

        city_record = get_or_create_jurisdiction_with_sources(
            db,
            org_id=org_id,
            jurisdiction_type=JURISDICTION_TYPE_CITY,
            state_code=default.state,
            county_name=county,
            city_name=default.city,
            parent_jurisdiction_id=counties.get(county) or state_record.id,
            official_website=(getattr(default, 'source_evidence', [{}])[0] or {}).get('url') if getattr(default, 'source_evidence', None) else None,
            onboarding_status=ONBOARDING_SOURCE_MAPPED,
            source_confidence=0.90 if getattr(default, 'coverage_confidence', 'medium') != 'low' else 0.75,
            source_links=_registry_source_links_from_default(default),
            source_family_map=_registry_source_family_map_from_default(default),
            validation_metadata={
                'registry_seed': 'city_scope_from_defaults',
                'coverage_confidence': getattr(default, 'coverage_confidence', None),
                'housing_authority': getattr(default, 'housing_authority', None),
            },
            notes=_notes_from_default(default),
        )
        seeded.append({'city': city_record.slug, 'jurisdiction_id': city_record.id})

        housing_authority = getattr(default, 'housing_authority', None)
        if housing_authority:
            pha_record = get_or_create_jurisdiction_with_sources(
                db,
                org_id=org_id,
                jurisdiction_type=JURISDICTION_TYPE_PHA,
                state_code=default.state,
                county_name=county,
                city_name=default.city,
                parent_jurisdiction_id=city_record.id,
                onboarding_status=ONBOARDING_SOURCE_MAPPED,
                source_confidence=0.85,
                source_links=_registry_source_links_from_default(default),
                source_family_map={'program_overlay': ['pha_admin_plan', 'pha_program_page', 'official_form']},
                validation_metadata={'registry_seed': 'pha_scope_from_defaults'},
                notes=f'PHA registry record for {housing_authority}.',
            )
            seeded.append({'pha': pha_record.slug, 'jurisdiction_id': pha_record.id})
    return seeded


def seed_registry_and_sources(db: Session, org_id: int) -> list[dict]:
    synced: list[dict] = []
    synced.extend(seed_jurisdiction_registry(db, org_id))
    for default in defaults_for_michigan():
        policy = default.to_profile_policy()
        synced.append(
            ensure_registry_source_mapping(
                db,
                org_id=org_id,
                state=default.state,
                county=getattr(default, 'county', None),
                city=default.city,
                pha_name=getattr(default, 'housing_authority', None),
                policy=policy,
            )
        )
    return synced
