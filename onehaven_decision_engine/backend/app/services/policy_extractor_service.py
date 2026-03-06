# backend/app/services/policy_extractor_service.py
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.policy_models import PolicyAssertion, PolicySource


def _dumps(v: Any) -> str:
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return "{}"


def _add_assertion(
    created: list[PolicyAssertion],
    *,
    target_org_id: Optional[int],
    source: PolicySource,
    now: datetime,
    rule_key: str,
    value: dict[str, Any],
    confidence: float,
) -> None:
    created.append(
        PolicyAssertion(
            org_id=target_org_id,
            source_id=source.id,
            state=source.state,
            county=source.county,
            city=source.city,
            pha_name=source.pha_name,
            program_type=source.program_type,
            rule_key=rule_key,
            value_json=_dumps(value),
            confidence=confidence,
            review_status="extracted",
            extracted_at=now,
        )
    )


def extract_assertions_for_source(
    db: Session,
    *,
    source: PolicySource,
    org_id: Optional[int],
    org_scope: bool = True,
) -> list[PolicyAssertion]:
    """
    Conservative extraction:
    - remove prior extracted assertions for this exact source + scope
    - re-emit clean extracted suggestions
    """
    target_org_id = org_id if org_scope else None
    now = datetime.utcnow()
    created: list[PolicyAssertion] = []

    q = db.query(PolicyAssertion).filter(PolicyAssertion.source_id == source.id)
    if target_org_id is None:
        q = q.filter(PolicyAssertion.org_id.is_(None))
    else:
        q = q.filter(PolicyAssertion.org_id == target_org_id)
    q = q.filter(PolicyAssertion.review_status == "extracted")
    q.delete(synchronize_session=False)
    db.commit()

    url = (source.url or "").lower()
    title = (source.title or "").lower()
    publisher = (source.publisher or "").lower()

    _add_assertion(
        created,
        target_org_id=target_org_id,
        source=source,
        now=now,
        rule_key="document_reference",
        value={
            "type": "document_reference",
            "url": source.url,
            "publisher": source.publisher,
            "title": source.title,
            "content_type": source.content_type,
            "retrieved_at": source.retrieved_at.isoformat() if source.retrieved_at else None,
            "sha256": source.content_sha256,
            "notes": source.notes,
        },
        confidence=0.15,
    )

    # Federal
    if "ecfr.gov" in url and "part-982" in url:
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            now=now,
            rule_key="federal_hcv_regulations_anchor",
            value={
                "summary": "HCV regulations live in 24 CFR Part 982 (eCFR).",
                "url": source.url,
            },
            confidence=0.85,
        )

    if "ecfr.gov" in url and "/part-5" in url:
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            now=now,
            rule_key="federal_nspire_anchor",
            value={
                "summary": "HUD program requirements and inspection standards are reflected in 24 CFR Part 5 / current NSPIRE structure.",
                "url": source.url,
            },
            confidence=0.75,
        )

    if "federalregister.gov" in url and "nspire" in url:
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            now=now,
            rule_key="federal_notice_anchor",
            value={
                "summary": "Federal Register notice relevant to HCV / NSPIRE implementation timing or standards.",
                "url": source.url,
            },
            confidence=0.60,
        )

    # Michigan state
    if "legislature.mi.gov" in url:
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            now=now,
            rule_key="mi_statute_anchor",
            value={
                "summary": "Michigan statutory landlord-tenant baseline source.",
                "url": source.url,
            },
            confidence=0.70,
        )

    # Detroit
    if "detroitmi.gov" in url:
        if "landlord-rental" in url or "tenant-rental-property" in url:
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                now=now,
                rule_key="rental_registration_required",
                value={
                    "summary": "Detroit rental property workflow appears to require registration and local compliance handling.",
                    "url": source.url,
                    "scope_hint": "city",
                },
                confidence=0.55,
            )

        if "rental-certificate" in url or "certificate of compliance" in title:
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                now=now,
                rule_key="certificate_required_before_occupancy",
                value={
                    "summary": "Detroit certificate of compliance / rental certificate process is relevant before normal compliant operation.",
                    "url": source.url,
                    "scope_hint": "city",
                },
                confidence=0.55,
            )

        if "inspections" in url or "faq" in url or "rental-compliance-map" in url:
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                now=now,
                rule_key="inspection_program_exists",
                value={
                    "summary": "Detroit rental workflow includes local inspection program requirements or cadence guidance.",
                    "url": source.url,
                    "scope_hint": "city",
                },
                confidence=0.55,
            )

    # Dearborn
    if "dearborn.gov" in url and "rental-property-information" in url:
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            now=now,
            rule_key="rental_registration_required",
            value={
                "summary": "Dearborn rental property workflow includes application, inspection, and registration steps.",
                "url": source.url,
                "scope_hint": "city",
            },
            confidence=0.60,
        )
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            now=now,
            rule_key="inspection_program_exists",
            value={
                "summary": "Dearborn rental properties are inspected through a city process before compliance is finalized.",
                "url": source.url,
                "scope_hint": "city",
            },
            confidence=0.60,
        )
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            now=now,
            rule_key="certificate_required_before_occupancy",
            value={
                "summary": "Dearborn indicates a Certificate of Occupancy is obtained through inspection/compliance flow before rental listing/operation.",
                "url": source.url,
                "scope_hint": "city",
            },
            confidence=0.60,
        )

    # Warren
    if "cityofwarren.org" in url:
        if "rental-inspections-division" in url or "rental-application" in url:
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                now=now,
                rule_key="rental_registration_required",
                value={
                    "summary": "Warren appears to require rental licensing / application / inspections workflow.",
                    "url": source.url,
                    "scope_hint": "city",
                },
                confidence=0.60,
            )
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                now=now,
                rule_key="inspection_program_exists",
                value={
                    "summary": "Warren rental workflow includes inspection division oversight.",
                    "url": source.url,
                    "scope_hint": "city",
                },
                confidence=0.60,
            )

        if "property-maintenance-division" in url:
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                now=now,
                rule_key="property_maintenance_enforcement_anchor",
                value={
                    "summary": "Warren property maintenance enforcement page relevant to rental compliance operations.",
                    "url": source.url,
                    "scope_hint": "city",
                },
                confidence=0.45,
            )

    # Southfield
    if "cityofsouthfield.com" in url:
        if "rental-housing" in url or "rental_registration_application" in url:
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                now=now,
                rule_key="rental_registration_required",
                value={
                    "summary": "Southfield rental workflow includes registration and inspection requirements.",
                    "url": source.url,
                    "scope_hint": "city",
                },
                confidence=0.60,
            )
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                now=now,
                rule_key="inspection_program_exists",
                value={
                    "summary": "Southfield rental housing page indicates inspection requirements.",
                    "url": source.url,
                    "scope_hint": "city",
                },
                confidence=0.60,
            )

        if "housing-section-8" in url:
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                now=now,
                rule_key="pha_admin_plan_anchor",
                value={
                    "summary": "Southfield HCV / Section 8 administrative source.",
                    "url": source.url,
                    "pha_name": source.pha_name or "Southfield Housing Commission",
                },
                confidence=0.55,
            )

        if "transfer%20letter%202025" in url or "transfer letter" in title:
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                now=now,
                rule_key="pha_administrator_changed",
                value={
                    "summary": "Southfield indicates HCV administration transferred to Plymouth Housing Commission effective 2025-10-01.",
                    "url": source.url,
                    "prior_pha_name": "Southfield Housing Commission",
                    "new_admin_hint": "Plymouth Housing Commission",
                },
                confidence=0.75,
            )

    # Pontiac
    if "pontiac.mi.us" in url or "pontiacminew" in url:
        if "property_rentals" in url or "rentalapp" in url:
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                now=now,
                rule_key="rental_registration_required",
                value={
                    "summary": "Pontiac indicates rental properties must be properly registered and kept up to code.",
                    "url": source.url,
                    "scope_hint": "city",
                },
                confidence=0.60,
            )
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                now=now,
                rule_key="inspection_program_exists",
                value={
                    "summary": "Pontiac rental process includes inspection / compliance workflow through Building Safety.",
                    "url": source.url,
                    "scope_hint": "city",
                },
                confidence=0.60,
            )

        if "building_safety" in url:
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                now=now,
                rule_key="building_safety_division_anchor",
                value={
                    "summary": "Pontiac Building Safety Division is the city enforcement / inspection anchor for rental workflow.",
                    "url": source.url,
                    "scope_hint": "city",
                },
                confidence=0.50,
            )

    # Livonia
    if "livonia.gov" in url:
        if "inspection-building-enforcement" in url or "rental-guide" in url or "rental-license-application" in url:
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                now=now,
                rule_key="rental_registration_required",
                value={
                    "summary": "Livonia rental workflow uses inspection / enforcement and rental licensing process.",
                    "url": source.url,
                    "scope_hint": "city",
                },
                confidence=0.60,
            )
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                now=now,
                rule_key="inspection_program_exists",
                value={
                    "summary": "Livonia rental properties are governed through the Inspection Department / enforcement workflow.",
                    "url": source.url,
                    "scope_hint": "city",
                },
                confidence=0.60,
            )

    # Westland
    if "cityofwestland.com" in url:
        if "residential-rental-program" in url or "rental-registration-application" in url:
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                now=now,
                rule_key="rental_registration_required",
                value={
                    "summary": "Westland residential rental program requires registration on a recurring cycle.",
                    "url": source.url,
                    "scope_hint": "city",
                },
                confidence=0.65,
            )
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                now=now,
                rule_key="inspection_program_exists",
                value={
                    "summary": "Westland rental workflow includes initial/final inspections and compliance certification.",
                    "url": source.url,
                    "scope_hint": "city",
                },
                confidence=0.65,
            )
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                now=now,
                rule_key="certificate_required_before_occupancy",
                value={
                    "summary": "Westland rental program includes certification of compliance within the registration/inspection process.",
                    "url": source.url,
                    "scope_hint": "city",
                },
                confidence=0.60,
            )

        if "building-division" in url:
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                now=now,
                rule_key="building_division_anchor",
                value={
                    "summary": "Westland Building Division page is the city anchor for rental certificates and inspection actions.",
                    "url": source.url,
                    "scope_hint": "city",
                },
                confidence=0.45,
            )

    # Taylor
    if "cityoftaylor.com" in url or "ci.taylor.mi.us" in url:
        if "rental-department" in url or "rental-property-registration-application" in url:
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                now=now,
                rule_key="rental_registration_required",
                value={
                    "summary": "Taylor rental workflow requires property registration and inspection scheduling through the Rental Department.",
                    "url": source.url,
                    "scope_hint": "city",
                },
                confidence=0.65,
            )

        if "rental-property-insp" in url or "rental-inspection" in url or "rental-department" in url:
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                now=now,
                rule_key="inspection_program_exists",
                value={
                    "summary": "Taylor rental workflow includes formal rental inspection process and associated fees/forms.",
                    "url": source.url,
                    "scope_hint": "city",
                },
                confidence=0.65,
            )

    # DHC / Detroit PHA
    if "dhcmi.org" in url or "detroit housing commission" in publisher:
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            now=now,
            rule_key="pha_admin_plan_anchor",
            value={
                "summary": "Detroit Housing Commission administrative or landlord process source.",
                "url": source.url,
                "pha_name": source.pha_name or "Detroit Housing Commission",
            },
            confidence=0.70,
        )

        if "landlord" in url or "faq" in url or "guide" in title or "guidebook" in title:
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                now=now,
                rule_key="pha_landlord_packet_required",
                value={
                    "summary": "Detroit PHA landlord-facing forms/guidance exist and may be operationally required.",
                    "url": source.url,
                    "pha_name": source.pha_name or "Detroit Housing Commission",
                },
                confidence=0.55,
            )

    # MSHDA
    if "michigan.gov/mshda" in url:
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            now=now,
            rule_key="mshda_program_anchor",
            value={
                "summary": "MSHDA statewide HCV / landlord operations source.",
                "url": source.url,
            },
            confidence=0.60,
        )

        if "hcv-landlords" in url:
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                now=now,
                rule_key="hap_contract_and_tenancy_addendum_required",
                value={
                    "summary": "MSHDA landlord pages indicate HAP contract / tenancy addendum workflow requirements.",
                    "url": source.url,
                },
                confidence=0.60,
            )

        if "payment" in url or "schedule" in url:
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                now=now,
                rule_key="landlord_payment_timing_reference",
                value={
                    "summary": "MSHDA payment timing / direct deposit schedule source for landlord operations.",
                    "url": source.url,
                },
                confidence=0.50,
            )

    db.add_all(created)
    db.commit()

    for a in created:
        db.refresh(a)

    return created
