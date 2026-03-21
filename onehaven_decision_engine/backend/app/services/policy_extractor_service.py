from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.domain.jurisdiction_categories import normalize_category, normalize_categories
from app.policy_models import PolicyAssertion, PolicySource


def _dumps(v: Any) -> str:
    try:
        return json.dumps(v, ensure_ascii=False, sort_keys=True)
    except Exception:
        return "{}"


def _rule_family_for(rule_key: str) -> str:
    mapping = {
        "document_reference": "document_reference",
        "federal_hcv_regulations_anchor": "federal_hcv",
        "federal_nspire_anchor": "federal_nspire",
        "federal_notice_anchor": "federal_notice",
        "mi_statute_anchor": "mi_landlord_tenant",
        "rental_registration_required": "rental_registration",
        "certificate_required_before_occupancy": "certificate_before_occupancy",
        "inspection_program_exists": "inspection_program",
        "property_maintenance_enforcement_anchor": "property_maintenance",
        "building_safety_division_anchor": "building_safety",
        "building_division_anchor": "building_division",
        "pha_admin_plan_anchor": "pha_admin_plan",
        "pha_administrator_changed": "pha_admin_transfer",
        "pha_landlord_packet_required": "pha_landlord_workflow",
        "mshda_program_anchor": "mshda_program",
        "hap_contract_and_tenancy_addendum_required": "voucher_lease_packet",
        "landlord_payment_timing_reference": "landlord_payment_timing",
    }
    return mapping.get(rule_key, rule_key)


def _assertion_type_for(rule_key: str) -> str:
    if rule_key == "document_reference":
        return "document_reference"
    if rule_key.endswith("_anchor"):
        return "anchor"
    if rule_key == "pha_administrator_changed":
        return "superseding_notice"
    return "operational"


def _priority_for(rule_key: str) -> int:
    if rule_key in {
        "rental_registration_required",
        "certificate_required_before_occupancy",
        "inspection_program_exists",
        "hap_contract_and_tenancy_addendum_required",
        "pha_landlord_packet_required",
    }:
        return 10
    if rule_key in {
        "pha_administrator_changed",
        "landlord_payment_timing_reference",
    }:
        return 20
    if rule_key.endswith("_anchor"):
        return 50
    return 100


def _source_rank_for(source: PolicySource) -> int:
    url = (source.url or "").lower()
    publisher = (source.publisher or "").lower()

    if "ecfr.gov" in url:
        return 10
    if "federalregister.gov" in url:
        return 20
    if "legislature.mi.gov" in url:
        return 30
    if "michigan.gov/mshda" in url:
        return 40
    if ".gov" in url:
        return 50
    if "housing commission" in publisher or "dhcmi.org" in url:
        return 60
    return 100


def _stale_after_for(rule_key: str, source: PolicySource, now: datetime) -> datetime:
    url = (source.url or "").lower()

    if "ecfr.gov" in url or "legislature.mi.gov" in url:
        return now + timedelta(days=365)

    if rule_key in {
        "federal_hcv_regulations_anchor",
        "federal_nspire_anchor",
        "mi_statute_anchor",
    }:
        return now + timedelta(days=365)

    if rule_key in {
        "pha_administrator_changed",
        "landlord_payment_timing_reference",
        "pha_landlord_packet_required",
        "hap_contract_and_tenancy_addendum_required",
    }:
        return now + timedelta(days=120)

    return now + timedelta(days=180)


def _normalized_category_for(rule_key: str) -> str | None:
    mapping = {
        "document_reference": None,
        "federal_hcv_regulations_anchor": "section8",
        "federal_nspire_anchor": "inspection",
        "federal_notice_anchor": "section8",
        "mi_statute_anchor": "safety",
        "rental_registration_required": "registration",
        "certificate_required_before_occupancy": "occupancy",
        "inspection_program_exists": "inspection",
        "property_maintenance_enforcement_anchor": "safety",
        "building_safety_division_anchor": "safety",
        "building_division_anchor": "permits",
        "pha_admin_plan_anchor": "section8",
        "pha_administrator_changed": "section8",
        "pha_landlord_packet_required": "section8",
        "mshda_program_anchor": "section8",
        "hap_contract_and_tenancy_addendum_required": "section8",
        "landlord_payment_timing_reference": "section8",
    }
    return normalize_category(mapping.get(rule_key))


def _coverage_status_for(rule_key: str) -> str:
    if rule_key == "document_reference":
        return "candidate"
    if rule_key.endswith("_anchor"):
        return "candidate"
    return "covered"


def _refresh_source_category_metadata(source: PolicySource, created: list[PolicyAssertion], now: datetime) -> None:
    categories = normalize_categories(
        [a.normalized_category for a in created if getattr(a, "normalized_category", None)]
    )
    source.normalized_categories_json = _dumps(categories)
    source.freshness_checked_at = now
    source.last_verified_at = now if bool(getattr(source, "is_authoritative", False)) else source.last_verified_at

    status_ok = source.http_status is not None and 200 <= int(source.http_status) < 400
    if not status_ok:
        source.freshness_status = "fetch_failed"
        source.freshness_reason = "http_status_not_successful"
    elif not source.retrieved_at:
        source.freshness_status = "unknown"
        source.freshness_reason = "missing_retrieved_at"
    elif source.retrieved_at < (now - timedelta(days=180)):
        source.freshness_status = "stale"
        source.freshness_reason = "retrieved_at_older_than_180_days"
    else:
        source.freshness_status = "fresh"
        source.freshness_reason = None


def _maybe_add_warren_certificate_rule(
    created: list[PolicyAssertion],
    *,
    target_org_id: Optional[int],
    source: PolicySource,
    now: datetime,
) -> None:
    url = (source.url or "").lower()
    title = (source.title or "").lower()

    if "cityofwarren.org" not in url:
        return

    if (
        "building_res_city_certification_app.pdf" in url
        or "residential city certification" in title
    ):
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            now=now,
            rule_key="certificate_required_before_occupancy",
            value={
                "summary": (
                    "Warren requires residential city certification before occupancy "
                    "for a vacant residential dwelling that has been posted 'no occupancy'."
                ),
                "status": "conditional",
                "condition": (
                    "Applies when a vacant residential dwelling has been posted "
                    "'no occupancy' and is being reoccupied."
                ),
                "url": source.url,
                "scope_hint": "city",
            },
            confidence=0.92,
        )
        return

    if (
        "building_certificate_of_compliance_application.pdf" in url
        or "certificate of compliance application" in title
    ):
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            now=now,
            rule_key="certificate_required_before_occupancy",
            value={
                "summary": (
                    "Warren requires a certificate of compliance before occupancy "
                    "where land, a building, or a structure is erected, altered, "
                    "or changed in use."
                ),
                "status": "conditional",
                "condition": (
                    "Applies to erected, altered, or changed-in-use land/buildings/structures "
                    "before occupancy or use."
                ),
                "url": source.url,
                "scope_hint": "city",
            },
            confidence=0.92,
        )
        return


def _haystack(source: PolicySource) -> str:
    return " ".join(
        [
            str(source.url or ""),
            str(source.title or ""),
            str(source.publisher or ""),
            str(source.notes or ""),
            str(source.extracted_text or ""),
        ]
    ).lower()


def _has_any(text: str, patterns: list[str]) -> bool:
    return any(p in text for p in patterns)


def _already_added(created: list[PolicyAssertion], rule_key: str) -> bool:
    return any(a.rule_key == rule_key for a in created)


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
    if _already_added(created, rule_key):
        return

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
            rule_family=_rule_family_for(rule_key),
            assertion_type=_assertion_type_for(rule_key),
            value_json=_dumps(value),
            confidence=confidence,
            priority=_priority_for(rule_key),
            source_rank=_source_rank_for(source),
            review_status="extracted",
            normalized_category=_normalized_category_for(rule_key),
            coverage_status=_coverage_status_for(rule_key),
            source_freshness_status=getattr(source, "freshness_status", None),
            stale_after=_stale_after_for(rule_key, source, now),
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
    - inference is based on source metadata only (url/title/publisher/notes/text)
    - attach normalized_category + coverage_status for downstream completeness
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
    text = _haystack(source)

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
        if _has_any(
            text,
            [
                "landlord-rental",
                "tenant-rental-property",
                "rental requirements faq",
            ],
        ):
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

        if _has_any(text, ["rental-certificate", "certificate of compliance"]):
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

        if _has_any(text, ["inspections", "faq", "rental-compliance-map"]):
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
    if "dearborn.gov" in url and "rental-property-information" in text:
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
        _maybe_add_warren_certificate_rule(
            created,
            target_org_id=target_org_id,
            source=source,
            now=now,
        )

        if _has_any(text, ["rental-inspections-division", "rental application", "rental license application"]):
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

        if "property-maintenance-division" in text:
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
        if _has_any(text, ["rental-housing", "rental registration application"]):
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

        if "housing-section-8" in text:
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

        if _has_any(text, ["transfer letter 2025", "transfer%20letter%202025", "plymouth"]):
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
        if _has_any(text, ["property_rentals", "rental registration application", "rentalapp"]):
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

        if "building_safety" in text:
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
        if _has_any(
            text,
            [
                "inspection-building-enforcement",
                "rental-guide",
                "rental-license-application",
                "rental properties guide",
            ],
        ):
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
        if _has_any(text, ["residential-rental-program", "rental registration application"]):
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

        if "building-division" in text:
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
        if _has_any(text, ["rental-department", "rental property registration application"]):
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

        if _has_any(text, ["rental-property-insp", "rental-inspection", "rental-department"]):
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
    if "dhcmi.org" in url or "detroit housing commission" in (source.publisher or "").lower():
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

        if _has_any(text, ["landlord", "faq", "guide", "guidebook"]):
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

        if "hcv-landlords" in text:
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

        if _has_any(text, ["payment", "schedule", "hap/uap"]):
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

    _refresh_source_category_metadata(source, created, now)

    db.add(source)
    db.add_all(created)
    db.commit()

    for a in created:
        db.refresh(a)

    return created