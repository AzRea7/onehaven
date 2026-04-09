from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any, Optional

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.domain.jurisdiction_categories import normalize_category, normalize_categories
from app.policy_models import PolicyAssertion, PolicySource, PolicySourceVersion
from app.services.policy_rule_normalizer import normalize_rule_candidate


DEFAULT_STALE_AFTER_DAYS = 90


def _utcnow() -> datetime:
    return datetime.utcnow()


def _dumps(v: Any) -> str:
    try:
        return json.dumps(v, ensure_ascii=False, sort_keys=True, default=str)
    except Exception:
        return "{}"


def _norm_state(value: Optional[str]) -> str:
    return (value or "MI").strip().upper()


def _norm_lower(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    out = value.strip().lower()
    return out or None


def _norm_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    out = value.strip()
    return out or None


def _rule_family_for(rule_key: str) -> str:
    mapping = {
        "document_reference": "document_reference",
        "federal_hcv_regulations_anchor": "federal_hcv",
        "federal_nspire_anchor": "federal_nspire",
        "federal_notice_anchor": "federal_notice",
        "mi_statute_anchor": "mi_landlord_tenant",
        "mshda_program_anchor": "mshda_program",
        "pha_admin_plan_anchor": "pha_admin_plan",
        "pha_administrator_changed": "pha_admin_transfer",
        "pha_landlord_packet_required": "pha_landlord_workflow",
        "hap_contract_and_tenancy_addendum_required": "voucher_lease_packet",
        "landlord_payment_timing_reference": "landlord_payment_timing",
        "rental_registration_required": "rental_registration",
        "rental_license_required": "rental_license",
        "certificate_required_before_occupancy": "certificate_before_occupancy",
        "certificate_of_occupancy_required": "certificate_before_occupancy",
        "certificate_of_compliance_required": "certificate_before_occupancy",
        "inspection_program_exists": "inspection_program",
        "fire_safety_inspection_required": "inspection_program",
        "property_maintenance_enforcement_anchor": "property_maintenance",
        "building_safety_division_anchor": "building_safety",
        "building_division_anchor": "building_division",
        "lead_paint_affidavit_required": "lead",
        "lead_clearance_required": "lead",
        "lead_inspection_required": "lead",
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
        "rental_license_required",
        "certificate_required_before_occupancy",
        "certificate_of_occupancy_required",
        "certificate_of_compliance_required",
        "inspection_program_exists",
        "fire_safety_inspection_required",
        "hap_contract_and_tenancy_addendum_required",
        "pha_landlord_packet_required",
        "lead_clearance_required",
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


def _source_version_for_source(db: Session, source_id: int) -> PolicySourceVersion | None:
    return db.scalar(
        select(PolicySourceVersion)
        .where(PolicySourceVersion.source_id == int(source_id))
        .order_by(PolicySourceVersion.retrieved_at.desc(), PolicySourceVersion.id.desc())
    )


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

    refresh_days = int(getattr(source, "refresh_interval_days", 30) or 30)
    return now + timedelta(days=max(30, refresh_days, DEFAULT_STALE_AFTER_DAYS))


def _normalized_category_for(rule_key: str) -> str | None:
    mapping = {
        "document_reference": None,
        "federal_hcv_regulations_anchor": "section8",
        "federal_nspire_anchor": "inspection",
        "federal_notice_anchor": "section8",
        "mi_statute_anchor": "safety",
        "mshda_program_anchor": "section8",
        "pha_admin_plan_anchor": "section8",
        "pha_administrator_changed": "section8",
        "pha_landlord_packet_required": "section8",
        "hap_contract_and_tenancy_addendum_required": "section8",
        "landlord_payment_timing_reference": "section8",
        "rental_registration_required": "registration",
        "rental_license_required": "registration",
        "certificate_required_before_occupancy": "occupancy",
        "certificate_of_occupancy_required": "occupancy",
        "certificate_of_compliance_required": "occupancy",
        "inspection_program_exists": "inspection",
        "fire_safety_inspection_required": "inspection",
        "property_maintenance_enforcement_anchor": "safety",
        "building_safety_division_anchor": "safety",
        "building_division_anchor": "permits",
        "lead_paint_affidavit_required": "lead",
        "lead_clearance_required": "lead",
        "lead_inspection_required": "lead",
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
    source.last_fetched_at = getattr(source, "last_fetched_at", None) or now

    status_ok = source.http_status is not None and 200 <= int(source.http_status) < 400
    if not status_ok:
        source.freshness_status = "fetch_failed"
        source.freshness_reason = "http_status_not_successful"
    elif not source.retrieved_at:
        source.freshness_status = "unknown"
        source.freshness_reason = "missing_retrieved_at"
    elif source.retrieved_at < (
        now - timedelta(days=max(30, int(getattr(source, "refresh_interval_days", 30) or 30) * 2))
    ):
        source.freshness_status = "stale"
        source.freshness_reason = "retrieved_at_older_than_refresh_window"
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


def _already_added(created: list[PolicyAssertion], rule_key: str, raw_excerpt: Optional[str] = None) -> bool:
    for row in created:
        if row.rule_key != rule_key:
            continue
        if raw_excerpt is None:
            return True
        if (getattr(row, "raw_excerpt", None) or "").strip() == raw_excerpt.strip():
            return True
    return False


def _existing_candidate_for_source(
    db: Session,
    *,
    source_id: int,
    org_id: Optional[int],
    version_group: Optional[str],
    raw_excerpt: Optional[str],
    rule_key: str,
) -> PolicyAssertion | None:
    stmt = select(PolicyAssertion).where(
        PolicyAssertion.source_id == int(source_id),
        PolicyAssertion.rule_key == rule_key,
    )
    if org_id is None:
        stmt = stmt.where(PolicyAssertion.org_id.is_(None))
    else:
        stmt = stmt.where(PolicyAssertion.org_id == int(org_id))

    rows = list(db.scalars(stmt).all())
    excerpt = (raw_excerpt or "").strip()
    for row in rows:
        if version_group and (getattr(row, "version_group", None) or "") != version_group:
            continue
        if excerpt and (getattr(row, "raw_excerpt", None) or "").strip() == excerpt:
            return row
    for row in rows:
        if version_group and (getattr(row, "version_group", None) or "") == version_group:
            return row
    return rows[0] if rows else None


def _candidate_payload(
    *,
    source: PolicySource,
    rule_key: str,
    value: dict[str, Any],
    confidence: float,
) -> dict[str, Any]:
    raw_text = value.get("summary") or value.get("text") or value.get("condition") or rule_key
    candidate = normalize_rule_candidate(
        {
            "rule_key": rule_key,
            "title": source.title,
            "publisher": source.publisher,
            "url": source.url,
            "text": str(raw_text),
            "raw_excerpt": str(raw_text),
            "source_level": getattr(source, "source_type", None) or "local",
            "property_type": _norm_text(getattr(source, "program_type", None)),
            "normalized_version": "v2",
            "confidence": confidence,
        }
    )
    if candidate is None:
        source_level = getattr(source, "source_type", None) or "local"
        property_type = _norm_text(getattr(source, "program_type", None))
        version_group = f"{source_level}:{rule_key}:{property_type or 'all'}"
        return {
            "rule_family": _rule_family_for(rule_key),
            "rule_category": _normalized_category_for(rule_key),
            "source_level": source_level,
            "property_type": property_type,
            "required": True,
            "blocking": rule_key in {
                "rental_registration_required",
                "rental_license_required",
                "certificate_required_before_occupancy",
                "certificate_of_occupancy_required",
                "certificate_of_compliance_required",
                "inspection_program_exists",
                "fire_safety_inspection_required",
                "pha_landlord_packet_required",
                "hap_contract_and_tenancy_addendum_required",
                "lead_clearance_required",
            },
            "source_citation": source.url,
            "raw_excerpt": str(raw_text),
            "normalized_version": "v2",
            "version_group": version_group,
            "confidence": confidence,
        }

    return {
        "rule_family": candidate.rule_family,
        "rule_category": candidate.rule_category,
        "source_level": candidate.source_level,
        "property_type": candidate.property_type,
        "required": bool(candidate.required),
        "blocking": bool(candidate.blocking),
        "source_citation": candidate.source_citation or source.url,
        "raw_excerpt": candidate.raw_excerpt or str(raw_text),
        "normalized_version": candidate.normalized_version,
        "version_group": candidate.version_group,
        "confidence": max(float(candidate.confidence), float(confidence)),
    }


def _add_assertion(
    created: list[PolicyAssertion],
    *,
    target_org_id: Optional[int],
    source: PolicySource,
    source_version_id: Optional[int],
    now: datetime,
    rule_key: str,
    value: dict[str, Any],
    confidence: float,
) -> None:
    payload = _candidate_payload(
        source=source,
        rule_key=rule_key,
        value=value,
        confidence=confidence,
    )
    if _already_added(created, rule_key, payload.get("raw_excerpt")):
        return

    created.append(
        PolicyAssertion(
            org_id=target_org_id,
            source_id=source.id,
            source_version_id=source_version_id,
            state=_norm_state(source.state),
            county=_norm_lower(source.county),
            city=_norm_lower(source.city),
            pha_name=_norm_text(source.pha_name),
            program_type=_norm_text(source.program_type),
            rule_key=rule_key,
            rule_family=payload["rule_family"],
            assertion_type=_assertion_type_for(rule_key),
            value_json=_dumps(value),
            effective_date=getattr(source, "effective_date", None),
            expires_at=None,
            confidence=float(payload["confidence"]),
            priority=_priority_for(rule_key),
            source_rank=_source_rank_for(source),
            review_status="extracted",
            review_notes="auto_extracted_from_source_refresh",
            reviewed_by_user_id=None,
            verification_reason=None,
            stale_after=_stale_after_for(rule_key, source, now),
            superseded_by_assertion_id=None,
            replaced_by_assertion_id=None,
            jurisdiction_slug=getattr(source, "jurisdiction_slug", None),
            source_level=payload["source_level"],
            property_type=payload["property_type"],
            rule_category=payload["rule_category"],
            required=bool(payload["required"]),
            blocking=bool(payload["blocking"]),
            source_citation=payload["source_citation"],
            raw_excerpt=payload["raw_excerpt"],
            normalized_version=payload["normalized_version"],
            rule_status="candidate",
            governance_state="draft",
            version_group=payload["version_group"],
            version_number=1,
            is_current=False,
            citation_json=_dumps(
                {
                    "url": source.url,
                    "publisher": source.publisher,
                    "title": source.title,
                    "source_version_id": source_version_id,
                }
            ),
            rule_provenance_json=_dumps(
                {
                    "source_id": int(source.id),
                    "source_version_id": source_version_id,
                    "source_type": getattr(source, "source_type", None),
                    "jurisdiction_slug": getattr(source, "jurisdiction_slug", None),
                }
            ),
            value_hash=None,
            confidence_basis="inferred",
            change_summary="extracted_from_source_refresh",
            normalized_category=payload["rule_category"],
            coverage_status=_coverage_status_for(rule_key),
            source_freshness_status=getattr(source, "freshness_status", None),
            extracted_at=now,
            reviewed_at=None,
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
    - preserve active/replaced assertions
    - refresh extracted/draft candidates for this exact source + scope
    - inference is based on source metadata only (url/title/publisher/notes/text)
    - attach normalized_category + governance/provenance fields for downstream review
    """
    target_org_id = org_id if org_scope else None
    now = _utcnow()
    created: list[PolicyAssertion] = []
    source_version = _source_version_for_source(db, int(source.id))

    q = db.query(PolicyAssertion).filter(PolicyAssertion.source_id == source.id)
    if target_org_id is None:
        q = q.filter(PolicyAssertion.org_id.is_(None))
    else:
        q = q.filter(PolicyAssertion.org_id == target_org_id)
    q = q.filter(
        or_(
            PolicyAssertion.review_status == "extracted",
            PolicyAssertion.governance_state == "draft",
            PolicyAssertion.rule_status == "candidate",
        )
    )
    q.delete(synchronize_session=False)
    db.commit()

    url = (source.url or "").lower()
    text = _haystack(source)

    _add_assertion(
        created,
        target_org_id=target_org_id,
        source=source,
        source_version_id=int(source_version.id) if source_version is not None else None,
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
            source_version_id=int(source_version.id) if source_version is not None else None,
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
            source_version_id=int(source_version.id) if source_version is not None else None,
            now=now,
            rule_key="federal_nspire_anchor",
            value={
                "summary": "HUD program requirements and inspection standards are reflected in 24 CFR Part 5 / NSPIRE structure.",
                "url": source.url,
            },
            confidence=0.75,
        )

    if "federalregister.gov" in url and "nspire" in text:
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            source_version_id=int(source_version.id) if source_version is not None else None,
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
            source_version_id=int(source_version.id) if source_version is not None else None,
            now=now,
            rule_key="mi_statute_anchor",
            value={
                "summary": "Michigan statutory landlord-tenant baseline source.",
                "url": source.url,
            },
            confidence=0.70,
        )

    if "michigan.gov/mshda" in url or "mshda" in text:
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            source_version_id=int(source_version.id) if source_version is not None else None,
            now=now,
            rule_key="mshda_program_anchor",
            value={
                "summary": "MSHDA program guidance relevant to state housing program administration.",
                "url": source.url,
            },
            confidence=0.70,
        )

    # Detroit
    if "detroitmi.gov" in url:
        if _has_any(text, ["landlord-rental", "tenant-rental-property", "rental requirements faq"]):
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                source_version_id=int(source_version.id) if source_version is not None else None,
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
                source_version_id=int(source_version.id) if source_version is not None else None,
                now=now,
                rule_key="certificate_required_before_occupancy",
                value={
                    "summary": "Detroit certificate of compliance / rental certificate process is relevant before compliant operation.",
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
                source_version_id=int(source_version.id) if source_version is not None else None,
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
            source_version_id=int(source_version.id) if source_version is not None else None,
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
            source_version_id=int(source_version.id) if source_version is not None else None,
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
            source_version_id=int(source_version.id) if source_version is not None else None,
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
                source_version_id=int(source_version.id) if source_version is not None else None,
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
                source_version_id=int(source_version.id) if source_version is not None else None,
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
                source_version_id=int(source_version.id) if source_version is not None else None,
                now=now,
                rule_key="property_maintenance_enforcement_anchor",
                value={
                    "summary": "Warren property maintenance division is a local enforcement anchor.",
                    "url": source.url,
                    "scope_hint": "city",
                },
                confidence=0.75,
            )

        if "building division" in text:
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                source_version_id=int(source_version.id) if source_version is not None else None,
                now=now,
                rule_key="building_division_anchor",
                value={
                    "summary": "Warren building division is a local permit/compliance anchor.",
                    "url": source.url,
                    "scope_hint": "city",
                },
                confidence=0.70,
            )

    # PHA / program
    if _has_any(text, ["administrative plan", "admin plan"]):
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            source_version_id=int(source_version.id) if source_version is not None else None,
            now=now,
            rule_key="pha_admin_plan_anchor",
            value={
                "summary": "PHA administrative plan is an authoritative local program anchor.",
                "url": source.url,
            },
            confidence=0.85,
        )

    if _has_any(text, ["landlord packet"]):
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            source_version_id=int(source_version.id) if source_version is not None else None,
            now=now,
            rule_key="pha_landlord_packet_required",
            value={
                "summary": "Landlord packet appears required for voucher landlord onboarding/approval.",
                "url": source.url,
            },
            confidence=0.72,
        )

    if _has_any(text, ["hap contract", "tenancy addendum"]):
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            source_version_id=int(source_version.id) if source_version is not None else None,
            now=now,
            rule_key="hap_contract_and_tenancy_addendum_required",
            value={
                "summary": "HAP contract and/or tenancy addendum appears required for voucher execution.",
                "url": source.url,
            },
            confidence=0.80,
        )

    if _has_any(text, ["payment timing", "housing assistance payment", "landlord payment"]):
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            source_version_id=int(source_version.id) if source_version is not None else None,
            now=now,
            rule_key="landlord_payment_timing_reference",
            value={
                "summary": "Source contains timing/reference guidance for landlord payment processing.",
                "url": source.url,
            },
            confidence=0.65,
        )

    if _has_any(text, ["administrator changed", "new pha administrator", "program administrator"]):
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            source_version_id=int(source_version.id) if source_version is not None else None,
            now=now,
            rule_key="pha_administrator_changed",
            value={
                "summary": "Source indicates a change to PHA/program administrator handling.",
                "url": source.url,
            },
            confidence=0.70,
        )

    if _has_any(text, ["lead affidavit", "lead paint affidavit"]):
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            source_version_id=int(source_version.id) if source_version is not None else None,
            now=now,
            rule_key="lead_paint_affidavit_required",
            value={
                "summary": "Lead paint affidavit requirement appears in local or program source.",
                "url": source.url,
            },
            confidence=0.72,
        )

    if _has_any(text, ["lead clearance"]):
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            source_version_id=int(source_version.id) if source_version is not None else None,
            now=now,
            rule_key="lead_clearance_required",
            value={
                "summary": "Lead clearance requirement appears in source.",
                "url": source.url,
            },
            confidence=0.75,
        )

    if _has_any(text, ["lead inspection"]):
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            source_version_id=int(source_version.id) if source_version is not None else None,
            now=now,
            rule_key="lead_inspection_required",
            value={
                "summary": "Lead inspection requirement appears in source.",
                "url": source.url,
            },
            confidence=0.70,
        )

    persisted: list[PolicyAssertion] = []
    for item in created:
        existing = _existing_candidate_for_source(
            db,
            source_id=int(source.id),
            org_id=target_org_id,
            version_group=getattr(item, "version_group", None),
            raw_excerpt=getattr(item, "raw_excerpt", None),
            rule_key=item.rule_key,
        )
        if existing is not None:
            existing.rule_family = item.rule_family
            existing.assertion_type = item.assertion_type
            existing.value_json = item.value_json
            existing.effective_date = item.effective_date
            existing.expires_at = item.expires_at
            existing.confidence = item.confidence
            existing.priority = item.priority
            existing.source_rank = item.source_rank
            existing.review_status = "extracted"
            existing.review_notes = "auto_extracted_from_source_refresh"
            existing.stale_after = item.stale_after
            existing.jurisdiction_slug = item.jurisdiction_slug
            existing.source_level = item.source_level
            existing.property_type = item.property_type
            existing.rule_category = item.rule_category
            existing.required = item.required
            existing.blocking = item.blocking
            existing.source_citation = item.source_citation
            existing.raw_excerpt = item.raw_excerpt
            existing.normalized_version = item.normalized_version
            existing.rule_status = "candidate"
            existing.governance_state = "draft"
            existing.normalized_category = item.normalized_category
            existing.coverage_status = item.coverage_status
            existing.source_freshness_status = item.source_freshness_status
            existing.extracted_at = now
            existing.source_version_id = int(source_version.id) if source_version is not None else getattr(existing, "source_version_id", None)
            existing.citation_json = item.citation_json
            existing.rule_provenance_json = item.rule_provenance_json
            existing.is_current = False
            db.add(existing)
            persisted.append(existing)
        else:
            db.add(item)
            persisted.append(item)

    _refresh_source_category_metadata(source, persisted, now)
    db.add(source)
    db.commit()

    for row in persisted:
        if getattr(row, "id", None) is not None:
            db.refresh(row)

    return persisted


def mark_assertions_stale_for_source(
    db: Session,
    *,
    source_id: int,
    reason: str = "source_changed",
) -> dict[str, Any]:
    rows = list(
        db.scalars(
            select(PolicyAssertion).where(PolicyAssertion.source_id == int(source_id))
        ).all()
    )

    stale_ids: list[int] = []
    for row in rows:
        if (row.governance_state or "").lower() in {"active", "approved", "replaced"}:
            continue
        row.review_status = "stale"
        row.rule_status = "stale"
        row.coverage_status = "stale"
        row.is_current = False
        row.change_summary = reason
        db.add(row)
        stale_ids.append(int(row.id))

    db.commit()
    return {
        "ok": True,
        "source_id": int(source_id),
        "stale_count": len(stale_ids),
        "stale_ids": stale_ids,
        "reason": reason,
    }