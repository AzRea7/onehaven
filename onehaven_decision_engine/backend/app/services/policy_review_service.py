from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from app.policy_models import PolicyAssertion, PolicySource

AUTO_VERIFY_RULE_KEYS = {
    "rental_registration_required",
    "inspection_program_exists",
    "certificate_required_before_occupancy",
    "property_maintenance_enforcement_anchor",
    "building_safety_division_anchor",
    "building_division_anchor",
    "pha_admin_plan_anchor",
    "pha_administrator_changed",
    "pha_landlord_packet_required",
    "hap_contract_and_tenancy_addendum_required",
    "federal_hcv_regulations_anchor",
    "federal_nspire_anchor",
    "federal_notice_anchor",
    "mi_statute_anchor",
    "mshda_program_anchor",
    "landlord_payment_timing_reference",
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


def _source_is_authoritative(src: Optional[PolicySource]) -> bool:
    if src is None:
        return False

    url = (src.url or "").lower()
    publisher = (src.publisher or "").lower()
    status_ok = src.http_status is not None and 200 <= int(src.http_status) < 400

    if not status_ok:
        return False

    authoritative_domains = (
        "ecfr.gov",
        "federalregister.gov",
        "hud.gov",
        "legislature.mi.gov",
        "michigan.gov",
        ".gov",
        "dhcmi.org",
        "cityofwarren.org",
        "cityofwestland.com",
        "cityoftaylor.com",
        "ci.taylor.mi.us",
        "livonia.gov",
        "pontiac.mi.us",
        "cityofsouthfield.com",
        "dearborn.gov",
        "detroitmi.gov",
    )

    if any(domain in url for domain in authoritative_domains):
        return True

    if any(
        phrase in publisher
        for phrase in (
            "city of",
            "michigan legislature",
            "mshda",
            "hud",
            "federal register",
            "detroit housing commission",
            "housing commission",
            "michigan courts",
        )
    ):
        return True

    return False


def _confidence_for(rule_key: str, src: Optional[PolicySource]) -> float:
    url = (src.url or "").lower() if src else ""

    if rule_key in {"federal_hcv_regulations_anchor", "federal_nspire_anchor"}:
        return 0.97
    if rule_key == "mi_statute_anchor":
        return 0.96
    if rule_key in {
        "rental_registration_required",
        "inspection_program_exists",
        "certificate_required_before_occupancy",
    }:
        if ".pdf" in url:
            return 0.94
        return 0.91
    if rule_key in {
        "property_maintenance_enforcement_anchor",
        "building_safety_division_anchor",
        "building_division_anchor",
        "mshda_program_anchor",
        "pha_admin_plan_anchor",
        "pha_administrator_changed",
        "pha_landlord_packet_required",
        "hap_contract_and_tenancy_addendum_required",
        "landlord_payment_timing_reference",
        "federal_notice_anchor",
    }:
        return 0.90

    return 0.88


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
    for a in rows:
        if a.county is not None and a.county != cnty:
            continue
        if a.city is not None and a.city != cty:
            continue
        if a.pha_name is not None and a.pha_name != pha:
            continue
        out.append(a)
    return out


def _source_map_for_assertions(
    db: Session,
    assertions: list[PolicyAssertion],
) -> dict[int, PolicySource]:
    source_ids = sorted({a.source_id for a in assertions if a.source_id is not None})
    if not source_ids:
        return {}
    rows = db.query(PolicySource).filter(PolicySource.id.in_(source_ids)).all()
    return {r.id: r for r in rows}


def auto_verify_market_assertions(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    reviewer_user_id: Optional[int] = None,
) -> dict:
    rows = _market_assertions(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )
    src_map = _source_map_for_assertions(db, rows)

    updated_ids: list[int] = []
    now = datetime.utcnow()

    for a in rows:
        if a.rule_key not in AUTO_VERIFY_RULE_KEYS:
            continue
        if a.review_status not in {"extracted", "needs_recheck"}:
            continue

        src = src_map.get(a.source_id) if a.source_id is not None else None
        if not _source_is_authoritative(src):
            continue

        a.review_status = "verified"
        a.confidence = max(float(a.confidence or 0.0), _confidence_for(a.rule_key, src))
        a.verification_reason = "official_source_review"
        a.reviewed_by_user_id = reviewer_user_id
        a.reviewed_at = now
        a.stale_after = None

        existing_note = (a.review_notes or "").strip()
        auto_note = f"Auto-verified from authoritative source: {src.title or src.url}" if src else "Auto-verified from authoritative source"
        a.review_notes = auto_note if not existing_note else existing_note

        updated_ids.append(a.id)

    db.commit()

    return {
        "updated_count": len(updated_ids),
        "updated_ids": updated_ids,
    }


def supersede_replaced_assertions(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    reviewer_user_id: Optional[int] = None,
) -> dict:
    rows = _market_assertions(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )

    by_rule: dict[str, list[PolicyAssertion]] = {}
    for a in rows:
        by_rule.setdefault(a.rule_key, []).append(a)

    superseded_ids: list[int] = []
    now = datetime.utcnow()

    for rule_key, group in by_rule.items():
        verified = [a for a in group if a.review_status == "verified"]
        if not verified:
            continue

        winner = sorted(
            verified,
            key=lambda x: (
                -(float(x.confidence or 0.0)),
                -(x.id or 0),
            ),
        )[0]

        for a in group:
            if a.id == winner.id:
                continue
            if a.review_status not in {"needs_recheck", "stale", "verified"}:
                continue

            if a.review_status == "verified":
                # keep parallel verified state/federal anchors if truly different sources/rules,
                # but supersede duplicate verified rows of the exact same rule for the same market
                if a.rule_key != winner.rule_key:
                    continue

            a.review_status = "superseded"
            a.superseded_by_assertion_id = winner.id
            a.reviewed_by_user_id = reviewer_user_id
            a.reviewed_at = now
            a.stale_after = None

            existing_note = (a.review_notes or "").strip()
            extra = f"Superseded by verified assertion {winner.id}"
            a.review_notes = extra if not existing_note else f"{existing_note} | {extra}"

            superseded_ids.append(a.id)

    db.commit()

    return {
        "superseded_count": len(superseded_ids),
        "superseded_ids": superseded_ids,
    }
