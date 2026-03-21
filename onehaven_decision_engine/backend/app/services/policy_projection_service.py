from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.domain.jurisdiction_categories import normalize_categories
from app.policy_models import JurisdictionProfile, PolicyAssertion, PolicySource
from app.services.jurisdiction_completeness_service import recompute_profile_and_coverage
from app.services.policy_coverage_service import compute_coverage_status


def _dumps(v: Any) -> str:
    try:
        return json.dumps(v, ensure_ascii=False, sort_keys=True)
    except Exception:
        return "{}"


def _loads(s: Optional[str], default: Any = None) -> Any:
    if default is None:
        default = {}
    if not s:
        return default
    try:
        return json.loads(s)
    except Exception:
        return default


def _norm_state(s: Optional[str]) -> str:
    return (s or "MI").strip().upper()


def _norm_lower(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    v = s.strip().lower()
    return v or None


def _rule_family(rule_key: str) -> str:
    explicit = {
        "rental_registration_required": "rental_registration",
        "inspection_program_exists": "inspection_requirement",
        "certificate_required_before_occupancy": "occupancy_certificate",
        "building_safety_division_anchor": "building_enforcement",
        "building_division_anchor": "building_enforcement",
        "property_maintenance_enforcement_anchor": "building_enforcement",
        "pha_admin_plan_anchor": "pha_administration",
        "pha_administrator_changed": "pha_administration",
        "pha_landlord_packet_required": "pha_landlord_workflow",
        "hap_contract_and_tenancy_addendum_required": "voucher_contract_workflow",
        "federal_hcv_regulations_anchor": "federal_hcv",
        "federal_nspire_anchor": "federal_nspire",
        "mi_statute_anchor": "mi_landlord_tenant",
        "mshda_program_anchor": "mshda_program",
        "document_reference": "document_reference",
    }
    return explicit.get(rule_key, rule_key)


def _assertion_type_rank(v: str) -> int:
    order = {
        "operational": 10,
        "anchor": 20,
        "superseding_notice": 30,
        "document_reference": 100,
    }
    return order.get(v or "", 50)


def _specificity_score(
    a: PolicyAssertion,
    target_county: Optional[str],
    target_city: Optional[str],
    target_pha_name: Optional[str],
) -> int:
    score = 0
    if target_county and a.county == target_county:
        score += 10
    if target_city and a.city == target_city:
        score += 20
    if target_pha_name and a.pha_name == target_pha_name:
        score += 25
    return score


def _source_info_map(
    db: Session, assertions: list[PolicyAssertion]
) -> dict[int, PolicySource]:
    ids = sorted({a.source_id for a in assertions if a.source_id is not None})
    if not ids:
        return {}
    rows = db.query(PolicySource).filter(PolicySource.id.in_(ids)).all()
    return {r.id: r for r in rows}


def _winner_key(
    a: PolicyAssertion,
    *,
    source_map: dict[int, PolicySource],
    target_county: Optional[str],
    target_city: Optional[str],
    target_pha_name: Optional[str],
) -> tuple:
    src = source_map.get(a.source_id) if a.source_id is not None else None
    retrieved = src.retrieved_at if src else datetime(1970, 1, 1)
    return (
        -_specificity_score(a, target_county, target_city, target_pha_name),
        0 if a.review_status == "verified" else 1,
        _assertion_type_rank(a.assertion_type or ""),
        a.source_rank or 100,
        -(a.confidence or 0.0),
        -(a.priority or 100),
        -int(retrieved.timestamp()),
        -(a.id or 0),
    )


def _pick_winners(
    db: Session,
    assertions: list[PolicyAssertion],
    *,
    target_county: Optional[str],
    target_city: Optional[str],
    target_pha_name: Optional[str],
) -> list[PolicyAssertion]:
    source_map = _source_info_map(db, assertions)

    families: dict[str, list[PolicyAssertion]] = {}
    for a in assertions:
        fam = a.rule_family or _rule_family(a.rule_key)
        families.setdefault(fam, []).append(a)

    winners: list[PolicyAssertion] = []
    for _, group in families.items():
        group_sorted = sorted(
            group,
            key=lambda a: _winner_key(
                a,
                source_map=source_map,
                target_county=target_county,
                target_city=target_city,
                target_pha_name=target_pha_name,
            ),
        )
        winners.append(group_sorted[0])

    winners.sort(key=lambda a: ((a.rule_family or _rule_family(a.rule_key)), a.rule_key))
    return winners


def _score_friction(assertions: list[PolicyAssertion]) -> float:
    friction = 1.0
    keys = {a.rule_key for a in assertions}

    if "rental_registration_required" in keys:
        friction += 0.08
    if "inspection_program_exists" in keys:
        friction += 0.10
    if "certificate_required_before_occupancy" in keys:
        friction += 0.12
    if "pha_landlord_packet_required" in keys:
        friction += 0.05
    if "hap_contract_and_tenancy_addendum_required" in keys:
        friction += 0.05
    if "pha_administrator_changed" in keys:
        friction += 0.06

    return round(min(friction, 2.0), 2)


def _actions_and_blockers(assertions: list[PolicyAssertion]) -> tuple[list[dict], list[dict]]:
    actions: list[dict] = []
    blockers: list[dict] = []

    keys = {a.rule_key for a in assertions}

    if "rental_registration_required" in keys:
        actions.append(
            {
                "key": "register_rental_property",
                "title": "Register rental property with the local authority",
                "category": "municipal_registration",
                "severity": "required",
                "normalized_category": "registration",
            }
        )
    if "inspection_program_exists" in keys:
        actions.append(
            {
                "key": "schedule_initial_inspection",
                "title": "Schedule initial rental inspection",
                "category": "municipal_inspection",
                "severity": "required",
                "normalized_category": "inspection",
            }
        )
    cert_rows = [a for a in assertions if a.rule_key == "certificate_required_before_occupancy"]
    if cert_rows:
        cert_statuses = []
        for a in cert_rows:
            value = _loads(a.value_json, {})
            cert_statuses.append(str(value.get("status") or "yes").strip().lower())

        if "yes" in cert_statuses:
            actions.append(
                {
                    "key": "obtain_certificate_before_occupancy",
                    "title": "Obtain required certificate/compliance approval before occupancy",
                    "category": "municipal_certificate",
                    "severity": "required",
                    "normalized_category": "occupancy",
                }
            )
            blockers.append(
                {
                    "key": "certificate_pre_lease_blocker",
                    "title": "Do not move to lease-ready until certificate/compliance requirement is satisfied",
                    "category": "municipal_certificate",
                    "severity": "blocking",
                    "normalized_category": "occupancy",
                }
            )
        elif "conditional" in cert_statuses:
            actions.append(
                {
                    "key": "check_certificate_before_occupancy_conditions",
                    "title": "Check whether certificate/city certification is required before occupancy",
                    "category": "municipal_certificate",
                    "severity": "required_if_applicable",
                    "normalized_category": "occupancy",
                }
            )

    if "hap_contract_and_tenancy_addendum_required" in keys:
        actions.append(
            {
                "key": "prepare_hap_and_addendum",
                "title": "Prepare HAP contract and tenancy addendum if voucher strategy is used",
                "category": "voucher_workflow",
                "severity": "required_if_voucher",
                "normalized_category": "section8",
            }
        )
    if "pha_landlord_packet_required" in keys:
        actions.append(
            {
                "key": "prepare_pha_landlord_packet",
                "title": "Prepare required landlord packet / PHA paperwork",
                "category": "pha_workflow",
                "severity": "required_if_voucher",
                "normalized_category": "section8",
            }
        )
    if "pha_administrator_changed" in keys:
        blockers.append(
            {
                "key": "confirm_current_pha_admin",
                "title": "Confirm current PHA administrator before relying on older process documents",
                "category": "pha_workflow",
                "severity": "warning",
                "normalized_category": "section8",
            }
        )

    return actions, blockers


def _query_inherited_assertions(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
    statuses: Optional[list[str]] = None,
) -> list[PolicyAssertion]:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = pha_name.strip() if pha_name else None

    q = db.query(PolicyAssertion).filter(PolicyAssertion.state == st)

    if org_id is None:
        q = q.filter(PolicyAssertion.org_id.is_(None))
    else:
        q = q.filter((PolicyAssertion.org_id == org_id) | (PolicyAssertion.org_id.is_(None)))

    if statuses:
        q = q.filter(PolicyAssertion.review_status.in_(statuses))

    rows = q.all()

    out: list[PolicyAssertion] = []
    for a in rows:
        if a.city is not None and cty != a.city:
            continue
        if a.county is not None and cnty != a.county:
            continue
        if a.pha_name is not None and pha != a.pha_name:
            continue
        out.append(a)

    return out


def _query_local_assertions_any_status(
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
    pha = pha_name.strip() if pha_name else None

    q = db.query(PolicyAssertion).filter(PolicyAssertion.state == st)

    if org_id is None:
        q = q.filter(PolicyAssertion.org_id.is_(None))
    else:
        q = q.filter((PolicyAssertion.org_id == org_id) | (PolicyAssertion.org_id.is_(None)))

    if cnty is None:
        q = q.filter(PolicyAssertion.county.is_(None))
    else:
        q = q.filter(PolicyAssertion.county == cnty)

    if cty is None:
        q = q.filter(PolicyAssertion.city.is_(None))
    else:
        q = q.filter(PolicyAssertion.city == cty)

    if pha is not None:
        q = q.filter((PolicyAssertion.pha_name == pha) | (PolicyAssertion.pha_name.is_(None)))

    return q.all()


def _compute_local_rule_statuses(local_assertions: list[PolicyAssertion]) -> dict[str, str]:
    core = [
        "rental_registration_required",
        "inspection_program_exists",
        "certificate_required_before_occupancy",
    ]
    out: dict[str, str] = {}

    for rule_key in core:
        rows = [a for a in local_assertions if a.rule_key == rule_key]
        if not rows:
            out[rule_key] = "unknown"
            continue

        verified_rows = [a for a in rows if a.review_status == "verified"]
        if verified_rows:
            statuses: list[str] = []
            for a in verified_rows:
                value = _loads(a.value_json, {})
                explicit_status = str(value.get("status") or "").strip().lower()
                if explicit_status in {"yes", "no", "conditional"}:
                    statuses.append(explicit_status)
                else:
                    statuses.append("yes")

            if "yes" in statuses:
                out[rule_key] = "yes"
            elif "conditional" in statuses:
                out[rule_key] = "conditional"
            elif "no" in statuses:
                out[rule_key] = "no"
            else:
                out[rule_key] = "unknown"
        else:
            out[rule_key] = "unknown"

    return out


def build_policy_summary(
    db: Session,
    assertions: list[PolicyAssertion],
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> dict[str, Any]:
    winners = _pick_winners(
        db,
        assertions,
        target_county=county,
        target_city=city,
        target_pha_name=pha_name,
    )

    source_map = _source_info_map(db, winners)
    actions, blockers = _actions_and_blockers(winners)
    coverage = compute_coverage_status(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )

    local_all = _query_local_assertions_any_status(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )

    local_verified = [a for a in local_all if a.review_status == "verified"]
    local_rule_statuses = _compute_local_rule_statuses(local_all)

    verified_rules: list[dict[str, Any]] = []
    evidence_links: list[dict[str, Any]] = []
    source_ids: list[int] = []
    seen_source_ids: set[int] = set()

    for a in winners:
        value = _loads(a.value_json, {})
        fam = a.rule_family or _rule_family(a.rule_key)
        src = source_map.get(a.source_id) if a.source_id is not None else None
        if a.source_id is not None and a.source_id not in source_ids:
            source_ids.append(a.source_id)

        verified_rules.append(
            {
                "id": a.id,
                "rule_key": a.rule_key,
                "rule_family": fam,
                "assertion_type": a.assertion_type,
                "normalized_category": a.normalized_category,
                "coverage_status": a.coverage_status,
                "value": value,
                "confidence": a.confidence,
                "review_status": a.review_status,
                "source_id": a.source_id,
            }
        )

        if src is not None:
            status_ok = src.http_status is not None and 200 <= int(src.http_status) < 400
            if status_ok and src.id not in seen_source_ids:
                evidence_links.append(
                    {
                        "source_id": src.id,
                        "publisher": src.publisher,
                        "title": src.title,
                        "url": src.url,
                        "retrieved_at": src.retrieved_at.isoformat() if src.retrieved_at else None,
                        "http_status": src.http_status,
                        "freshness_status": getattr(src, "freshness_status", None),
                    }
                )
                seen_source_ids.add(src.id)

    normalized_categories = normalize_categories(
        [r.get("normalized_category") for r in verified_rules if r.get("normalized_category")]
    )

    return {
        "summary": "Built from verified policy assertions with conflict resolution and inheritance.",
        "market": {
            "state": state,
            "county": county,
            "city": city,
            "pha_name": pha_name,
        },
        "coverage": coverage,
        "verified_rule_count_effective": len(verified_rules),
        "verified_rule_count_local": len(local_verified),
        "local_rule_statuses": local_rule_statuses,
        "verified_rules": verified_rules,
        "required_actions": actions,
        "blocking_items": blockers,
        "evidence_links": evidence_links,
        "source_ids": source_ids,
        "normalized_categories": normalized_categories,
    }


def project_verified_assertions_to_profile(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    notes: Optional[str] = None,
) -> JurisdictionProfile:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = pha_name.strip() if pha_name else None

    assertions = _query_inherited_assertions(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        statuses=["verified"],
    )

    policy_json = build_policy_summary(
        db,
        assertions,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )
    friction = _score_friction(assertions)
    covered_categories = normalize_categories(policy_json.get("normalized_categories") or [])

    existing = (
        db.query(JurisdictionProfile)
        .filter(JurisdictionProfile.state == st)
        .filter(JurisdictionProfile.county == cnty)
        .filter(JurisdictionProfile.city == cty)
        .filter(
            JurisdictionProfile.org_id.is_(None)
            if org_id is None
            else JurisdictionProfile.org_id == org_id
        )
        .first()
    )

    now = datetime.utcnow()

    if existing is None:
        row = JurisdictionProfile(
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            friction_multiplier=friction,
            pha_name=pha,
            policy_json=_dumps(policy_json),
            covered_categories_json=_dumps(covered_categories),
            notes=notes or "Projected from verified policy assertions.",
            updated_at=now,
        )
        db.add(row)
        db.flush()
    else:
        row = existing
        row.friction_multiplier = friction
        row.pha_name = pha or row.pha_name
        row.policy_json = _dumps(policy_json)
        row.covered_categories_json = _dumps(covered_categories)
        row.notes = notes or row.notes or "Projected from verified policy assertions."
        row.updated_at = now

    db.flush()
    recompute_profile_and_coverage(db, row, commit=False)
    db.commit()
    db.refresh(row)
    return row


def build_property_compliance_brief(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
) -> dict[str, Any]:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = pha_name.strip() if pha_name else None

    assertions = _query_inherited_assertions(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
        statuses=["verified"],
    )

    summary = build_policy_summary(
        db,
        assertions,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )

    local_statuses = summary["local_rule_statuses"]

    market_parts = []
    if city:
        market_parts.append(city.title())
    if county:
        market_parts.append(f"{county.title()} County")
    market_parts.append(st)

    compliance = {
        "market_label": ", ".join(market_parts),
        "registration_required": local_statuses.get("rental_registration_required", "unknown"),
        "inspection_required": local_statuses.get("inspection_program_exists", "unknown"),
        "certificate_required_before_occupancy": local_statuses.get("certificate_required_before_occupancy", "unknown"),
        "pha_specific_workflow": any(
            r["rule_key"] in {
                "pha_admin_plan_anchor",
                "pha_administrator_changed",
                "pha_landlord_packet_required",
                "hap_contract_and_tenancy_addendum_required",
            }
            for r in summary["verified_rules"]
        ),
        "coverage_confidence": summary["coverage"]["confidence_label"],
        "production_readiness": summary["coverage"]["production_readiness"],
        "completeness_status": summary["coverage"]["completeness_status"],
        "missing_categories": summary["coverage"]["missing_categories"],
        "is_stale": summary["coverage"]["is_stale"],
    }

    explanation_parts = []

    if compliance["registration_required"] == "yes":
        explanation_parts.append("local rental registration appears required")
    elif compliance["registration_required"] == "conditional":
        explanation_parts.append("local rental registration is conditionally required")
    elif compliance["registration_required"] == "unknown":
        explanation_parts.append("local rental registration is still under review")

    if compliance["inspection_required"] == "yes":
        explanation_parts.append("a local inspection workflow appears required")
    elif compliance["inspection_required"] == "conditional":
        explanation_parts.append("a local inspection workflow appears conditionally required")
    elif compliance["inspection_required"] == "unknown":
        explanation_parts.append("inspection requirements are still under review")

    if compliance["certificate_required_before_occupancy"] == "yes":
        explanation_parts.append("certificate/compliance approval appears required before occupancy")
    elif compliance["certificate_required_before_occupancy"] == "conditional":
        explanation_parts.append(
            "certificate/compliance approval is required in certain occupancy scenarios"
        )
    elif compliance["certificate_required_before_occupancy"] == "unknown":
        explanation_parts.append("certificate-before-occupancy requirements are still under review")

    if compliance["pha_specific_workflow"]:
        explanation_parts.append("voucher / PHA workflow requirements may also apply")

    if compliance["missing_categories"]:
        explanation_parts.append(
            "jurisdiction coverage is still missing "
            + ", ".join(compliance["missing_categories"]).replace("_", " ")
        )

    if compliance["is_stale"]:
        explanation_parts.append("some policy source evidence is stale and should be refreshed")

    explanation = (
        f"This property resolves to {compliance['market_label']}. "
        + (
            "Current verified and inherited rules indicate " + ", ".join(explanation_parts) + "."
            if explanation_parts
            else "Verified market-specific compliance rules are still limited."
        )
        + f" Coverage confidence is {compliance['coverage_confidence']}; production readiness is {compliance['production_readiness']}."
    )

    return {
        "ok": True,
        "market": {
            "state": st,
            "county": cnty,
            "city": cty,
            "pha_name": pha,
        },
        "compliance": compliance,
        "explanation": explanation,
        "required_actions": summary["required_actions"],
        "blocking_items": summary["blocking_items"],
        "evidence_links": summary["evidence_links"],
        "coverage": summary["coverage"],
        "verified_rules": summary["verified_rules"],
        "local_rule_statuses": summary["local_rule_statuses"],
        "verified_rule_count_local": summary["verified_rule_count_local"],
        "verified_rule_count_effective": summary["verified_rule_count_effective"],
    }
