from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.domain.jurisdiction_categories import normalize_categories
from app.policy_models import (
    JurisdictionProfile,
    PolicyAssertion,
    PolicySource,
    PropertyComplianceProjection,
    PropertyComplianceProjectionItem,
)
from app.services.jurisdiction_completeness_service import recompute_profile_and_coverage
from app.services.policy_coverage_service import compute_coverage_status

try:
    from app.models import Property
except Exception:  # pragma: no cover
    Property = None  # type: ignore


def _dumps(v: Any) -> str:
    try:
        return json.dumps(v, ensure_ascii=False, sort_keys=True, default=str)
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


def _source_info_map(db: Session, assertions: list[PolicyAssertion]) -> dict[int, PolicySource]:
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
    retrieved = src.retrieved_at if src and src.retrieved_at is not None else datetime(1970, 1, 1)
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
                "required": bool(getattr(a, "required", True)),
                "blocking": bool(getattr(a, "blocking", False)),
                "source_level": getattr(a, "source_level", None),
                "property_type": getattr(a, "property_type", None),
                "program_type": getattr(a, "program_type", None),
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
        "required_categories": coverage.get("required_categories") or [],
        "category_coverage": coverage.get("category_coverage") or {},
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


# ---- Chunk 5 projection enrichments / Phase 4 property projection helpers ----

def _stable_projection_hash(payload: Any) -> str:
    try:
        raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    except Exception:
        raw = str(payload)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


def _layer_for_assertion(a: PolicyAssertion) -> str:
    source_level = str(getattr(a, "source_level", "") or "").strip().lower()
    if source_level == "federal":
        return "federal"
    if source_level == "state":
        return "state"
    if source_level == "county":
        return "county"
    if source_level == "city":
        return "city"
    if source_level == "program":
        return "program"
    if source_level == "property":
        return "property"
    if getattr(a, "org_id", None) is not None:
        return "property"
    if getattr(a, "pha_name", None):
        return "program"
    if getattr(a, "city", None):
        return "city"
    if getattr(a, "county", None):
        return "county"
    return "state"


def _source_evidence_rows(db: Session, assertions: list[PolicyAssertion]) -> list[dict[str, Any]]:
    source_map = _source_info_map(db, assertions)
    rows: list[dict[str, Any]] = []
    for a in assertions:
        src = source_map.get(a.source_id) if a.source_id is not None else None
        rows.append(
            {
                "assertion_id": int(a.id),
                "rule_key": a.rule_key,
                "rule_family": a.rule_family or _rule_family(a.rule_key),
                "layer": _layer_for_assertion(a),
                "source_id": a.source_id,
                "source_url": getattr(src, "url", None),
                "publisher": getattr(src, "publisher", None),
                "title": getattr(src, "title", None),
                "review_status": a.review_status,
                "confidence": float(a.confidence or 0.0),
                "freshness_status": getattr(src, "freshness_status", None),
            }
        )
    return rows


def _resolved_layer_summary(assertions: list[PolicyAssertion]) -> list[dict[str, Any]]:
    order = {
        "federal": 0,
        "state": 1,
        "county": 2,
        "city": 3,
        "program": 4,
        "property": 5,
    }
    bucket: dict[str, dict[str, Any]] = {}
    for a in assertions:
        layer = _layer_for_assertion(a)
        row = bucket.setdefault(layer, {"layer": layer, "assertion_count": 0, "rule_keys": []})
        row["assertion_count"] += 1
        if a.rule_key and a.rule_key not in row["rule_keys"]:
            row["rule_keys"].append(a.rule_key)
    return sorted(bucket.values(), key=lambda x: order.get(x["layer"], 99))


def _normalize_property_type(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "singlefamily": "single_family",
        "single_family_home": "single_family",
        "sfr": "single_family",
        "duplex": "multi_family",
        "triplex": "multi_family",
        "quadplex": "multi_family",
        "multifamily": "multi_family",
        "multi_family_home": "multi_family",
    }
    return aliases.get(text, text or None)


def _normalize_program_type(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().lower().replace("-", "_").replace(" ", "_")
    if not text:
        return None
    aliases = {
        "section8": "section8",
        "hcv": "section8",
        "voucher": "section8",
        "housing_choice_voucher": "section8",
        "market": "market",
        "conventional": "market",
    }
    return aliases.get(text, text)


def _coerce_float(*values: Any) -> Optional[float]:
    for value in values:
        if value is None:
            continue
        try:
            return float(value)
        except Exception:
            continue
    return None


def _coerce_int(*values: Any) -> Optional[int]:
    for value in values:
        if value is None:
            continue
        try:
            return int(value)
        except Exception:
            continue
    return None


def _property_snapshot(prop: Any) -> dict[str, Any]:
    return {
        "property_id": int(getattr(prop, "id")),
        "address": getattr(prop, "address", None),
        "city": getattr(prop, "city", None),
        "county": getattr(prop, "county", None),
        "state": getattr(prop, "state", None),
        "zip": getattr(prop, "zip", None),
        "property_type": _normalize_property_type(
            getattr(prop, "property_type", None)
            or getattr(prop, "listing_type", None)
        ),
        "program_type": _normalize_program_type(
            getattr(prop, "program_type", None)
            or getattr(prop, "rent_strategy", None)
            or getattr(prop, "tenant_program_type", None)
            or getattr(prop, "voucher_strategy", None)
        ),
        "units": _coerce_int(getattr(prop, "units", None), getattr(prop, "unit_count", None)),
        "bedrooms": _coerce_int(getattr(prop, "bedrooms", None)),
        "bathrooms": _coerce_float(getattr(prop, "bathrooms", None)),
        "square_feet": _coerce_int(getattr(prop, "square_feet", None)),
        "year_built": _coerce_int(getattr(prop, "year_built", None)),
        "listing_price": _coerce_float(
            getattr(prop, "listing_price", None),
            getattr(prop, "asking_price", None),
            getattr(prop, "purchase_price", None),
        ),
        "market_rent_estimate": _coerce_float(getattr(prop, "market_rent_estimate", None)),
        "rent_used": _coerce_float(getattr(prop, "rent_used", None)),
        "monthly_taxes": _coerce_float(getattr(prop, "monthly_taxes", None)),
        "monthly_insurance": _coerce_float(getattr(prop, "monthly_insurance", None)),
        "monthly_debt_service": _coerce_float(getattr(prop, "monthly_debt_service", None)),
        "monthly_housing_cost": _coerce_float(getattr(prop, "monthly_housing_cost", None)),
        "projected_monthly_cashflow": _coerce_float(getattr(prop, "projected_monthly_cashflow", None)),
        "dscr": _coerce_float(getattr(prop, "dscr", None)),
    }


def _data_completeness(snapshot: dict[str, Any]) -> float:
    checks = [
        snapshot.get("state"),
        snapshot.get("county"),
        snapshot.get("city"),
        snapshot.get("property_type"),
        snapshot.get("listing_price"),
        snapshot.get("monthly_debt_service"),
        snapshot.get("market_rent_estimate") or snapshot.get("rent_used"),
    ]
    present = sum(1 for x in checks if x not in (None, "", []))
    return round(present / max(1, len(checks)), 3)


def _source_is_stale(src: Optional[PolicySource], assertion: PolicyAssertion) -> bool:
    if str(getattr(src, "freshness_status", "") or "").lower() in {"stale", "fetch_failed"}:
        return True
    stale_after = getattr(assertion, "stale_after", None)
    if stale_after is not None and stale_after <= datetime.utcnow():
        return True
    return False


def _family_conflict_map(assertions: list[PolicyAssertion]) -> dict[str, dict[str, Any]]:
    families: dict[str, list[PolicyAssertion]] = {}
    for a in assertions:
        fam = a.rule_family or _rule_family(a.rule_key)
        families.setdefault(fam, []).append(a)

    out: dict[str, dict[str, Any]] = {}
    for family, rows in families.items():
        statuses: set[str] = set()
        rule_keys: set[str] = set()
        for row in rows:
            payload = _loads(getattr(row, "value_json", None), {})
            explicit = str(payload.get("status") or "").strip().lower()
            if explicit in {"yes", "no", "conditional"}:
                statuses.add(explicit)
            rule_keys.add(str(getattr(row, "rule_key", "") or ""))
        if len(statuses) > 1:
            out[family] = {
                "family": family,
                "statuses": sorted(statuses),
                "rule_keys": sorted(x for x in rule_keys if x),
                "assertion_ids": sorted(int(getattr(r, "id", 0) or 0) for r in rows),
            }
    return out


def _property_type_matches(assertion_property_type: Optional[str], property_type: Optional[str]) -> bool:
    expected = _normalize_property_type(assertion_property_type)
    actual = _normalize_property_type(property_type)
    if expected is None:
        return True
    if actual is None:
        return False
    if expected == actual:
        return True
    if expected == "residential":
        return actual in {"single_family", "multi_family", "condo", "townhome"}
    if expected == "multi_family_small":
        return actual == "multi_family"
    return False


def _program_type_matches(assertion_program_type: Optional[str], property_program_type: Optional[str]) -> bool:
    expected = _normalize_program_type(assertion_program_type)
    actual = _normalize_program_type(property_program_type)
    if expected is None:
        return True
    if actual is None:
        return False
    return expected == actual


def _rule_default_cost_days(rule_key: str, value: dict[str, Any]) -> tuple[float, int]:
    explicit_cost = _coerce_float(value.get("estimated_cost"), value.get("cost"))
    explicit_days = _coerce_int(value.get("estimated_days"), value.get("days"))
    if explicit_cost is not None or explicit_days is not None:
        return round(explicit_cost or 0.0, 2), int(explicit_days or 0)

    defaults = {
        "rental_registration_required": (175.0, 7),
        "inspection_program_exists": (350.0, 14),
        "certificate_required_before_occupancy": (650.0, 21),
        "pha_landlord_packet_required": (75.0, 3),
        "hap_contract_and_tenancy_addendum_required": (50.0, 2),
        "pha_administrator_changed": (0.0, 2),
        "property_maintenance_enforcement_anchor": (200.0, 7),
        "building_safety_division_anchor": (200.0, 7),
        "building_division_anchor": (200.0, 7),
        "federal_nspire_anchor": (500.0, 14),
    }
    return defaults.get(rule_key, (0.0, 0))


def _resolve_evaluation_status(
    *,
    assertion: PolicyAssertion,
    source: Optional[PolicySource],
    property_snapshot: dict[str, Any],
    family_conflict: Optional[dict[str, Any]],
) -> tuple[str, str, float, Optional[str], Optional[str], float, int, dict[str, Any]]:
    value = _loads(getattr(assertion, "value_json", None), {})
    status_hint = str(value.get("status") or "").strip().lower()
    property_type = property_snapshot.get("property_type")
    program_type = property_snapshot.get("program_type")
    matches_property_type = _property_type_matches(getattr(assertion, "property_type", None), property_type)
    matches_program_type = _program_type_matches(getattr(assertion, "program_type", None), program_type)

    reason_bits: list[str] = []
    detail: dict[str, Any] = {
        "property_type": property_type,
        "program_type": program_type,
        "assertion_property_type": getattr(assertion, "property_type", None),
        "assertion_program_type": getattr(assertion, "program_type", None),
        "status_hint": status_hint or None,
    }

    if family_conflict is not None:
        reason_bits.append("conflicting active assertions exist for this rule family")
        detail["conflict"] = family_conflict
        cost, days = _rule_default_cost_days(assertion.rule_key, value)
        return (
            "conflicting",
            "conflicting",
            max(0.05, min(0.55, float(getattr(assertion, "confidence", 0.0) or 0.0) * 0.55)),
            "Conflicting rule inputs require review before this property can be trusted.",
            "Resolve conflicting policy assertions or refresh authoritative sources.",
            round(cost, 2),
            int(days),
            detail,
        )

    if _source_is_stale(source, assertion):
        reason_bits.append("supporting source is stale or verification has expired")
        detail["freshness_status"] = getattr(source, "freshness_status", None)
        cost, days = _rule_default_cost_days(assertion.rule_key, value)
        return (
            "stale",
            "stale_source",
            max(0.05, min(0.60, float(getattr(assertion, "confidence", 0.0) or 0.0) * 0.60)),
            "Rule applies, but its supporting source is stale.",
            "Refresh and verify this source before relying on the requirement.",
            round(cost, 2),
            int(days),
            detail,
        )

    if status_hint == "no":
        reason_bits.append("assertion explicitly indicates the requirement is not required")
        detail["reason"] = reason_bits
        return (
            "confirmed",
            "not_applicable",
            round(max(0.25, float(getattr(assertion, "confidence", 0.0) or 0.0)), 3),
            "The current rule payload marks this requirement as not required.",
            None,
            0.0,
            0,
            detail,
        )

    if getattr(assertion, "program_type", None) and not matches_program_type:
        if property_snapshot.get("program_type") is not None:
            reason_bits.append("rule is scoped to a different program type")
            detail["reason"] = reason_bits
            return (
                "confirmed",
                "not_applicable",
                round(max(0.20, float(getattr(assertion, "confidence", 0.0) or 0.0)), 3),
                "Requirement is scoped to a different program type than the current property strategy.",
                None,
                0.0,
                0,
                detail,
            )
        reason_bits.append("property program type is unknown, so applicability cannot be proven")
        detail["reason"] = reason_bits
        cost, days = _rule_default_cost_days(assertion.rule_key, value)
        return (
            "unknown",
            "missing_program_type",
            round(max(0.05, float(getattr(assertion, "confidence", 0.0) or 0.0) * 0.65), 3),
            "Property program type is missing, so applicability of this rule is unknown.",
            "Set the property program strategy or attach evidence that this rule is not applicable.",
            round(cost, 2),
            int(days),
            detail,
        )

    if getattr(assertion, "property_type", None) and not matches_property_type:
        if property_snapshot.get("property_type") is not None:
            reason_bits.append("rule is scoped to a different property type")
            detail["reason"] = reason_bits
            return (
                "confirmed",
                "not_applicable",
                round(max(0.20, float(getattr(assertion, "confidence", 0.0) or 0.0)), 3),
                "Requirement is scoped to a different property type than this property.",
                None,
                0.0,
                0,
                detail,
            )
        reason_bits.append("property type is missing, so applicability cannot be proven")
        detail["reason"] = reason_bits
        cost, days = _rule_default_cost_days(assertion.rule_key, value)
        return (
            "unknown",
            "missing_property_type",
            round(max(0.05, float(getattr(assertion, "confidence", 0.0) or 0.0) * 0.65), 3),
            "Property type is missing, so applicability of this rule is unknown.",
            "Set the property type or attach evidence that this rule is not applicable.",
            round(cost, 2),
            int(days),
            detail,
        )

    cost, days = _rule_default_cost_days(assertion.rule_key, value)
    if bool(getattr(assertion, "required", True)):
        severity = "blocking" if bool(getattr(assertion, "blocking", False)) else "required"
        if property_snapshot.get("address"):
            reason_bits.append("rule applicability is inferred from jurisdiction and property metadata")
            detail["reason"] = reason_bits
            return (
                "inferred",
                "missing_property_evidence",
                round(max(0.10, float(getattr(assertion, "confidence", 0.0) or 0.0) * 0.80), 3),
                f"This {severity} requirement likely applies, but property-level proof is not attached yet.",
                "Attach registration, certificate, inspection, or other compliance proof to resolve this rule.",
                round(cost, 2),
                int(days),
                detail,
            )
        reason_bits.append("property metadata is insufficient to support rule application")
        detail["reason"] = reason_bits
        return (
            "unknown",
            "missing_property_metadata",
            round(max(0.05, float(getattr(assertion, "confidence", 0.0) or 0.0) * 0.55), 3),
            "Property metadata is insufficient to prove whether this requirement applies.",
            "Complete the basic property record before relying on this rule projection.",
            round(cost, 2),
            int(days),
            detail,
        )

    return (
        "confirmed",
        "not_required",
        round(max(0.20, float(getattr(assertion, "confidence", 0.0) or 0.0)), 3),
        "Rule is informational and does not create a required property action.",
        None,
        0.0,
        0,
        detail,
    )


def _projection_rollup(
    *,
    items: list[dict[str, Any]],
    coverage: dict[str, Any],
    property_snapshot: dict[str, Any],
) -> dict[str, Any]:
    unresolved = [
        item for item in items
        if item["evaluation_status"] in {"unknown", "inferred", "stale", "conflicting"}
        and item["evidence_status"] not in {"not_applicable", "not_required"}
    ]
    blocking_unresolved = [item for item in unresolved if item.get("blocking")]
    stale_rows = [item for item in items if item["evaluation_status"] == "stale"]
    conflict_rows = [item for item in items if item["evaluation_status"] == "conflicting"]
    unknown_rows = [item for item in items if item["evaluation_status"] == "unknown"]

    projected_cost = round(sum(float(item.get("estimated_cost") or 0.0) for item in unresolved), 2)
    blocking_days = max([int(item.get("estimated_days") or 0) for item in blocking_unresolved] or [0])
    supplemental_days = sum(int(item.get("estimated_days") or 0) for item in unresolved if not item.get("blocking"))
    projected_days_to_rent = int(min(90, blocking_days + min(30, supplemental_days)))

    coverage_confidence = float(coverage.get("confidence_score") or 0.0)
    data_quality = _data_completeness(property_snapshot)
    item_confidence = (
        sum(float(item.get("confidence") or 0.0) for item in items) / max(1, len(items))
        if items else 0.0
    )

    confidence_score = (
        0.45 * coverage_confidence
        + 0.30 * item_confidence
        + 0.25 * data_quality
        - min(0.20, len(stale_rows) * 0.04)
        - min(0.20, len(conflict_rows) * 0.06)
    )
    confidence_score = round(max(0.0, min(1.0, confidence_score)), 3)

    readiness_score = 1.0
    readiness_score -= min(0.55, len(blocking_unresolved) * 0.14)
    readiness_score -= min(0.25, len(unknown_rows) * 0.05)
    readiness_score -= min(0.15, len(stale_rows) * 0.04)
    readiness_score -= min(0.20, len(conflict_rows) * 0.06)
    readiness_score *= max(0.45, 0.65 + 0.35 * confidence_score)

    cashflow = _coerce_float(property_snapshot.get("projected_monthly_cashflow"))
    if cashflow is not None and cashflow < 0:
        readiness_score -= min(0.10, abs(cashflow) / 5000.0)
    dscr = _coerce_float(property_snapshot.get("dscr"))
    if dscr is not None and dscr < 1.0:
        readiness_score -= 0.05

    readiness_score = round(max(0.0, min(1.0, readiness_score)), 3)

    impacted_rules = [
        {
            "rule_key": item["rule_key"],
            "evaluation_status": item["evaluation_status"],
            "blocking": bool(item.get("blocking")),
            "estimated_cost": float(item.get("estimated_cost") or 0.0),
            "estimated_days": int(item.get("estimated_days") or 0),
            "evidence_gap": item.get("evidence_gap"),
        }
        for item in unresolved
    ]
    unresolved_gaps = [
        {
            "rule_key": item["rule_key"],
            "gap": item.get("evidence_gap"),
            "resolution": item.get("resolution_hint"),
            "evaluation_status": item["evaluation_status"],
        }
        for item in unresolved
        if item.get("evidence_gap")
    ]

    projection_status = "ready"
    if conflict_rows:
        projection_status = "conflicting"
    elif stale_rows:
        projection_status = "stale"
    elif blocking_unresolved or unknown_rows:
        projection_status = "needs_evidence"

    return {
        "projection_status": projection_status,
        "blocking_count": len(blocking_unresolved),
        "unknown_count": len(unknown_rows),
        "stale_count": len(stale_rows),
        "conflicting_count": len(conflict_rows),
        "readiness_score": readiness_score,
        "projected_compliance_cost": projected_cost,
        "projected_days_to_rent": projected_days_to_rent,
        "confidence_score": confidence_score,
        "impacted_rules": impacted_rules,
        "unresolved_evidence_gaps": unresolved_gaps,
    }


def _serialize_projection_row(row: PropertyComplianceProjection, items: list[PropertyComplianceProjectionItem]) -> dict[str, Any]:
    item_rows: list[dict[str, Any]] = []
    for item in items:
        item_rows.append(
            {
                "id": int(item.id),
                "projection_id": int(item.projection_id),
                "property_id": int(item.property_id),
                "policy_assertion_id": item.policy_assertion_id,
                "jurisdiction_slug": item.jurisdiction_slug,
                "program_type": item.program_type,
                "property_type": item.property_type,
                "source_level": item.source_level,
                "rule_key": item.rule_key,
                "rule_category": item.rule_category,
                "required": bool(item.required),
                "blocking": bool(item.blocking),
                "evaluation_status": item.evaluation_status,
                "evidence_status": item.evidence_status,
                "confidence": float(item.confidence or 0.0),
                "estimated_cost": float(item.estimated_cost or 0.0) if item.estimated_cost is not None else None,
                "estimated_days": int(item.estimated_days or 0) if item.estimated_days is not None else None,
                "evidence_summary": item.evidence_summary,
                "evidence_gap": item.evidence_gap,
                "resolution_detail": _loads(item.resolution_detail_json, {}),
            }
        )
    item_rows.sort(key=lambda r: (r["evaluation_status"], 0 if r.get("blocking") else 1, r["rule_key"]))

    return {
        "id": int(row.id),
        "org_id": int(row.org_id),
        "property_id": int(row.property_id),
        "jurisdiction_slug": row.jurisdiction_slug,
        "program_type": row.program_type,
        "rules_version": row.rules_version,
        "projection_status": row.projection_status,
        "projection_basis": _loads(row.projection_basis_json, {}),
        "blocking_count": int(row.blocking_count or 0),
        "unknown_count": int(row.unknown_count or 0),
        "stale_count": int(row.stale_count or 0),
        "conflicting_count": int(row.conflicting_count or 0),
        "readiness_score": float(row.readiness_score or 0.0),
        "projected_compliance_cost": float(row.projected_compliance_cost or 0.0) if row.projected_compliance_cost is not None else None,
        "projected_days_to_rent": int(row.projected_days_to_rent or 0) if row.projected_days_to_rent is not None else None,
        "confidence_score": float(row.confidence_score or 0.0),
        "impacted_rules": _loads(row.impacted_rules_json, []),
        "unresolved_evidence_gaps": _loads(row.unresolved_evidence_gaps_json, []),
        "last_projected_at": row.last_projected_at.isoformat() if row.last_projected_at else None,
        "is_current": bool(row.is_current),
        "items": item_rows,
    }


def _fetch_property(db: Session, *, org_id: int, property_id: int) -> Any:
    if Property is None:
        raise RuntimeError("Property model import failed")
    stmt = select(Property).where(Property.org_id == int(org_id), Property.id == int(property_id))
    prop = db.scalar(stmt)
    if prop is None:
        raise ValueError("property not found")
    return prop


def project_property_compliance(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    force_recompute: bool = False,
) -> dict[str, Any]:
    prop = _fetch_property(db, org_id=org_id, property_id=property_id)
    snapshot = _property_snapshot(prop)

    state = _norm_state(snapshot.get("state") or "MI")
    county = _norm_lower(snapshot.get("county"))
    city = _norm_lower(snapshot.get("city"))
    program_type = _normalize_program_type(snapshot.get("program_type"))

    if not force_recompute:
        current = db.scalar(
            select(PropertyComplianceProjection)
            .where(
                PropertyComplianceProjection.org_id == int(org_id),
                PropertyComplianceProjection.property_id == int(property_id),
                PropertyComplianceProjection.is_current.is_(True),
            )
            .order_by(PropertyComplianceProjection.id.desc())
        )
        if current is not None:
            items = list(
                db.scalars(
                    select(PropertyComplianceProjectionItem)
                    .where(PropertyComplianceProjectionItem.projection_id == int(current.id))
                    .order_by(PropertyComplianceProjectionItem.id.asc())
                ).all()
            )
            payload = _serialize_projection_row(current, items)
            payload["cached"] = True
            payload["ok"] = True
            return payload

    assertions = _query_inherited_assertions(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=None,
        statuses=["verified"],
    )
    winners = _pick_winners(
        db,
        assertions,
        target_county=county,
        target_city=city,
        target_pha_name=None,
    )
    source_map = _source_info_map(db, winners)
    conflicts = _family_conflict_map(assertions)
    coverage = compute_coverage_status(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=None,
    )

    item_payloads: list[dict[str, Any]] = []
    for winner in winners:
        family = winner.rule_family or _rule_family(winner.rule_key)
        source = source_map.get(winner.source_id) if winner.source_id is not None else None
        (
            evaluation_status,
            evidence_status,
            item_confidence,
            evidence_summary,
            evidence_gap,
            estimated_cost,
            estimated_days,
            resolution_detail,
        ) = _resolve_evaluation_status(
            assertion=winner,
            source=source,
            property_snapshot=snapshot,
            family_conflict=conflicts.get(family),
        )

        item_payloads.append(
            {
                "policy_assertion_id": int(winner.id),
                "jurisdiction_slug": getattr(winner, "jurisdiction_slug", None),
                "program_type": getattr(winner, "program_type", None),
                "property_type": getattr(winner, "property_type", None),
                "source_level": getattr(winner, "source_level", None) or _layer_for_assertion(winner),
                "rule_key": winner.rule_key,
                "rule_category": getattr(winner, "rule_category", None) or getattr(winner, "normalized_category", None),
                "required": bool(getattr(winner, "required", True)),
                "blocking": bool(getattr(winner, "blocking", False)),
                "evaluation_status": evaluation_status,
                "evidence_status": evidence_status,
                "confidence": item_confidence,
                "estimated_cost": estimated_cost,
                "estimated_days": estimated_days,
                "evidence_summary": evidence_summary,
                "evidence_gap": evidence_gap,
                "resolution_detail": {
                    **resolution_detail,
                    "source_url": getattr(source, "url", None),
                    "source_title": getattr(source, "title", None),
                    "source_publisher": getattr(source, "publisher", None),
                    "source_retrieved_at": source.retrieved_at.isoformat() if source and source.retrieved_at else None,
                    "source_freshness_status": getattr(source, "freshness_status", None),
                    "rule_family": family,
                },
            }
        )

    rollup = _projection_rollup(items=item_payloads, coverage=coverage, property_snapshot=snapshot)
    rules_version = _stable_projection_hash(
        {
            "scope": {
                "org_id": org_id,
                "property_id": property_id,
                "state": state,
                "county": county,
                "city": city,
                "program_type": program_type,
            },
            "rules": [
                {
                    "assertion_id": item["policy_assertion_id"],
                    "rule_key": item["rule_key"],
                    "evaluation_status": item["evaluation_status"],
                    "confidence": item["confidence"],
                }
                for item in item_payloads
            ],
            "coverage_score": coverage.get("confidence_score"),
            "property_basis": {
                "property_type": snapshot.get("property_type"),
                "program_type": snapshot.get("program_type"),
                "listing_price": snapshot.get("listing_price"),
            },
        }
    )

    basis = {
        "market": {
            "state": state,
            "county": county,
            "city": city,
        },
        "property_snapshot": snapshot,
        "coverage": {
            "coverage_status": coverage.get("coverage_status"),
            "production_readiness": coverage.get("production_readiness"),
            "confidence_score": coverage.get("confidence_score"),
            "confidence_label": coverage.get("coverage_confidence"),
            "completeness_status": coverage.get("completeness_status"),
            "missing_categories": coverage.get("missing_categories") or [],
            "missing_rule_keys": coverage.get("missing_rule_keys") or [],
        },
        "resolved_layers": _resolved_layer_summary(winners),
        "source_evidence": _source_evidence_rows(db, winners),
        "data_completeness": _data_completeness(snapshot),
    }

    existing_current = list(
        db.scalars(
            select(PropertyComplianceProjection).where(
                PropertyComplianceProjection.org_id == int(org_id),
                PropertyComplianceProjection.property_id == int(property_id),
                PropertyComplianceProjection.is_current.is_(True),
            )
        ).all()
    )
    now = datetime.utcnow()
    for row in existing_current:
        row.is_current = False
        row.superseded_at = now
        row.updated_at = now
        db.add(row)

    projection = PropertyComplianceProjection(
        org_id=int(org_id),
        property_id=int(property_id),
        jurisdiction_slug=(coverage.get("jurisdiction_slug") or f"{state.lower()}:{county or '-'}:{city or '-'}"),
        program_type=program_type,
        rules_version=rules_version,
        projection_status=rollup["projection_status"],
        projection_basis_json=_dumps(basis),
        blocking_count=int(rollup["blocking_count"]),
        unknown_count=int(rollup["unknown_count"]),
        stale_count=int(rollup["stale_count"]),
        conflicting_count=int(rollup["conflicting_count"]),
        readiness_score=float(rollup["readiness_score"]),
        projected_compliance_cost=rollup["projected_compliance_cost"],
        projected_days_to_rent=rollup["projected_days_to_rent"],
        confidence_score=float(rollup["confidence_score"]),
        impacted_rules_json=_dumps(rollup["impacted_rules"]),
        unresolved_evidence_gaps_json=_dumps(rollup["unresolved_evidence_gaps"]),
        last_projected_at=now,
        is_current=True,
        updated_at=now,
    )
    db.add(projection)
    db.flush()

    item_rows: list[PropertyComplianceProjectionItem] = []
    for payload in item_payloads:
        row = PropertyComplianceProjectionItem(
            org_id=int(org_id),
            projection_id=int(projection.id),
            property_id=int(property_id),
            policy_assertion_id=payload["policy_assertion_id"],
            jurisdiction_slug=payload["jurisdiction_slug"],
            program_type=payload["program_type"],
            property_type=payload["property_type"],
            source_level=payload["source_level"],
            rule_key=payload["rule_key"],
            rule_category=payload["rule_category"],
            required=bool(payload["required"]),
            blocking=bool(payload["blocking"]),
            evaluation_status=payload["evaluation_status"],
            evidence_status=payload["evidence_status"],
            confidence=float(payload["confidence"]),
            estimated_cost=float(payload["estimated_cost"]) if payload["estimated_cost"] is not None else None,
            estimated_days=int(payload["estimated_days"]) if payload["estimated_days"] is not None else None,
            evidence_summary=payload["evidence_summary"],
            evidence_gap=payload["evidence_gap"],
            resolution_detail_json=_dumps(payload["resolution_detail"]),
        )
        db.add(row)
        item_rows.append(row)

    db.commit()
    db.refresh(projection)
    for row in item_rows:
        db.refresh(row)

    payload = _serialize_projection_row(projection, item_rows)
    payload["cached"] = False
    payload["ok"] = True
    return payload


def get_current_property_projection(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    recompute_if_missing: bool = True,
) -> dict[str, Any]:
    current = db.scalar(
        select(PropertyComplianceProjection)
        .where(
            PropertyComplianceProjection.org_id == int(org_id),
            PropertyComplianceProjection.property_id == int(property_id),
            PropertyComplianceProjection.is_current.is_(True),
        )
        .order_by(PropertyComplianceProjection.id.desc())
    )
    if current is None:
        if recompute_if_missing:
            return project_property_compliance(
                db,
                org_id=org_id,
                property_id=property_id,
                force_recompute=True,
            )
        return {"ok": False, "error": "projection_not_found", "property_id": int(property_id)}

    items = list(
        db.scalars(
            select(PropertyComplianceProjectionItem)
            .where(PropertyComplianceProjectionItem.projection_id == int(current.id))
            .order_by(PropertyComplianceProjectionItem.id.asc())
        ).all()
    )
    payload = _serialize_projection_row(current, items)
    payload["ok"] = True
    payload["cached"] = True
    return payload


def build_property_compliance_brief(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    property_id: Optional[int] = None,
    include_projection: bool = False,
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

    winners = _pick_winners(
        db,
        assertions,
        target_county=cnty,
        target_city=cty,
        target_pha_name=pha,
    )
    coverage = compute_coverage_status(db, org_id=org_id, state=st, county=cnty, city=cty, pha_name=pha)

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
        "coverage_confidence": summary["coverage"].get("confidence_label"),
        "production_readiness": summary["coverage"].get("production_readiness"),
        "completeness_status": summary["coverage"].get("completeness_status"),
        "missing_categories": summary["coverage"].get("missing_categories"),
        "is_stale": summary["coverage"].get("is_stale"),
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
        explanation_parts.append("certificate/compliance approval is required in certain occupancy scenarios")
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

    brief = {
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
        "required_categories": summary.get("required_categories") or [],
        "category_coverage": summary.get("category_coverage") or {},
        "resolved_layers": _resolved_layer_summary(winners),
        "source_evidence": _source_evidence_rows(db, winners),
        "coverage_confidence": coverage.get("coverage_confidence"),
        "confidence_score": coverage.get("confidence_score"),
        "missing_local_rule_areas": coverage.get("missing_local_rule_areas") or coverage.get("missing_categories") or [],
        "missing_rule_keys": coverage.get("missing_rule_keys") or [],
        "resolved_rule_version": _stable_projection_hash(
            {
                "scope": {"state": state, "county": county, "city": city, "pha_name": pha_name, "org_id": org_id},
                "source_evidence": _source_evidence_rows(db, winners),
                "confidence": coverage.get("confidence_score"),
            }
        ),
    }

    if include_projection and org_id is not None and property_id is not None:
        try:
            brief["property_projection"] = get_current_property_projection(
                db,
                org_id=int(org_id),
                property_id=int(property_id),
                recompute_if_missing=True,
            )
        except Exception as exc:
            brief["property_projection"] = {
                "ok": False,
                "error": "projection_failed",
                "detail": str(exc),
                "property_id": int(property_id),
            }

    return brief