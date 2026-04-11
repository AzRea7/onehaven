# backend/app/services/jurisdiction_profile_service.py
from __future__ import annotations

import copy
import hashlib
import json
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from app.domain.jurisdiction_categories import expected_rule_universe_for_scope
from app.domain.jurisdiction_defaults import default_policy_for_scope
from app.policy_models import JurisdictionProfile


def _norm(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    v = s.strip()
    return v if v else None


def _norm_city(s: Optional[str]) -> Optional[str]:
    v = _norm(s)
    return v.lower() if v else None


def _norm_county(s: Optional[str]) -> Optional[str]:
    v = _norm(s)
    return v.lower() if v else None


def _norm_state(s: Optional[str]) -> str:
    v = (s or "MI").strip().upper()
    return v or "MI"


def _loads(s: Optional[str], default: Any = None) -> Any:
    if default is None:
        default = {}
    if not s:
        return default
    try:
        return json.loads(s)
    except Exception:
        return default


def _dumps(v: Any) -> str:
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return "{}"


def _deep_merge(base: Any, override: Any) -> Any:
    if isinstance(base, dict) and isinstance(override, dict):
        out = dict(base)
        for k, v in override.items():
            if k in out:
                out[k] = _deep_merge(out[k], v)
            else:
                out[k] = copy.deepcopy(v)
        return out
    return copy.deepcopy(override)


def _dedupe_dict_list(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()

    for x in rows:
        if not isinstance(x, dict):
            continue
        key = str(
            x.get("code")
            or x.get("rule_key")
            or x.get("title")
            or x.get("description")
            or x.get("label")
            or ""
        ).strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(x)

    return out


def _expected_rule_universe_payload(
    *,
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
    include_section8: bool = True,
    tenant_waitlist_depth: Optional[str] = None,
) -> dict[str, Any]:
    return expected_rule_universe_for_scope(
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        include_section8=include_section8,
        tenant_waitlist_depth=tenant_waitlist_depth,
    ).to_dict()


def _augment_policy_with_expected_universe(
    *,
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
    policy: dict[str, Any],
) -> dict[str, Any]:
    universe = _expected_rule_universe_payload(
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        include_section8=True,
        tenant_waitlist_depth=(policy.get("operations") or {}).get("tenant_waitlist_depth") if isinstance(policy, dict) else None,
    )
    merged = _deep_merge(
        default_policy_for_scope(
            state=state,
            county=county,
            city=city,
            housing_authority=pha_name,
            include_section8=True,
        ),
        policy or {},
    )
    merged["expected_rule_universe"] = universe
    coverage = merged.setdefault("coverage", {})
    coverage["expected_rule_universe"] = universe
    coverage["required_categories"] = list(universe.get("required_categories") or [])
    coverage["critical_categories"] = list(universe.get("critical_categories") or [])
    coverage["optional_categories"] = list(universe.get("optional_categories") or [])
    coverage["jurisdiction_types"] = list(universe.get("jurisdiction_types") or [])
    merged["required_categories"] = list(universe.get("required_categories") or [])
    merged["critical_categories"] = list(universe.get("critical_categories") or [])
    merged["optional_categories"] = list(universe.get("optional_categories") or [])
    merged["jurisdiction_types"] = list(universe.get("jurisdiction_types") or [])
    if not merged.get("missing_local_rule_areas"):
        merged["missing_local_rule_areas"] = list(coverage.get("missing_local_rule_areas") or [])
    return merged


def _is_warren_market(*, state: str, county: Optional[str], city: Optional[str]) -> bool:
    return (
        _norm_state(state) == "MI"
        and _norm_county(county) == "macomb"
        and _norm_city(city) == "warren"
    )


def _default_warren_policy() -> dict[str, Any]:
    return {
        "summary": "Evidence-backed Warren municipal rental operations profile",
        "compliance": {
            "rental_license_required": "yes",
            "inspection_required": "yes",
            "inspection_frequency": "biennial",
            "certificate_required_before_occupancy": "yes",
            "local_agent_required": "yes",
            "local_agent_radius_miles": 50,
            "owner_po_box_allowed": "no",
            "all_fees_must_be_paid": "yes",
            "city_debts_block_license": "yes",
        },
        "licensing": {
            "license_nontransferable": "yes",
            "renewal_days_before_expiration": 60,
            "license_term_years": 2,
        },
        "documents": {
            "application_packet_known": "yes",
            "instructions_known": "yes",
            "registration_checklist_new_known": "yes",
            "registration_checklist_renewal_known": "yes",
            "inspection_checklist_known": "yes",
            "owner_information_form_known": "yes",
            "tenant_information_form_known": "yes",
        },
        "fees": {
            "schedule_known": "yes",
            "fee_schedule_source": "city_rental_inspections_division_page",
        },
        "state_rules": {
            "source_of_income_discrimination_prohibited": "yes",
            "source_of_income_effective_date": "2025-04-02",
            "source_of_income_threshold_units": 5,
        },
        "hqs_addenda": [
            {
                "code": "WARREN_POSTED_ADDRESS_VISIBLE",
                "description": "Unit address / numbering should be clearly identifiable for inspection access and record matching",
                "category": "jurisdiction",
                "severity": "warn",
                "suggested_fix": "Ensure address numbering is visible and matches inspection records.",
            },
            {
                "code": "WARREN_APPLICATION_PACKET_READY",
                "description": "Municipal rental packet documentation should be organized and inspection-ready",
                "category": "documents",
                "severity": "warn",
                "suggested_fix": "Prepare owner, tenant, and registration packet documentation before inspection.",
            },
        ],
        "required_actions": [
            {
                "code": "WARREN_RENTAL_LICENSE_REQUIRED",
                "title": "Warren rental license required",
                "severity": "fail",
                "category": "licensing",
                "source": "warren_profile",
                "blocks_local": True,
                "blocks_voucher": True,
                "blocks_lease_up": True,
                "suggested_fix": "Complete Warren rental license application and obtain license approval.",
            },
            {
                "code": "WARREN_BIENNIAL_INSPECTION_REQUIRED",
                "title": "Warren biennial rental inspection required",
                "severity": "fail",
                "category": "inspection",
                "source": "warren_profile",
                "blocks_local": True,
                "blocks_voucher": True,
                "blocks_lease_up": True,
                "suggested_fix": "Schedule and pass Warren's required rental inspection.",
            },
            {
                "code": "WARREN_ALL_FEES_PAID_REQUIRED",
                "title": "Warren requires rental fees to be paid before license issuance",
                "severity": "fail",
                "category": "fees",
                "source": "warren_profile",
                "blocks_local": True,
                "blocks_voucher": True,
                "blocks_lease_up": True,
                "suggested_fix": "Pay all required rental registration / licensing / inspection fees.",
            },
            {
                "code": "WARREN_CITY_DEBTS_BLOCK_LICENSE",
                "title": "Warren blocks license issuance when listed city debts remain unpaid",
                "severity": "fail",
                "category": "fees",
                "source": "warren_profile",
                "blocks_local": True,
                "blocks_voucher": True,
                "blocks_lease_up": True,
                "suggested_fix": "Clear listed taxes, assessments, utility balances, blight-related debts, and related city obligations.",
            },
            {
                "code": "WARREN_LOCAL_AGENT_REQUIRED",
                "title": "Warren local agent required",
                "severity": "fail",
                "category": "jurisdiction",
                "source": "warren_profile",
                "blocks_local": True,
                "blocks_voucher": True,
                "blocks_lease_up": True,
                "suggested_fix": "Designate a qualified local agent that meets Warren requirements.",
            },
            {
                "code": "WARREN_LOCAL_AGENT_MAX_RADIUS_MILES",
                "title": "Warren local agent must be within 50 miles",
                "severity": "fail",
                "category": "jurisdiction",
                "source": "warren_profile",
                "blocks_local": True,
                "blocks_voucher": True,
                "blocks_lease_up": True,
                "suggested_fix": "Confirm your local agent is an individual located within 50 miles of Warren.",
            },
            {
                "code": "WARREN_OWNER_PO_BOX_ALLOWED",
                "title": "Warren does not allow P.O. boxes for required legal/home address fields",
                "severity": "fail",
                "category": "documents",
                "source": "warren_profile",
                "blocks_local": True,
                "blocks_voucher": True,
                "blocks_lease_up": True,
                "suggested_fix": "Use a valid physical legal/home address where Warren requires one; do not use a P.O. box.",
            },
            {
                "code": "MI_SOURCE_OF_INCOME_DISCRIMINATION_PROHIBITED",
                "title": "Michigan source-of-income discrimination protections apply",
                "severity": "warn",
                "category": "fair_housing",
                "source": "mi_state_rule",
                "blocks_local": False,
                "blocks_voucher": False,
                "blocks_lease_up": False,
                "suggested_fix": "Ensure screening, leasing, and rejection logic do not discriminate based on lawful source of income where applicable.",
            },
        ],
        "blocking_items": [
            {
                "code": "WARREN_RENTAL_LICENSE_REQUIRED",
                "title": "Warren rental license required",
                "severity": "fail",
                "category": "licensing",
                "source": "warren_profile",
                "blocks_local": True,
                "blocks_voucher": True,
                "blocks_lease_up": True,
            },
            {
                "code": "WARREN_BIENNIAL_INSPECTION_REQUIRED",
                "title": "Warren biennial rental inspection required",
                "severity": "fail",
                "category": "inspection",
                "source": "warren_profile",
                "blocks_local": True,
                "blocks_voucher": True,
                "blocks_lease_up": True,
            },
            {
                "code": "WARREN_CITY_DEBTS_BLOCK_LICENSE",
                "title": "Warren blocks license issuance when listed city debts remain unpaid",
                "severity": "fail",
                "category": "fees",
                "source": "warren_profile",
                "blocks_local": True,
                "blocks_voucher": True,
                "blocks_lease_up": True,
            },
            {
                "code": "WARREN_LOCAL_AGENT_REQUIRED",
                "title": "Warren local agent required",
                "severity": "fail",
                "category": "jurisdiction",
                "source": "warren_profile",
                "blocks_local": True,
                "blocks_voucher": True,
                "blocks_lease_up": True,
            },
            {
                "code": "WARREN_OWNER_PO_BOX_ALLOWED",
                "title": "Warren does not allow P.O. boxes for required legal/home address fields",
                "severity": "fail",
                "category": "documents",
                "source": "warren_profile",
                "blocks_local": True,
                "blocks_voucher": True,
                "blocks_lease_up": True,
            },
        ],
        "rules": [
            {
                "rule_key": "MI_SOURCE_OF_INCOME_DISCRIMINATION_PROHIBITED",
                "label": "Michigan source-of-income discrimination protections apply",
                "status": "warn",
                "severity": "warn",
                "category": "fair_housing",
                "source": "mi_state_rule",
                "blocks_local": False,
                "blocks_voucher": False,
                "blocks_lease_up": False,
                "suggested_fix": "Ensure screening and leasing workflows comply with Michigan source-of-income protections.",
            }
        ],
        "evidence": {
            "municipal_primary": [
                "https://www.cityofwarren.org/departments/rental-inspections-division/",
                "https://www.cityofwarren.org/wp-content/uploads/2024/03/Rental-Application-Paperwork-revised2-Fillable.pdf",
            ],
            "state_primary": [
                "https://www.legislature.mi.gov/documents/mcl/pdf/mcl-Act-348-of-1972.pdf",
                "https://www.courts.michigan.gov/496687/siteassets/publications/impact/written/civil/impact-e-mail-4-9-25-civil.pdf",
            ],
        },
    }


def _augment_policy_for_market(
    *,
    state: str,
    county: Optional[str],
    city: Optional[str],
    policy: dict[str, Any],
) -> dict[str, Any]:
    base = copy.deepcopy(policy or {})

    if _is_warren_market(state=state, county=county, city=city):
        merged = _deep_merge(_default_warren_policy(), base)
        merged["required_actions"] = _dedupe_dict_list(merged.get("required_actions") or [])
        merged["blocking_items"] = _dedupe_dict_list(merged.get("blocking_items") or [])
        merged["rules"] = _dedupe_dict_list(merged.get("rules") or [])
        merged["hqs_addenda"] = _dedupe_dict_list(merged.get("hqs_addenda") or [])
        return merged

    if isinstance(base.get("required_actions"), list):
        base["required_actions"] = _dedupe_dict_list(base["required_actions"])
    if isinstance(base.get("blocking_items"), list):
        base["blocking_items"] = _dedupe_dict_list(base["blocking_items"])
    if isinstance(base.get("rules"), list):
        base["rules"] = _dedupe_dict_list(base["rules"])
    if isinstance(base.get("hqs_addenda"), list):
        base["hqs_addenda"] = _dedupe_dict_list(base["hqs_addenda"])

    return base


def list_profiles(
    db: Session,
    *,
    org_id: int,
    include_global: bool = True,
    state: str = "MI",
) -> list[JurisdictionProfile]:
    st = _norm_state(state)

    q_org = select(JurisdictionProfile).where(
        and_(JurisdictionProfile.state == st, JurisdictionProfile.org_id == org_id)
    )

    if not include_global:
        return list(db.scalars(q_org).all())

    q_global = select(JurisdictionProfile).where(
        and_(JurisdictionProfile.state == st, JurisdictionProfile.org_id.is_(None))
    )

    rows = list(db.scalars(q_global).all()) + list(db.scalars(q_org).all())
    return rows


def _specificity(r: JurisdictionProfile) -> int:
    if (r.city or "").strip():
        return 2
    if (r.county or "").strip():
        return 1
    return 0


def resolve_profile(
    db: Session,
    *,
    org_id: Optional[int],
    city: Optional[str] = None,
    county: Optional[str] = None,
    state: str = 'MI',
):
    st = _norm_state(state)
    req_city = _norm_city(city)
    req_county = _norm_county(county)

    rows = list_profiles(db, org_id=org_id or 0, include_global=True, state=st)

    def match_level(r: JurisdictionProfile) -> Optional[str]:
        r_city = _norm_city(r.city)
        r_county = _norm_county(r.county)

        if req_city:
            if r_city and r_city == req_city:
                return "city"
            if (not r_city) and r_county and req_county and r_county == req_county:
                return "county"
            if (not r_city) and (not r_county):
                return "state"
            return None

        if req_county:
            if (not r_city) and r_county and r_county == req_county:
                return "county"
            if (not r_city) and (not r_county):
                return "state"
            return None

        if (not r_city) and (not r_county):
            return "state"
        return None

    candidates: list[tuple[int, int, int, JurisdictionProfile, str]] = []
    for r in rows:
        lvl = match_level(r)
        if not lvl:
            continue

        spec = _specificity(r)
        scope_pri = 1 if (r.org_id == org_id) else 0
        rid = int(r.id)
        candidates.append((spec, scope_pri, rid, r, lvl))

    if not candidates:
        universe = _expected_rule_universe_payload(
            state=st,
            county=req_county,
            city=req_city,
            pha_name=None,
            include_section8=True,
        )
        policy = _augment_policy_with_expected_universe(
            state=st,
            county=req_county,
            city=req_city,
            pha_name=None,
            policy={},
        )
        return {
            "matched": False,
            "scope": None,
            "match_level": None,
            "friction_multiplier": 1.0,
            "pha_name": None,
            "policy": policy,
            "rules": [],
            "hqs_addenda": [],
            "notes": None,
            "profile_id": None,
            "required_categories": list(universe.get("required_categories") or []),
            "critical_categories": list(universe.get("critical_categories") or []),
            "optional_categories": list(universe.get("optional_categories") or []),
            "jurisdiction_types": list(universe.get("jurisdiction_types") or []),
            "market": {
                "state": st,
                "county": req_county,
                "city": req_city,
            },
        }

    candidates.sort(key=lambda t: (-t[0], -t[1], t[2]))
    _best_spec, best_scope_pri, _rid, chosen, lvl = candidates[0]

    scope = "org" if best_scope_pri == 1 else "global"
    raw_policy = _loads(getattr(chosen, "policy_json", None), {})
    policy = _augment_policy_with_expected_universe(
        state=st,
        county=req_county,
        city=req_city,
        pha_name=chosen.pha_name,
        policy=_augment_policy_for_market(
            state=st,
            county=req_county,
            city=req_city,
            policy=raw_policy,
        ),
    )
    universe = policy.get("expected_rule_universe") or {}

    return {
        "matched": True,
        "scope": scope,
        "match_level": lvl,
        "friction_multiplier": float(chosen.friction_multiplier or 1.0),
        "pha_name": chosen.pha_name,
        "policy": policy,
        "rules": policy.get("rules", []),
        "hqs_addenda": policy.get("hqs_addenda", []),
        "notes": chosen.notes,
        "profile_id": int(chosen.id),
        "required_categories": list(universe.get("required_categories") or []),
        "critical_categories": list(universe.get("critical_categories") or []),
        "optional_categories": list(universe.get("optional_categories") or []),
        "jurisdiction_types": list(universe.get("jurisdiction_types") or []),
        "market": {
            "state": st,
            "county": req_county,
            "city": req_city,
        },
    }


def upsert_profile(
    db: Session,
    *,
    org_id: int,
    state: str,
    county: Optional[str],
    city: Optional[str],
    friction_multiplier: float,
    pha_name: Optional[str],
    policy: Any,
    notes: Optional[str],
) -> JurisdictionProfile:
    st = _norm_state(state)
    cnty = _norm_county(county)
    cty = _norm_city(city)

    if isinstance(policy, dict):
        policy = _augment_policy_with_expected_universe(
            state=st,
            county=cnty,
            city=cty,
            pha_name=_norm(pha_name),
            policy=_augment_policy_for_market(
                state=st,
                county=cnty,
                city=cty,
                policy=policy,
            ),
        )

    q = (
        select(JurisdictionProfile)
        .where(JurisdictionProfile.org_id == org_id)
        .where(JurisdictionProfile.state == st)
        .where(
            and_(
                (JurisdictionProfile.county.is_(None) if cnty is None else JurisdictionProfile.county == cnty),
                (JurisdictionProfile.city.is_(None) if cty is None else JurisdictionProfile.city == cty),
            )
        )
    )
    row = db.scalar(q)

    now = datetime.utcnow()

    if row is None:
        row = JurisdictionProfile(
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            friction_multiplier=float(friction_multiplier or 1.0),
            pha_name=_norm(pha_name),
            policy_json=_dumps(policy or {}),
            notes=_norm(notes),
            updated_at=now,
        )
        if hasattr(row, "required_categories_json") and isinstance(policy, dict):
            row.required_categories_json = _dumps(policy.get("required_categories") or [])
        db.add(row)
    else:
        row.friction_multiplier = float(friction_multiplier or 1.0)
        row.pha_name = _norm(pha_name)
        row.policy_json = _dumps(policy or {})
        row.notes = _norm(notes)
        row.updated_at = now
        if hasattr(row, "required_categories_json") and isinstance(policy, dict):
            row.required_categories_json = _dumps(policy.get("required_categories") or [])

    db.commit()
    db.refresh(row)
    return row


def resolve_operational_policy(
    db: Session,
    *,
    org_id: int,
    city: Optional[str],
    county: Optional[str],
    state: str = "MI",
    pha_name: Optional[str] = None,
) -> dict[str, Any]:
    from app.services.policy_projection_service import build_property_compliance_brief

    base = resolve_profile(
        db,
        org_id=org_id,
        city=city,
        county=county,
        state=state,
    )

    brief = build_property_compliance_brief(
        db,
        org_id=None,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name or base.get("pha_name"),
    )

    policy = base.get("policy") or {}
    rules = policy.get("rules", [])
    policy_required_actions = policy.get("required_actions", [])
    policy_blocking_items = policy.get("blocking_items", [])
    policy_hqs_addenda = policy.get("hqs_addenda", [])

    combined_required_actions: list[dict[str, Any]] = []
    for x in brief.get("required_actions", []):
        if isinstance(x, dict):
            combined_required_actions.append(x)
    for x in policy_required_actions:
        if isinstance(x, dict):
            combined_required_actions.append(x)

    combined_blocking_items: list[dict[str, Any]] = []
    for x in brief.get("blocking_items", []):
        if isinstance(x, dict):
            combined_blocking_items.append(x)
    for x in policy_blocking_items:
        if isinstance(x, dict):
            combined_blocking_items.append(x)

    evidence_links = list(brief.get("evidence_links", []) or [])
    policy_evidence = policy.get("evidence") or {}
    if isinstance(policy_evidence, dict):
        for _, values in policy_evidence.items():
            if isinstance(values, list):
                for url in values:
                    if isinstance(url, str) and url.strip():
                        evidence_links.append({"title": "Policy evidence", "url": url.strip()})

    return {
        **base,
        "rules": _dedupe_dict_list(rules if isinstance(rules, list) else []),
        "hqs_addenda": _dedupe_dict_list(policy_hqs_addenda if isinstance(policy_hqs_addenda, list) else []),
        "coverage": brief.get("coverage", {}),
        "brief": brief.get("compliance", {}),
        "blocking_items": _dedupe_dict_list(combined_blocking_items),
        "required_actions": _dedupe_dict_list(combined_required_actions),
        "evidence_links": _dedupe_dict_list([x for x in evidence_links if isinstance(x, dict)]),
    }


def summarize_profile(
    db: Session,
    *,
    org_id: int,
    city: Optional[str],
    county: Optional[str],
    state: str = "MI",
    pha_name: Optional[str] = None,
) -> dict[str, Any]:
    return resolve_operational_policy(
        db,
        org_id=org_id,
        city=city,
        county=county,
        state=state,
        pha_name=pha_name,
    )


def delete_profile(
    db: Session,
    *,
    org_id: int,
    state: str,
    county: Optional[str],
    city: Optional[str],
) -> int:
    st = _norm_state(state)
    cnty = _norm_county(county)
    cty = _norm_city(city)

    q = (
        select(JurisdictionProfile)
        .where(JurisdictionProfile.org_id == org_id)
        .where(JurisdictionProfile.state == st)
        .where(
            and_(
                (JurisdictionProfile.county.is_(None) if cnty is None else JurisdictionProfile.county == cnty),
                (JurisdictionProfile.city.is_(None) if cty is None else JurisdictionProfile.city == cty),
            )
        )
    )
    row = db.scalar(q)
    if row is None:
        return 0

    db.delete(row)
    db.commit()
    return 1


# ---- Chunk 5 layered profile helpers ----
_base_resolve_profile = resolve_profile
_base_resolve_operational_policy = resolve_operational_policy
_base_summarize_profile = summarize_profile


def _policy_hash(payload: Any) -> str:
    try:
        raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    except Exception:
        raw = str(payload)
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()[:16]


def _layer_name_for_assertion(a: Any) -> str:
    if getattr(a, 'org_id', None) is not None:
        return 'org_override'
    if getattr(a, 'pha_name', None):
        return 'housing_authority'
    if getattr(a, 'city', None):
        return 'city'
    if getattr(a, 'county', None):
        return 'county'
    return 'statewide_baseline'


def _resolved_layers_from_policy(policy: dict[str, Any]) -> list[dict[str, Any]]:
    assertions = list(policy.get('resolved_assertions') or [])
    if not assertions:
        return []
    by_layer: dict[str, dict[str, Any]] = {}
    for row in assertions:
        layer = _layer_name_for_assertion(type('Obj', (), row)()) if isinstance(row, dict) else _layer_name_for_assertion(row)
        item = by_layer.setdefault(layer, {'layer': layer, 'rule_keys': [], 'count': 0})
        rule_key = row.get('rule_key') if isinstance(row, dict) else getattr(row, 'rule_key', None)
        if rule_key and rule_key not in item['rule_keys']:
            item['rule_keys'].append(rule_key)
        item['count'] += 1
    order = {'statewide_baseline': 0, 'county': 1, 'city': 2, 'housing_authority': 3, 'org_override': 4}
    return sorted(by_layer.values(), key=lambda x: order.get(x['layer'], 99))


def resolve_profile(
    db: Session,
    *,
    org_id: Optional[int],
    city: Optional[str] = None,
    county: Optional[str] = None,
    state: str = 'MI',
):
    out = _base_resolve_profile(db, org_id=org_id, city=city, county=county, state=state)
    policy = dict(out.get('policy') or {})
    layers = _resolved_layers_from_policy(policy)
    out['resolved_layers'] = layers
    out['resolved_rule_version'] = _policy_hash({
        'state': out.get('state'),
        'county': out.get('county'),
        'city': out.get('city'),
        'pha_name': out.get('pha_name'),
        'policy': policy,
        'layers': layers,
    })
    out['coverage_confidence'] = (policy.get('coverage_confidence') or 'medium')
    out['missing_local_rule_areas'] = list(policy.get('missing_local_rule_areas') or policy.get('missing_categories') or [])
    out['source_evidence'] = list(policy.get('source_evidence') or [])
    return out


def resolve_operational_policy(
    db: Session,
    *,
    org_id: Optional[int],
    city: Optional[str] = None,
    county: Optional[str] = None,
    state: str = 'MI',
    pha_name: Optional[str] = None,
):
    policy = _base_resolve_operational_policy(
        db,
        org_id=org_id,
        city=city,
        county=county,
        state=state,
        pha_name=pha_name,
    )
    layers = _resolved_layers_from_policy(policy if isinstance(policy, dict) else {})
    if isinstance(policy, dict):
        policy['resolved_layers'] = layers
        policy['resolved_rule_version'] = _policy_hash(policy)
        policy['missing_local_rule_areas'] = list(policy.get('missing_local_rule_areas') or policy.get('missing_categories') or [])
        policy['source_evidence'] = list(policy.get('source_evidence') or [])
        universe = _expected_rule_universe_payload(
            state=_norm_state(state),
            county=_norm_county(county),
            city=_norm_city(city),
            pha_name=_norm(pha_name) or policy.get('pha_name'),
            include_section8=True,
            tenant_waitlist_depth=((policy.get('brief') or {}).get('tenant_waitlist_depth') if isinstance(policy.get('brief'), dict) else None),
        )
        policy['expected_rule_universe'] = universe
        policy['required_categories'] = list(policy.get('required_categories') or universe.get('required_categories') or [])
        policy['critical_categories'] = list(policy.get('critical_categories') or universe.get('critical_categories') or [])
        policy['optional_categories'] = list(policy.get('optional_categories') or universe.get('optional_categories') or [])
        policy['jurisdiction_types'] = list(policy.get('jurisdiction_types') or universe.get('jurisdiction_types') or [])
    return policy


def summarize_profile(
    db: Session,
    *,
    org_id: Optional[int],
    city: Optional[str],
    county: Optional[str],
    state: str = 'MI',
    pha_name: Optional[str] = None,
) -> dict[str, Any]:
    out = _base_summarize_profile(
        db,
        org_id=org_id,
        city=city,
        county=county,
        state=state,
        pha_name=pha_name,
    )
    policy = dict(out.get('policy') or {})
    out['resolved_layers'] = _resolved_layers_from_policy(policy)
    out['resolved_rule_version'] = _policy_hash(policy)
    out['coverage_confidence'] = policy.get('coverage_confidence') or 'medium'
    out['missing_local_rule_areas'] = list(out.get('missing_local_rule_areas') or policy.get('missing_local_rule_areas') or policy.get('missing_categories') or [])
    out['source_evidence'] = list(policy.get('source_evidence') or [])
    universe = policy.get('expected_rule_universe') or _expected_rule_universe_payload(
        state=_norm_state(state),
        county=_norm_county(county),
        city=_norm_city(city),
        pha_name=_norm(pha_name) or out.get('pha_name'),
        include_section8=True,
    )
    out['expected_rule_universe'] = universe
    out['required_categories'] = list(out.get('required_categories') or universe.get('required_categories') or [])
    out['critical_categories'] = list(out.get('critical_categories') or universe.get('critical_categories') or [])
    out['optional_categories'] = list(out.get('optional_categories') or universe.get('optional_categories') or [])
    out['jurisdiction_types'] = list(out.get('jurisdiction_types') or universe.get('jurisdiction_types') or [])
    return out