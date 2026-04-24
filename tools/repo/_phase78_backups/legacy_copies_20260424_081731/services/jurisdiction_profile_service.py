# backend/app/services/jurisdiction_profile_service.py
from __future__ import annotations

import copy
import hashlib
import json
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from onehaven_platform.backend.src.domain.policy.categories import expected_rule_universe_for_scope
from onehaven_platform.backend.src.domain.policy.defaults import default_policy_for_scope
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
        create_kwargs = {
            "org_id": org_id,
            "state": st,
            "county": cnty,
            "city": cty,
            "friction_multiplier": float(friction_multiplier or 1.0),
            "policy_json": _dumps(policy or {}),
            "notes": _norm(notes),
            "updated_at": now,
        }
        if hasattr(JurisdictionProfile, "pha_name"):
            create_kwargs["pha_name"] = _norm(pha_name)
        row = JurisdictionProfile(**create_kwargs)
        if hasattr(row, "required_categories_json") and isinstance(policy, dict):
            row.required_categories_json = _dumps(policy.get("required_categories") or [])
        db.add(row)
    else:
        row.friction_multiplier = float(friction_multiplier or 1.0)
        if hasattr(row, "pha_name"):
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
    from products.compliance.backend.src.services.compliance_engine.projection_service import build_property_compliance_brief

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


# ---- Chunk 7 profile-level completeness rollup helpers ----
def _policy_json_dict(profile: JurisdictionProfile | None) -> dict[str, Any]:
    if profile is None:
        return {}
    raw = _loads(getattr(profile, "policy_json", None), {})
    return raw if isinstance(raw, dict) else {}


def _policy_meta(profile: JurisdictionProfile | None) -> dict[str, Any]:
    policy = _policy_json_dict(profile)
    meta = policy.get("meta") or {}
    return meta if isinstance(meta, dict) else {}


def merge_profile_policy_meta(
    profile: JurisdictionProfile,
    meta_updates: dict[str, Any],
) -> JurisdictionProfile:
    policy = _policy_json_dict(profile)
    current_meta = _policy_meta(profile)
    current_meta = _deep_merge(current_meta, meta_updates or {})
    policy["meta"] = current_meta
    profile.policy_json = _dumps(policy)
    return profile


def set_profile_operational_rollup(
    profile: JurisdictionProfile,
    *,
    completeness: dict[str, Any],
    commit_hash_source: Optional[str] = None,
) -> JurisdictionProfile:
    policy = _policy_json_dict(profile)
    meta = _policy_meta(profile)

    completeness_summary = {
        "completeness_score": completeness.get("completeness_score"),
        "completeness_status": completeness.get("completeness_status"),
        "confidence_label": completeness.get("confidence_label")
        or completeness.get("coverage_confidence"),
        "production_readiness": completeness.get("production_readiness"),
        "trustworthy_for_projection": bool(
            completeness.get("trustworthy_for_projection", False)
        ),
        "required_categories": list(completeness.get("required_categories") or []),
        "covered_categories": list(completeness.get("covered_categories") or []),
        "missing_categories": list(completeness.get("missing_categories") or []),
        "stale_categories": list(completeness.get("stale_categories") or []),
        "inferred_categories": list(completeness.get("inferred_categories") or []),
        "conflicting_categories": list(completeness.get("conflicting_categories") or []),
        "discovery_status": completeness.get("discovery_status"),
        "last_refresh": completeness.get("last_refresh")
        or completeness.get("last_refreshed"),
        "last_refreshed": completeness.get("last_refreshed")
        or completeness.get("last_refresh"),
        "last_discovery_run": completeness.get("last_discovery_run"),
        "last_discovered_at": completeness.get("last_discovered_at"),
        "last_verified_at": completeness.get("last_verified_at"),
    }

    meta["completeness"] = completeness_summary
    meta["coverage_confidence"] = completeness_summary["confidence_label"]
    meta["missing_local_rule_areas"] = list(
        completeness_summary.get("missing_categories") or []
    )
    meta["last_refreshed"] = completeness_summary.get("last_refreshed")
    meta["discovery_status"] = completeness_summary.get("discovery_status")
    meta["last_discovery_run"] = completeness_summary.get("last_discovery_run")
    meta["last_discovered_at"] = completeness_summary.get("last_discovered_at")
    meta["production_readiness"] = completeness_summary.get("production_readiness")
    meta["trustworthy_for_projection"] = bool(
        completeness_summary.get("trustworthy_for_projection", False)
    )

    if commit_hash_source:
        meta["resolved_rule_version"] = hashlib.sha1(
            commit_hash_source.encode("utf-8")
        ).hexdigest()[:12]
    elif not meta.get("resolved_rule_version"):
        meta["resolved_rule_version"] = hashlib.sha1(
            json.dumps(
                {
                    "profile_id": getattr(profile, "id", None),
                    "state": getattr(profile, "state", None),
                    "county": getattr(profile, "county", None),
                    "city": getattr(profile, "city", None),
                    "pha_name": getattr(profile, "pha_name", None),
                    "completeness": completeness_summary,
                },
                sort_keys=True,
                ensure_ascii=False,
                default=str,
            ).encode("utf-8")
        ).hexdigest()[:12]

    policy["meta"] = meta
    profile.policy_json = _dumps(policy)
    return profile



# ---- Step 2 jurisdiction registry + source family mapping overlays ----
try:
    from onehaven_platform.backend.src.domain.policy.categories import (
        authority_scope_for_categories,
        required_source_families_for_categories,
    )
except Exception:
    authority_scope_for_categories = None  # type: ignore[assignment]
    required_source_families_for_categories = None  # type: ignore[assignment]


def _coverage_confidence_to_score(value: Any) -> float:
    raw = str(value or '').strip().lower()
    if raw == 'high':
        return 0.95
    if raw == 'medium':
        return 0.75
    if raw == 'low':
        return 0.55
    return 0.65


def _registry_category_to_source_family(category: str) -> str:
    c = str(category or '').strip().lower()
    mapping = {
        'rental_license': 'rental_registration',
        'registration': 'rental_registration',
        'inspection': 'rental_inspection',
        'occupancy': 'certificate_of_occupancy',
        'permits': 'permits_building',
        'fees': 'fees_forms',
        'documents': 'fees_forms',
        'contacts': 'contact',
        'program_overlay': 'program_overlay',
        'section8': 'program_overlay',
        'safety': 'local_code',
        'lead': 'local_code',
        'source_of_income': 'local_code',
        'zoning': 'local_code',
        'tax': 'local_code',
        'utilities': 'local_code',
    }
    return mapping.get(c, 'local_code')


def _flatten_source_evidence(policy: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for key in ('source_evidence', 'evidence'):
        value = policy.get(key)
        if isinstance(value, list):
            for row in value:
                if isinstance(row, dict):
                    out.append(dict(row))
        elif isinstance(value, dict):
            for nested in value.values():
                if isinstance(nested, list):
                    for row in nested:
                        if isinstance(row, dict):
                            out.append(dict(row))
                elif isinstance(nested, str) and nested.strip():
                    out.append({'url': nested.strip(), 'label': key})
    discovered = (((policy.get('meta') or {}).get('registry') or {}).get('source_family_matrix') or [])
    if isinstance(discovered, list):
        for row in discovered:
            if isinstance(row, dict):
                out.append(dict(row))
    return out


def _guess_official_website(policy: dict[str, Any], *, fallback: str | None = None) -> str | None:
    candidates: list[str] = []
    for key in ('official_website', 'official_site', 'official_url', 'website', 'url'):
        value = policy.get(key)
        if isinstance(value, str) and value.strip():
            candidates.append(value.strip())
    for row in _flatten_source_evidence(policy):
        url = str(row.get('url') or '').strip()
        if not url:
            continue
        if bool(row.get('is_official')) or 'official' in str(row.get('authority_level') or '').lower() or 'city' in url or '.gov' in url or '.org' in url:
            candidates.append(url)
    if fallback:
        candidates.append(fallback)
    for url in candidates:
        if url.startswith('http://') or url.startswith('https://'):
            return url
    return None


def _matching_evidence_for_category(policy: dict[str, Any], category: str) -> list[dict[str, Any]]:
    c = str(category or '').strip().lower()
    hits: list[dict[str, Any]] = []
    needles = {c, _registry_category_to_source_family(c)}
    if c == 'rental_license':
        needles.update({'rental', 'license', 'registration'})
    elif c == 'inspection':
        needles.update({'inspection', 'nspire', 'hqs'})
    elif c == 'occupancy':
        needles.update({'occupancy', 'certificate'})
    elif c == 'permits':
        needles.update({'permit', 'building'})
    elif c == 'fees':
        needles.update({'fee', 'fees', 'form', 'packet'})
    elif c == 'section8':
        needles.update({'section 8', 'voucher', 'pha', 'housing authority'})
    for row in _flatten_source_evidence(policy):
        hay = ' '.join([
            str(row.get('label') or ''),
            str(row.get('title') or ''),
            str(row.get('category') or ''),
            str(row.get('source_kind') or ''),
            str(row.get('publisher') or ''),
            str(row.get('url') or ''),
        ]).lower()
        if any(n in hay for n in needles):
            hits.append(row)
    return hits


def _dedupe_source_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        url = str(row.get('source_url') or row.get('url') or '').strip()
        category = str(row.get('category') or '').strip().lower()
        key = (category, url)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _compute_registry_source_matrix(
    *,
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
    policy: dict[str, Any],
) -> dict[str, Any]:
    universe = policy.get('expected_rule_universe') or _expected_rule_universe_payload(
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        include_section8=True,
        tenant_waitlist_depth=((policy.get('operations') or {}).get('tenant_waitlist_depth') if isinstance(policy.get('operations'), dict) else None),
    )
    required_categories = list(policy.get('required_categories') or universe.get('required_categories') or [])
    critical_categories = set(policy.get('critical_categories') or universe.get('critical_categories') or [])
    authority_scope = dict(policy.get('authority_scope_by_category') or universe.get('authority_scope_by_category') or {})
    required_source_families = dict(policy.get('required_source_families_by_category') or universe.get('required_source_families_by_category') or {})
    official_site = _guess_official_website(policy)
    rows: list[dict[str, Any]] = []
    for category in required_categories:
        families = list(required_source_families.get(category) or [])
        mapped_family = _registry_category_to_source_family(category)
        if mapped_family not in families:
            families.append(mapped_family)
        evidence_rows = _matching_evidence_for_category(policy, category)
        source_url = None
        source_label = None
        source_kind = None
        publisher_name = None
        publisher_type = None
        authority_level = 'authoritative_official' if authority_scope.get(category) in {'federal', 'state', 'county', 'city', 'local'} else 'approved_official_supporting'
        is_official = False
        if evidence_rows:
            top = evidence_rows[0]
            source_url = str(top.get('url') or '').strip() or official_site
            source_label = str(top.get('label') or top.get('title') or category).strip() or category
            source_kind = str(top.get('source_kind') or '').strip().lower() or None
            publisher_name = str(top.get('publisher') or top.get('publisher_name') or '').strip() or None
            publisher_type = str(top.get('publisher_type') or '').strip().lower() or None
            authority_level = str(top.get('authority_level') or authority_level).strip().lower() or authority_level
            is_official = bool(top.get('is_official')) or bool(source_url and ('.gov' in source_url or '.us/' in source_url or 'cityof' in source_url))
        else:
            source_url = official_site
            source_label = f'{category.replace("_", " ").title()} official source' if official_site else f'{category.replace("_", " ").title()} source pending confirmation'
            source_kind = 'official_site' if official_site else 'manual'
            publisher_name = city.title() if city else county.title() + ' County' if county else state
            publisher_type = 'municipality' if city else 'county' if county else 'state'
            is_official = bool(official_site)
        rows.append({
            'category': category,
            'source_family_category': mapped_family,
            'required_source_families': families,
            'authority_scope': authority_scope.get(category),
            'critical': category in critical_categories,
            'source_url': source_url,
            'source_label': source_label,
            'source_kind': source_kind,
            'publisher_name': publisher_name,
            'publisher_type': publisher_type,
            'authority_level': authority_level,
            'is_official': is_official,
            'coverage_hint': f"{category} should resolve from {', '.join(families)}",
        })
    rows = _dedupe_source_rows(rows)
    return {
        'official_website': official_site,
        'required_categories': required_categories,
        'critical_categories': list(critical_categories),
        'source_family_matrix': rows,
    }


def ensure_registry_source_mapping(
    db: Session,
    *,
    org_id: int | None,
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
    policy: dict[str, Any],
) -> dict[str, Any]:
    try:
        from products.compliance.backend.src.services.jurisdiction_registry_service import (
            JURISDICTION_TYPE_CITY,
            JURISDICTION_TYPE_COUNTY,
            JURISDICTION_TYPE_PHA,
            JURISDICTION_TYPE_STATE,
            ONBOARDING_DISCOVERED,
            ONBOARDING_SITE_CONFIRMED,
            ONBOARDING_SOURCE_MAPPED,
            get_or_create_jurisdiction,
            mark_onboarding_status,
            resolve_jurisdiction_hierarchy,
        )
        from products.compliance.backend.src.services.jurisdiction_source_family_service import (
            FETCH_MODE_HTML,
            FETCH_MODE_MANUAL,
            SOURCE_STATUS_ACTIVE,
            upsert_source_family,
            get_source_families_for_jurisdiction,
        )
    except Exception:
        return {'registry_enabled': False, 'official_website': _guess_official_website(policy), 'source_family_matrix': (_compute_registry_source_matrix(state=state, county=county, city=city, pha_name=pha_name, policy=policy).get('source_family_matrix') or [])}

    st = _norm_state(state)
    cnty = _norm_county(county)
    cty = _norm_city(city)
    scope = _compute_registry_source_matrix(state=st, county=cnty, city=cty, pha_name=_norm(pha_name), policy=policy)
    official_website = scope.get('official_website')

    state_row = get_or_create_jurisdiction(
        db,
        jurisdiction_type=JURISDICTION_TYPE_STATE,
        state_code=st,
        state_name=st,
        official_website=official_website if not cnty and not cty else None,
        onboarding_status=ONBOARDING_SITE_CONFIRMED if official_website and not cnty and not cty else ONBOARDING_DISCOVERED,
        source_confidence=_coverage_confidence_to_score((policy.get('coverage') or {}).get('coverage_confidence') or policy.get('coverage_confidence')),
        org_id=org_id,
    )
    county_row = None
    city_row = None
    pha_row = None
    if cnty:
        county_row = get_or_create_jurisdiction(
            db,
            jurisdiction_type=JURISDICTION_TYPE_COUNTY,
            state_code=st,
            county_name=cnty,
            parent_jurisdiction_id=int(state_row.id),
            official_website=official_website if not cty else None,
            onboarding_status=ONBOARDING_SITE_CONFIRMED if official_website and not cty else ONBOARDING_DISCOVERED,
            source_confidence=_coverage_confidence_to_score((policy.get('coverage') or {}).get('coverage_confidence') or policy.get('coverage_confidence')),
            org_id=org_id,
        )
    if cty:
        city_row = get_or_create_jurisdiction(
            db,
            jurisdiction_type=JURISDICTION_TYPE_CITY,
            state_code=st,
            county_name=cnty,
            city_name=cty,
            parent_jurisdiction_id=int(county_row.id if county_row else state_row.id),
            official_website=official_website,
            onboarding_status=ONBOARDING_SITE_CONFIRMED if official_website else ONBOARDING_DISCOVERED,
            source_confidence=_coverage_confidence_to_score((policy.get('coverage') or {}).get('coverage_confidence') or policy.get('coverage_confidence')),
            org_id=org_id,
        )
    if _norm(pha_name):
        pha_row = get_or_create_jurisdiction(
            db,
            jurisdiction_type=JURISDICTION_TYPE_PHA,
            state_code=st,
            county_name=cnty,
            city_name=_norm(pha_name),
            parent_jurisdiction_id=int(city_row.id if city_row else county_row.id if county_row else state_row.id),
            official_website=official_website,
            onboarding_status=ONBOARDING_SITE_CONFIRMED if official_website else ONBOARDING_DISCOVERED,
            source_confidence=_coverage_confidence_to_score((policy.get('coverage') or {}).get('coverage_confidence') or policy.get('coverage_confidence')),
            org_id=org_id,
        )

    target = pha_row or city_row or county_row or state_row
    mapped_rows: list[dict[str, Any]] = []
    if target is not None:
        for row in scope.get('source_family_matrix') or []:
            fetch_mode = FETCH_MODE_HTML if row.get('source_url') else FETCH_MODE_MANUAL
            saved = upsert_source_family(
                db,
                jurisdiction_id=int(target.id),
                category=str(row.get('source_family_category') or 'local_code'),
                source_url=row.get('source_url'),
                source_label=row.get('source_label'),
                source_kind=row.get('source_kind'),
                publisher_name=row.get('publisher_name'),
                publisher_type=row.get('publisher_type'),
                authority_level=row.get('authority_level'),
                fetch_mode=fetch_mode,
                status=SOURCE_STATUS_ACTIVE,
                is_official=bool(row.get('is_official')),
                is_active=True,
                notes=row.get('coverage_hint'),
                coverage_hint=row.get('coverage_hint'),
                review_state='seeded',
            )
            mapped_rows.append({
                'id': int(saved.id),
                'category': row.get('category'),
                'source_family_category': saved.category,
                'source_url': saved.source_url,
                'source_label': saved.source_label,
                'authority_level': saved.authority_level,
                'is_official': bool(saved.is_official),
                'coverage_hint': saved.coverage_hint,
            })
        final_status = ONBOARDING_SOURCE_MAPPED if mapped_rows else ONBOARDING_SITE_CONFIRMED if official_website else ONBOARDING_DISCOVERED
        mark_onboarding_status(db, jurisdiction_id=int(target.id), onboarding_status=final_status)
        hierarchy = resolve_jurisdiction_hierarchy(db, state_code=st, county_name=cnty, city_name=cty)
    else:
        hierarchy = {'state': None, 'county': None, 'city': None}
        final_status = ONBOARDING_DISCOVERED

    return {
        'registry_enabled': True,
        'official_website': official_website,
        'onboarding_status': final_status,
        'jurisdiction_id': int(target.id) if target is not None else None,
        'jurisdiction_slug': getattr(target, 'slug', None) if target is not None else None,
        'registry_hierarchy': {
            'state': {'id': int(hierarchy['state'].id), 'slug': hierarchy['state'].slug, 'display_name': hierarchy['state'].display_name} if hierarchy.get('state') else None,
            'county': {'id': int(hierarchy['county'].id), 'slug': hierarchy['county'].slug, 'display_name': hierarchy['county'].display_name} if hierarchy.get('county') else None,
            'city': {'id': int(hierarchy['city'].id), 'slug': hierarchy['city'].slug, 'display_name': hierarchy['city'].display_name} if hierarchy.get('city') else None,
            'pha': {'id': int(pha_row.id), 'slug': pha_row.slug, 'display_name': pha_row.display_name} if pha_row is not None else None,
        },
        'source_family_matrix': mapped_rows or list(scope.get('source_family_matrix') or []),
        'required_categories': list(scope.get('required_categories') or []),
        'critical_categories': list(scope.get('critical_categories') or []),
    }


_step2_base_resolve_profile = resolve_profile
_step2_base_upsert_profile = upsert_profile
_step2_base_summarize_profile = summarize_profile


def resolve_profile(
    db: Session,
    *,
    org_id: Optional[int],
    city: Optional[str] = None,
    county: Optional[str] = None,
    state: str = 'MI',
):
    out = _step2_base_resolve_profile(db, org_id=org_id, city=city, county=county, state=state)
    policy = dict(out.get('policy') or {})
    registry = ensure_registry_source_mapping(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=out.get('pha_name'),
        policy=policy,
    )
    meta = dict(policy.get('meta') or {})
    meta['registry'] = registry
    policy['meta'] = meta
    out['policy'] = policy
    out['official_website'] = registry.get('official_website')
    out['onboarding_status'] = registry.get('onboarding_status')
    out['registry_hierarchy'] = registry.get('registry_hierarchy')
    out['source_family_matrix'] = registry.get('source_family_matrix')
    return out


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
    row = _step2_base_upsert_profile(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        friction_multiplier=friction_multiplier,
        pha_name=pha_name,
        policy=policy,
        notes=notes,
    )
    policy_dict = _loads(getattr(row, 'policy_json', None), {})
    if not isinstance(policy_dict, dict):
        policy_dict = {}
    registry = ensure_registry_source_mapping(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        policy=policy_dict,
    )
    meta = dict(policy_dict.get('meta') or {})
    meta['registry'] = registry
    policy_dict['meta'] = meta
    if registry.get('official_website') and not policy_dict.get('official_website'):
        policy_dict['official_website'] = registry.get('official_website')
    row.policy_json = _dumps(policy_dict)
    row.updated_at = datetime.utcnow()
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def summarize_profile(
    db: Session,
    *,
    org_id: Optional[int],
    city: Optional[str],
    county: Optional[str],
    state: str = 'MI',
    pha_name: Optional[str] = None,
) -> dict[str, Any]:
    out = _step2_base_summarize_profile(
        db,
        org_id=org_id,
        city=city,
        county=county,
        state=state,
        pha_name=pha_name,
    )
    policy = dict(out.get('policy') or {})
    registry = ensure_registry_source_mapping(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        policy=policy,
    )
    out['official_website'] = registry.get('official_website')
    out['onboarding_status'] = registry.get('onboarding_status')
    out['registry_hierarchy'] = registry.get('registry_hierarchy')
    out['source_family_matrix'] = registry.get('source_family_matrix')
    return out
