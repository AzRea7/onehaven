from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Property
from app.policy_models import HqsAddendum, HqsRule

from .inspection_rules import criteria_as_dicts, normalize_rule_code, normalize_severity


def _baseline_hqs_items() -> list[dict[str, Any]]:
    """
    Full baseline catalog derived from the uploaded HUD-52580-A inspection form.
    """
    return criteria_as_dicts()


def _normalize_item(item: dict[str, Any]) -> dict[str, Any]:
    code = normalize_rule_code(item.get("code") or item.get("rule_key") or "")
    description = str(item.get("description") or item.get("label") or code.replace("_", " ").title()).strip()
    category = str(item.get("category") or "other").strip().lower() or "other"
    severity = normalize_severity(item.get("severity") or "fail")
    suggested_fix = str(item.get("suggested_fix")).strip() if item.get("suggested_fix") else None
    standard_label = str(item.get("standard_label")).strip() if item.get("standard_label") else None
    standard_citation = str(item.get("standard_citation")).strip() if item.get("standard_citation") else None
    fail_reason_hint = str(item.get("fail_reason_hint")).strip() if item.get("fail_reason_hint") else None
    common_fail = bool(item.get("common_fail", True))
    template_key = str(item.get("template_key") or "hud_52580a").strip() or "hud_52580a"
    template_version = str(item.get("template_version") or "hud_52580a_2019").strip() or "hud_52580a_2019"
    sort_order = int(item.get("sort_order", 0) or 0)
    section = str(item.get("section") or "").strip().lower() or None
    item_number = str(item.get("item_number") or "").strip() or None
    room_scope = str(item.get("room_scope") or "").strip().lower() or None
    not_applicable_allowed = bool(item.get("not_applicable_allowed", False))

    return {
        "code": code,
        "description": description,
        "category": category,
        "severity": severity,
        "suggested_fix": suggested_fix,
        "fail_reason_hint": fail_reason_hint,
        "standard_label": standard_label,
        "standard_citation": standard_citation,
        "common_fail": common_fail,
        "template_key": template_key,
        "template_version": template_version,
        "sort_order": sort_order,
        "section": section,
        "item_number": item_number,
        "room_scope": room_scope,
        "not_applicable_allowed": not_applicable_allowed,
        "source": item.get("source"),
    }


def _load_hqs_rule_rows(db: Session) -> list[Any]:
    try:
        return list(db.scalars(select(HqsRule)).all())
    except Exception:
        return []


def _load_hqs_addendum_rows(db: Session, *, org_id: int | None = None) -> list[Any]:
    try:
        if hasattr(HqsAddendum, "org_id") and org_id is not None:
            return list(
                db.scalars(
                    select(HqsAddendum).where(
                        (HqsAddendum.org_id == org_id) | (HqsAddendum.org_id.is_(None))
                    )
                ).all()
            )
        return list(db.scalars(select(HqsAddendum)).all())
    except Exception:
        return []


def _profile_hqs_items(profile_summary: dict[str, Any]) -> list[dict[str, Any]]:
    policy = profile_summary.get("policy") or {}
    if not isinstance(policy, dict):
        return []

    out: list[dict[str, Any]] = []
    raw_items = (
        policy.get("hqs_addenda")
        or policy.get("hqs_overrides")
        or policy.get("inspection_items")
        or []
    )

    if isinstance(raw_items, list):
        for raw in raw_items:
            if not isinstance(raw, dict):
                continue
            code = normalize_rule_code(raw.get("code") or raw.get("rule_key") or "")
            if not code:
                continue
            out.append(
                _normalize_item(
                    {
                        "code": code,
                        "description": raw.get("description") or raw.get("label") or raw.get("title"),
                        "category": raw.get("category") or "jurisdiction",
                        "severity": raw.get("severity") or "fail",
                        "suggested_fix": raw.get("suggested_fix") or raw.get("fix"),
                        "fail_reason_hint": raw.get("fail_reason_hint") or raw.get("reason_hint"),
                        "standard_label": raw.get("standard_label"),
                        "standard_citation": raw.get("standard_citation"),
                        "template_key": raw.get("template_key") or "hud_52580a",
                        "template_version": raw.get("template_version") or "hud_52580a_2019",
                        "sort_order": raw.get("sort_order") or 10_000,
                        "section": raw.get("section"),
                        "item_number": raw.get("item_number"),
                        "room_scope": raw.get("room_scope"),
                        "not_applicable_allowed": raw.get("not_applicable_allowed", False),
                        "common_fail": raw.get("common_fail", True),
                        "source": {"type": "jurisdiction_policy", "name": "profile_hqs_item"},
                    }
                )
            )

    compliance = policy.get("compliance") or {}
    if isinstance(compliance, dict):
        if str(compliance.get("inspection_required") or "").strip().lower() in {"yes", "true", "required", "1"}:
            out.append(
                _normalize_item(
                    {
                        "code": "LOCAL_INSPECTION_REQUIRED",
                        "description": "Jurisdiction requires local rental inspection readiness",
                        "category": "jurisdiction",
                        "severity": "fail",
                        "suggested_fix": "Prepare the unit for local rental inspection and complete jurisdiction-specific inspection steps.",
                        "fail_reason_hint": "Local inspection readiness requirement not satisfied.",
                        "standard_label": "Local inspection requirement",
                        "standard_citation": "Local jurisdiction policy",
                        "template_key": "hud_52580a",
                        "template_version": "hud_52580a_2019",
                        "sort_order": 20_000,
                        "section": "jurisdiction_overlay",
                        "item_number": "J.1",
                        "source": {"type": "jurisdiction_policy", "name": "inspection_required"},
                    }
                )
            )

    return out


def _contextual_items(prop: Property, profile_summary: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    year_built = getattr(prop, "year_built", None)
    if isinstance(year_built, int) and year_built < 1978:
        out.append(
            _normalize_item(
                {
                    "code": "PRE_1978_LEAD_RISK_SCREEN",
                    "description": "Pre-1978 property should be screened carefully for deteriorated paint / lead-safe compliance triggers",
                    "category": "lead",
                    "severity": "warn",
                    "suggested_fix": "Verify lead-safe workflow, stabilization, clearance rules, and required owner certification where applicable.",
                    "fail_reason_hint": "Potential pre-1978 lead-risk condition requires verification.",
                    "standard_label": "Pre-1978 lead risk screen",
                    "standard_citation": "HUD lead-based paint applicability",
                    "template_key": "hud_52580a",
                    "template_version": "hud_52580a_2019",
                    "sort_order": 30_000,
                    "section": "contextual",
                    "item_number": "C.1",
                    "source": {"type": "contextual_rule", "reason": "pre_1978"},
                }
            )
        )

    if getattr(prop, "property_type", "") == "manufactured_home":
        out.append(
            _normalize_item(
                {
                    "code": "BUILDING_EXTERIOR_MANUFACTURED_HOMES_TIE_DOWNS",
                    "description": "Manufactured home tie-down / anchoring should be verified",
                    "category": "structure",
                    "severity": "critical",
                    "suggested_fix": "Inspect and repair manufactured-home anchoring, tie-downs, and ground attachment.",
                    "fail_reason_hint": "Manufactured home anchoring may be unsafe or missing.",
                    "standard_label": "Manufactured home tie-downs",
                    "standard_citation": "HUD-52580-A 6.7",
                    "template_key": "hud_52580a",
                    "template_version": "hud_52580a_2019",
                    "sort_order": 30_100,
                    "section": "building_exterior",
                    "item_number": "6.7",
                    "source": {"type": "contextual_rule", "reason": "manufactured_home"},
                }
            )
        )

    policy = profile_summary.get("policy") or {}
    compliance = policy.get("compliance") or {}
    if isinstance(compliance, dict):
        if str(compliance.get("local_agent_required") or "").strip().lower() in {"yes", "true", "required", "1"}:
            out.append(
                _normalize_item(
                    {
                        "code": "LOCAL_AGENT_DOCUMENTATION",
                        "description": "Local agent / responsible party documentation should be ready for inspection packet",
                        "category": "documents",
                        "severity": "warn",
                        "suggested_fix": "Prepare valid local agent or responsible party information required by the jurisdiction.",
                        "fail_reason_hint": "Local agent / responsible party documentation missing.",
                        "standard_label": "Local agent documentation",
                        "standard_citation": "Local jurisdiction policy",
                        "template_key": "hud_52580a",
                        "template_version": "hud_52580a_2019",
                        "sort_order": 30_200,
                        "section": "contextual",
                        "item_number": "C.2",
                        "source": {"type": "contextual_rule", "reason": "local_agent_required"},
                    }
                )
            )

    return out


def get_effective_hqs_items(
    db: Session,
    *,
    org_id: int,
    prop: Property,
    profile_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Effective inspection rule set:
      1) full HUD-52580-A baseline
      2) HqsRule policy table overrides/extensions
      3) HqsAddendum policy table overrides/extensions
      4) jurisdiction profile adds
      5) contextual property adds
    """
    profile_summary = profile_summary or {}
    baseline_items = _baseline_hqs_items()

    items: dict[str, dict[str, Any]] = {
        row["code"]: _normalize_item(
            {
                **row,
                "source": {"type": "baseline_internal", "name": "HUD-52580-A full baseline"},
            }
        )
        for row in baseline_items
    }

    sources: list[dict[str, Any]] = [
        {"type": "baseline_internal", "name": "HUD-52580-A full baseline", "count": len(items)}
    ]

    rule_rows = _load_hqs_rule_rows(db)
    for row in rule_rows:
        code = normalize_rule_code(getattr(row, "code", "") or "")
        if not code:
            continue
        prior = items.get(code, {})
        items[code] = _normalize_item(
            {
                "code": code,
                "description": getattr(row, "description", None) or prior.get("description") or code.replace("_", " ").title(),
                "category": getattr(row, "category", None) or prior.get("category") or "other",
                "severity": getattr(row, "severity", None) or prior.get("severity") or "fail",
                "suggested_fix": getattr(row, "suggested_fix", None)
                or getattr(row, "remediation_guidance", None)
                or prior.get("suggested_fix"),
                "fail_reason_hint": getattr(row, "fail_reason_hint", None) or prior.get("fail_reason_hint"),
                "standard_label": getattr(row, "standard_label", None) or prior.get("standard_label"),
                "standard_citation": getattr(row, "standard_citation", None) or prior.get("standard_citation"),
                "template_key": getattr(row, "template_key", None) or prior.get("template_key") or "hud_52580a",
                "template_version": getattr(row, "template_version", None) or prior.get("template_version") or "hud_52580a_2019",
                "sort_order": getattr(row, "sort_order", None) or prior.get("sort_order") or 40_000,
                "section": getattr(row, "section", None) or prior.get("section"),
                "item_number": getattr(row, "item_number", None) or prior.get("item_number"),
                "room_scope": getattr(row, "room_scope", None) or prior.get("room_scope"),
                "not_applicable_allowed": getattr(row, "not_applicable_allowed", None)
                if getattr(row, "not_applicable_allowed", None) is not None
                else prior.get("not_applicable_allowed", False),
                "common_fail": prior.get("common_fail", True),
                "source": {"type": "policy_table", "table": "HqsRule"},
            }
        )
    if rule_rows:
        sources.append({"type": "policy_table", "table": "HqsRule", "count": len(rule_rows)})

    addenda = _load_hqs_addendum_rows(db, org_id=org_id)
    for row in addenda:
        code = normalize_rule_code(getattr(row, "code", "") or "")
        if not code:
            continue
        prior = items.get(code, {})
        items[code] = _normalize_item(
            {
                "code": code,
                "description": getattr(row, "description", None) or prior.get("description") or code.replace("_", " ").title(),
                "category": getattr(row, "category", None) or prior.get("category") or "other",
                "severity": getattr(row, "severity", None) or prior.get("severity") or "fail",
                "suggested_fix": getattr(row, "suggested_fix", None)
                or getattr(row, "remediation_guidance", None)
                or prior.get("suggested_fix"),
                "fail_reason_hint": getattr(row, "fail_reason_hint", None) or prior.get("fail_reason_hint"),
                "standard_label": getattr(row, "standard_label", None) or prior.get("standard_label"),
                "standard_citation": getattr(row, "standard_citation", None) or prior.get("standard_citation"),
                "template_key": getattr(row, "template_key", None) or prior.get("template_key") or "hud_52580a",
                "template_version": getattr(row, "template_version", None) or prior.get("template_version") or "hud_52580a_2019",
                "sort_order": getattr(row, "sort_order", None) or prior.get("sort_order") or 50_000,
                "section": getattr(row, "section", None) or prior.get("section"),
                "item_number": getattr(row, "item_number", None) or prior.get("item_number"),
                "room_scope": getattr(row, "room_scope", None) or prior.get("room_scope"),
                "not_applicable_allowed": getattr(row, "not_applicable_allowed", None)
                if getattr(row, "not_applicable_allowed", None) is not None
                else prior.get("not_applicable_allowed", False),
                "common_fail": prior.get("common_fail", True),
                "source": {"type": "policy_table", "table": "HqsAddendum"},
            }
        )
    if addenda:
        sources.append({"type": "policy_table", "table": "HqsAddendum", "count": len(addenda)})

    profile_items = _profile_hqs_items(profile_summary)
    for item in profile_items:
        items[item["code"]] = item
    if profile_items:
        sources.append({"type": "jurisdiction_policy", "name": "profile_hqs_items", "count": len(profile_items)})

    ctx_items = _contextual_items(prop, profile_summary)
    for item in ctx_items:
        items[item["code"]] = item
    if ctx_items:
        sources.append({"type": "contextual_rules", "count": len(ctx_items)})

    ordered = sorted(
        items.values(),
        key=lambda x: (
            str(x.get("template_key") or "hud_52580a"),
            str(x.get("template_version") or "hud_52580a_2019"),
            str(x.get("section") or ""),
            str(x.get("item_number") or ""),
            int(x.get("sort_order", 0) or 0),
            str(x.get("category") or ""),
            str(x.get("code") or ""),
        ),
    )

    return {
        "items": ordered,
        "sources": sources,
        "counts": {
            "total": len(items),
            "baseline": len(baseline_items),
            "profile_items": len(profile_items),
            "contextual_items": len(ctx_items),
        },
    }