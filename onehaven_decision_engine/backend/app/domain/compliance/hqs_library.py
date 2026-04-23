from __future__ import annotations

from typing import Any
import os
from pathlib import Path
import zipfile

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Property
from app.policy_models import HqsAddendum, HqsRule

from .checklist_templates import (
    ChecklistTemplateItem,
    build_property_scoped_checklist_items,
    template_items_as_dicts,
    template_items_from_effective_rules,
)
from .inspection_rules import criteria_as_dicts, normalize_rule_code, normalize_severity




def _normalize_pdf_key(value: Any) -> str:
    raw = str(value or "").strip().lower()
    raw = raw.replace("nspire-standard-", "")
    for ch in ["/", ",", ".", "(", ")", "'", '"', ":", ";"]:
        raw = raw.replace(ch, "")
    raw = raw.replace("&", "and")
    raw = raw.replace("-", "_")
    raw = "_".join(part for part in raw.split("_") if part)
    return raw


def _pdf_roots() -> list[Path]:
    candidates = [
        Path("backend/data/pdfs"),
        Path("/app/backend/data/pdfs"),
        Path("/mnt/data/step3_zip/pdfs"),
        Path("/mnt/data/pdfs"),
    ]
    env = os.getenv("NSPIRE_PDF_ROOT") or os.getenv("POLICY_PDFS_ROOT") or os.getenv("POLICY_PDF_ROOT")
    if env:
        for part in str(env).split(os.pathsep):
            if part.strip():
                candidates.append(Path(part.strip()))
    roots = []
    seen = set()
    for c in candidates:
        try:
            p = c.expanduser().resolve()
        except Exception:
            continue
        key = str(p)
        if key in seen:
            continue
        seen.add(key)
        if p.exists() and p.is_dir():
            roots.append(p)
    return roots


def _pdf_zip_paths() -> list[Path]:
    return [Path('/mnt/data/pdfs(1).zip')]


def _build_pdf_catalog() -> dict[str, dict[str, str]]:
    catalog: dict[str, dict[str, str]] = {}
    for root in _pdf_roots():
        for pdf in root.rglob('*.pdf'):
            key = _normalize_pdf_key(pdf.stem)
            if key and key not in catalog:
                catalog[key] = {"pdf_name": pdf.name, "pdf_path": str(pdf), "pdf_source": "filesystem"}
    for zpath in _pdf_zip_paths():
        if not zpath.exists():
            continue
        try:
            with zipfile.ZipFile(zpath) as zf:
                for name in zf.namelist():
                    if not name.lower().endswith('.pdf'):
                        continue
                    stem = Path(name).stem
                    key = _normalize_pdf_key(stem)
                    if key and key not in catalog:
                        catalog[key] = {"pdf_name": Path(name).name, "pdf_path": f"{zpath}:{name}", "pdf_source": "zip"}
        except Exception:
            continue
    return catalog


def _match_pdf_catalog(*values: Any) -> dict[str, str] | None:
    catalog = _build_pdf_catalog()
    probes = []
    for value in values:
        key = _normalize_pdf_key(value)
        if key:
            probes.append(key)
    for key in probes:
        if key in catalog:
            return catalog[key]
    for key in probes:
        for cat_key, payload in catalog.items():
            if key and (key in cat_key or cat_key in key):
                return payload
    return None

def _safe_import_nspire_service():
    try:
        from app.products.compliance.services.inspections.import_nspire_service import list_active_nspire_rules
        return {"list_active_nspire_rules": list_active_nspire_rules}
    except Exception:
        return {}


def _baseline_hqs_items() -> list[dict[str, Any]]:
    """
    Full baseline catalog derived from the uploaded HUD-52580-A inspection form.
    """
    return criteria_as_dicts()


def _nspire_rules(db: Session) -> list[dict[str, Any]]:
    services = _safe_import_nspire_service()
    fn = services.get("list_active_nspire_rules")
    if fn is None:
        return []
    try:
        rows = fn(db)
        return [dict(row) for row in (rows or []) if isinstance(row, dict)]
    except Exception:
        return []


def _nspire_key_variants(value: Any) -> list[str]:
    raw = normalize_rule_code(value or "")
    if not raw:
        return []
    vals = {raw}
    vals.add(raw.replace("__", "_"))
    vals.add(raw.replace("-", "_"))
    return sorted(v for v in vals if v)


def _build_nspire_index(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for row in rows or []:
        keys = set()
        for field in (
            row.get("rule_key"),
            row.get("standard_code"),
            row.get("standard_label"),
            row.get("deficiency_description"),
        ):
            for variant in _nspire_key_variants(field):
                keys.add(variant)
        for key in keys:
            index[key] = row
    return index


def _source_name_from_item(item: dict[str, Any]) -> str | None:
    source = item.get("source")
    if isinstance(source, dict):
        return str(source.get("name") or source.get("table") or source.get("type") or "").strip() or None
    return None


def _source_type_from_item(item: dict[str, Any]) -> str | None:
    source = item.get("source")
    if isinstance(source, dict):
        return str(source.get("type") or "").strip() or None
    return None


def _severity_from_nspire_designation(designation: str | None, fallback: str | None) -> str:
    raw = str(designation or "").strip().upper()
    if raw == "LT":
        return "critical"
    if raw in {"S", "M"}:
        return "fail"
    if raw == "L":
        return "warn"
    return normalize_severity(fallback or "fail")


def _enrich_item_with_nspire(item: dict[str, Any], nspire_index: dict[str, dict[str, Any]]) -> dict[str, Any]:
    code = normalize_rule_code(item.get("inspection_rule_code") or item.get("code") or "")
    match = None
    for key in (
        item.get("nspire_standard_key"),
        code,
        item.get("standard_label"),
        item.get("standard_citation"),
    ):
        for variant in _nspire_key_variants(key):
            if variant in nspire_index:
                match = nspire_index[variant]
                break
        if match is not None:
            break

    if match is None:
        return {
            **item,
            "inspection_rule_code": code or item.get("inspection_rule_code") or item.get("code"),
            "source_name": item.get("source_name") or _source_name_from_item(item),
            "source_type": item.get("source_type") or _source_type_from_item(item),
            "nspire_matched": False,
            "source_pdf_name": item.get("source_pdf_name"),
            "source_pdf_path": item.get("source_pdf_path"),
            "source_citation": item.get("source_citation") or item.get("standard_citation"),
        }

    designation = str(match.get("severity_code") or "").strip().upper() or None
    correction_days = match.get("correction_days")
    try:
        correction_days = int(correction_days) if correction_days is not None else None
    except Exception:
        correction_days = None

    pdf_match = _match_pdf_catalog(match.get("standard_label"), match.get("standard_code"), item.get("nspire_standard_label"), item.get("standard_label"), item.get("description"))
    enriched = {
        **item,
        "inspection_rule_code": code or item.get("inspection_rule_code") or item.get("code"),
        "severity": _severity_from_nspire_designation(designation, item.get("severity")),
        "standard_label": item.get("standard_label") or match.get("standard_label"),
        "standard_citation": item.get("standard_citation") or match.get("citation"),
        "nspire_standard_key": item.get("nspire_standard_key") or normalize_rule_code(match.get("rule_key") or match.get("standard_code")),
        "nspire_standard_code": item.get("nspire_standard_code") or match.get("standard_code"),
        "nspire_standard_label": item.get("nspire_standard_label") or match.get("standard_label"),
        "nspire_deficiency_description": item.get("nspire_deficiency_description") or match.get("deficiency_description"),
        "nspire_designation": item.get("nspire_designation") or designation,
        "correction_days": item.get("correction_days") if item.get("correction_days") is not None else correction_days,
        "affirmative_habitability_requirement": bool(
            item.get("affirmative_habitability_requirement")
            or (
                str(match.get("pass_fail") or "").strip().lower() == "fail"
                and designation in {"LT", "S", "M"}
            )
        ),
        "source_name": item.get("source_name") or match.get("source_name") or _source_name_from_item(item),
        "source_type": item.get("source_type") or "nspire_catalog",
        "nspire_matched": True,
        "source_pdf_name": item.get("source_pdf_name") or (pdf_match or {}).get("pdf_name"),
        "source_pdf_path": item.get("source_pdf_path") or (pdf_match or {}).get("pdf_path"),
        "source_citation": item.get("source_citation") or item.get("standard_citation") or match.get("citation"),
    }
    return enriched


def _normalize_item(item: dict[str, Any], *, nspire_index: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
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
    row = {
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
        "inspection_rule_code": normalize_rule_code(item.get("inspection_rule_code") or code),
        "nspire_standard_key": item.get("nspire_standard_key"),
        "nspire_standard_code": item.get("nspire_standard_code"),
        "nspire_standard_label": item.get("nspire_standard_label"),
        "nspire_deficiency_description": item.get("nspire_deficiency_description"),
        "nspire_designation": item.get("nspire_designation"),
        "correction_days": item.get("correction_days"),
        "affirmative_habitability_requirement": bool(item.get("affirmative_habitability_requirement", False)),
        "source_name": item.get("source_name") or _source_name_from_item(item),
        "source_type": item.get("source_type") or _source_type_from_item(item),
        "source_pdf_name": item.get("source_pdf_name"),
        "source_pdf_path": item.get("source_pdf_path"),
        "source_citation": item.get("source_citation") or standard_citation,
    }
    return _enrich_item_with_nspire(row, nspire_index or {})


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
                    select(HqsAddendum).where((HqsAddendum.org_id == org_id) | (HqsAddendum.org_id.is_(None)))
                ).all()
            )
        return list(db.scalars(select(HqsAddendum)).all())
    except Exception:
        return []


def _profile_hqs_items(profile_summary: dict[str, Any], *, nspire_index: dict[str, dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    policy = profile_summary.get("policy") or {}
    if not isinstance(policy, dict):
        return []

    out: list[dict[str, Any]] = []
    raw_items = policy.get("hqs_addenda") or policy.get("hqs_overrides") or policy.get("inspection_items") or []
    if isinstance(raw_items, list):
        for raw in raw_items:
            if not isinstance(raw, dict):
                continue
            code = normalize_rule_code(raw.get("code") or raw.get("rule_key") or "")
            if code:
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
                            "nspire_standard_key": raw.get("nspire_standard_key"),
                            "nspire_standard_code": raw.get("nspire_standard_code"),
                            "nspire_standard_label": raw.get("nspire_standard_label"),
                            "nspire_deficiency_description": raw.get("nspire_deficiency_description"),
                            "nspire_designation": raw.get("nspire_designation"),
                            "correction_days": raw.get("correction_days"),
                            "affirmative_habitability_requirement": raw.get("affirmative_habitability_requirement", False),
                        },
                        nspire_index=nspire_index,
                    )
                )

    compliance = policy.get("compliance") or {}
    if isinstance(compliance, dict) and str(compliance.get("inspection_required") or "").strip().lower() in {"yes", "true", "required", "1"}:
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
                },
                nspire_index=nspire_index,
            )
        )

    return out


def _contextual_items(prop: Property, profile_summary: dict[str, Any], *, nspire_index: dict[str, dict[str, Any]] | None = None) -> list[dict[str, Any]]:
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
                },
                nspire_index=nspire_index,
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
                    "standard_citation": "HUD inspection standard",
                    "template_key": "hud_52580a",
                    "template_version": "hud_52580a_2019",
                    "sort_order": 30_100,
                    "section": "building_exterior",
                    "item_number": "6.7",
                    "source": {"type": "contextual_rule", "reason": "manufactured_home"},
                },
                nspire_index=nspire_index,
            )
        )

    policy = profile_summary.get("policy") or {}
    compliance = policy.get("compliance") or {}
    if isinstance(compliance, dict) and str(compliance.get("local_agent_required") or "").strip().lower() in {"yes", "true", "required", "1"}:
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
                },
                nspire_index=nspire_index,
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
      6) NSPIRE enrichment metadata, when present in the imported catalog
    """
    profile_summary = profile_summary or {}
    baseline_items = _baseline_hqs_items()
    nspire_rows = _nspire_rules(db)
    nspire_index = _build_nspire_index(nspire_rows)

    items: dict[str, dict[str, Any]] = {
        normalize_rule_code(row.get("code") or ""): _normalize_item(
            {**row, "source": {"type": "baseline_internal", "name": "HUD-52580-A full baseline"}},
            nspire_index=nspire_index,
        )
        for row in baseline_items
        if normalize_rule_code(row.get("code") or "")
    }

    sources: list[dict[str, Any]] = [
        {"type": "baseline_internal", "name": "HUD-52580-A full baseline", "count": len(items)}
    ]
    if nspire_rows:
        sources.append({"type": "nspire_catalog", "name": "NSPIRE imported catalog", "count": len(nspire_rows)})

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
                "nspire_standard_key": getattr(row, "nspire_standard_key", None),
                "nspire_standard_code": getattr(row, "nspire_standard_code", None),
                "nspire_standard_label": getattr(row, "nspire_standard_label", None),
                "nspire_deficiency_description": getattr(row, "nspire_deficiency_description", None),
                "nspire_designation": getattr(row, "nspire_designation", None),
                "correction_days": getattr(row, "correction_days", None),
                "affirmative_habitability_requirement": getattr(row, "affirmative_habitability_requirement", False),
            },
            nspire_index=nspire_index,
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
                "nspire_standard_key": getattr(row, "nspire_standard_key", None),
                "nspire_standard_code": getattr(row, "nspire_standard_code", None),
                "nspire_standard_label": getattr(row, "nspire_standard_label", None),
                "nspire_deficiency_description": getattr(row, "nspire_deficiency_description", None),
                "nspire_designation": getattr(row, "nspire_designation", None),
                "correction_days": getattr(row, "correction_days", None),
                "affirmative_habitability_requirement": getattr(row, "affirmative_habitability_requirement", False),
            },
            nspire_index=nspire_index,
        )
    if addenda:
        sources.append({"type": "policy_table", "table": "HqsAddendum", "count": len(addenda)})

    profile_items = _profile_hqs_items(profile_summary, nspire_index=nspire_index)
    for item in profile_items:
        items[item["code"]] = item
    if profile_items:
        sources.append({"type": "jurisdiction_policy", "name": "profile_hqs_items", "count": len(profile_items)})

    ctx_items = _contextual_items(prop, profile_summary, nspire_index=nspire_index)
    for item in ctx_items:
        items[item["code"]] = item
    if ctx_items:
        sources.append({"type": "contextual_rule", "name": "property_context", "count": len(ctx_items)})

    ordered_items = sorted(
        items.values(),
        key=lambda row: (
            int(row.get("sort_order", 0) or 0),
            str(row.get("section") or ""),
            str(row.get("item_number") or ""),
            str(row.get("code") or ""),
        ),
    )
    matched_pdf_names = sorted({str(item.get("source_pdf_name") or "").strip() for item in ordered_items if str(item.get("source_pdf_name") or "").strip()})
    return {
        "items": ordered_items,
        "sources": sources,
        "counts": {
            "total": len(ordered_items),
            "baseline": len(baseline_items),
            "profile_items": len(profile_items),
            "contextual_items": len(ctx_items),
            "nspire_rules": len(nspire_rows),
            "nspire_enriched_items": sum(1 for row in ordered_items if row.get("nspire_matched")),
            "life_threatening_items": sum(1 for row in ordered_items if str(row.get("nspire_designation") or "").upper() == "LT"),
            "affirmative_habitability_items": sum(1 for row in ordered_items if row.get("affirmative_habitability_requirement")),
        },
    }


def build_property_inspection_packet(
    db: Session,
    *,
    org_id: int,
    prop: Property,
    property_id: int | None = None,
    inspection_id: int | None = None,
    profile_summary: dict[str, Any] | None = None,
    jurisdiction: str | None = None,
    inspector_name: str | None = None,
    inspection_date: str | None = None,
) -> dict[str, Any]:
    effective = get_effective_hqs_items(db, org_id=org_id, prop=prop, profile_summary=profile_summary)
    template_items = template_items_from_effective_rules(effective.get("items") or [])
    resolved_property_id = int(property_id or getattr(prop, "id", 0) or 0)

    checklist_rows = build_property_scoped_checklist_items(
        org_id=org_id,
        property_id=resolved_property_id,
        inspection_id=inspection_id,
        jurisdiction=jurisdiction,
        template_items=template_items,
        inspector_name=inspector_name,
        inspection_date=inspection_date,
    )

    template_versions = sorted({(item.template_key, item.template_version) for item in template_items})
    return {
        "property_id": resolved_property_id,
        "org_id": org_id,
        "jurisdiction": (jurisdiction or "").strip() or None,
        "template_catalog": template_items_as_dicts(template_items),
        "template_sources": effective.get("sources") or [],
        "template_counts": effective.get("counts") or {},
        "template_versions": [{"template_key": key, "template_version": version} for key, version in template_versions],
        "inspection_items": checklist_rows,
        "summary": {
            "total_items": len(checklist_rows),
            "common_fail_count": sum(1 for row in checklist_rows if row.get("common_fail")),
            "critical_items": sum(1 for row in checklist_rows if str(row.get("severity")).lower() == "critical"),
            "fail_items": sum(1 for row in checklist_rows if str(row.get("severity")).lower() == "fail"),
            "warn_items": sum(1 for row in checklist_rows if str(row.get("severity")).lower() == "warn"),
            "life_threatening_items": sum(1 for row in checklist_rows if str(row.get("nspire_designation") or "").upper() == "LT"),
            "affirmative_habitability_items": sum(1 for row in checklist_rows if row.get("affirmative_habitability_requirement")),
        },
    }


def hqs_items_lookup(
    db: Session,
    *,
    org_id: int,
    prop: Property,
    profile_summary: dict[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    effective = get_effective_hqs_items(db, org_id=org_id, prop=prop, profile_summary=profile_summary)
    return {
        normalize_rule_code(item.get("code") or ""): item
        for item in (effective.get("items") or [])
        if normalize_rule_code(item.get("code") or "")
    }


def explain_hqs_rule(
    db: Session,
    *,
    org_id: int,
    prop: Property,
    code: str,
    profile_summary: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    lookup = hqs_items_lookup(db, org_id=org_id, prop=prop, profile_summary=profile_summary)
    return lookup.get(normalize_rule_code(code))
