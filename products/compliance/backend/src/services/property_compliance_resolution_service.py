from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from products.compliance.backend.src.domain.inspection.hqs_library import get_effective_hqs_items
from onehaven_platform.backend.src.models import Inspection, InspectionItem, Property, PropertyChecklistItem
from products.compliance.backend.src.services import build_property_document_stack
from products.compliance.backend.src.services.jurisdiction_profile_service import resolve_operational_policy
from onehaven_platform.backend.src.services.compliance_projection_service import (
    build_property_compliance_brief,
    build_property_projection_snapshot,
    rebuild_property_projection,
)
from products.compliance.backend.src.services import build_property_jurisdiction_blocker

STATUS_PASS = "pass"
STATUS_FAIL = "fail"
STATUS_WARN = "warn"
STATUS_UNKNOWN = "unknown"
STATUS_NA = "not_applicable"


def _embedded_pdf_roots() -> list[Path]:
    roots: list[Path] = []
    env_raw = os.getenv("POLICY_PDFS_ROOT", "") or os.getenv("POLICY_PDF_ROOTS", "") or os.getenv("POLICY_PDF_ROOT", "") or os.getenv("NSPIRE_PDF_ROOT", "")
    for piece in str(env_raw).split(os.pathsep):
        piece = str(piece).strip()
        if not piece:
            continue
        try:
            path = Path(piece).expanduser().resolve()
        except Exception:
            continue
        if path.exists() and path.is_dir() and path not in roots:
            roots.append(path)
    for fallback in (
        Path("backend/data/pdfs").resolve(),
        Path("/app/backend/data/pdfs"),
        Path("/mnt/data/pdfs"),
        Path("/mnt/data/PDFs"),
        Path("/mnt/data/pfs"),
        Path(r"/mnt/data/step67_pdf_zip/pdfs"),
    ):
        try:
            path = Path(fallback)
        except Exception:
            continue
        if path.exists() and path.is_dir() and path not in roots:
            roots.append(path)
    return roots


SEVERITY_RANK = {
    "life_threatening": 5,
    "critical": 5,
    "severe": 4,
    "fail": 4,
    "moderate": 3,
    "warn": 3,
    "low": 2,
    "info": 1,
    "pass": 0,
    "not_applicable": -1,
}


def _utcnow() -> datetime:
    return datetime.utcnow()


def _j(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    except Exception:
        return "{}"


def _loads_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _loads_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def _norm_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _norm_lower(value: Any) -> str | None:
    text = _norm_text(value)
    return text.lower() if text else None


def _norm_upper(value: Any) -> str | None:
    text = _norm_text(value)
    return text.upper() if text else None


def _severity_rank(value: Any) -> int:
    return int(SEVERITY_RANK.get(str(value or "").strip().lower(), 0))


def _status_from_severity(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"life_threatening", "critical", "severe", "fail"}:
        return STATUS_FAIL
    if raw in {"moderate", "warn"}:
        return STATUS_WARN
    if raw in {"low", "info"}:
        return STATUS_PASS
    return STATUS_UNKNOWN


def _safe_call(default: Any, fn, *args, **kwargs) -> Any:
    try:
        out = fn(*args, **kwargs)
        return out if out is not None else default
    except Exception:
        try:
            if hasattr(args[0], "rollback"):
                args[0].rollback()
        except Exception:
            pass
        return default


def _property_or_raise(db: Session, *, org_id: int, property_id: int) -> Property:
    prop = db.scalar(
        select(Property).where(
            Property.org_id == org_id,
            Property.id == property_id,
        )
    )
    if prop is None:
        raise ValueError("property not found")
    return prop


def _latest_inspection(db: Session, *, org_id: int, property_id: int) -> Inspection | None:
    return db.scalar(
        select(Inspection)
        .where(Inspection.org_id == org_id, Inspection.property_id == property_id)
        .order_by(desc(Inspection.inspection_date), desc(Inspection.created_at), desc(Inspection.id))
        .limit(1)
    )


def _inspection_rows(db: Session, *, inspection_id: int | None) -> list[InspectionItem]:
    if inspection_id is None:
        return []
    return list(
        db.scalars(
            select(InspectionItem)
            .where(InspectionItem.inspection_id == inspection_id)
            .order_by(InspectionItem.id.asc())
        ).all()
    )


def _checklist_rows(db: Session, *, org_id: int, property_id: int) -> list[PropertyChecklistItem]:
    return list(
        db.scalars(
            select(PropertyChecklistItem)
            .where(
                PropertyChecklistItem.org_id == org_id,
                PropertyChecklistItem.property_id == property_id,
            )
            .order_by(PropertyChecklistItem.id.asc())
        ).all()
    )


def _status_from_checklist(code: str, by_code: dict[str, PropertyChecklistItem], severity: str) -> tuple[str, str | None]:
    row = by_code.get(_norm_upper(code) or "")
    if row is None:
        return STATUS_UNKNOWN, "No checklist evidence recorded yet."
    completed = getattr(row, "is_completed", None)
    status = str(getattr(row, "status", None) or "").strip().lower()
    result_status = str(getattr(row, "result_status", None) or "").strip().lower()
    notes = getattr(row, "notes", None)
    if completed is True or status in {"done", "pass", "passed", "complete", "completed"} or result_status == "pass":
        return STATUS_PASS, notes
    if completed is False or status in {"fail", "failed", "open", "blocked"} or result_status in {"fail", "blocked"}:
        return (STATUS_FAIL if _severity_rank(severity) >= _severity_rank("fail") else STATUS_WARN), notes
    if status in {"todo", "in_progress"}:
        return STATUS_UNKNOWN, notes
    return STATUS_UNKNOWN, notes


def _inspection_item_status(item: InspectionItem) -> str:
    raw = str(getattr(item, "result_status", None) or getattr(item, "status", None) or "").strip().lower()
    if raw in {"pass", "passed"}:
        return STATUS_PASS
    if raw in {"fail", "failed", "blocked"}:
        return STATUS_FAIL
    if raw in {"warn", "warning"}:
        return STATUS_WARN
    return STATUS_UNKNOWN


def _inspection_item_severity(item: InspectionItem) -> str:
    designation = _norm_lower(getattr(item, "nspire_designation", None))
    if designation in {"life_threatening", "severe", "moderate", "low"}:
        return designation
    sev = getattr(item, "severity", None)
    try:
        sev_num = int(sev) if sev is not None else 0
    except Exception:
        sev_num = 0
    if sev_num >= 4:
        return "life_threatening"
    if sev_num == 3:
        return "severe"
    if sev_num == 2:
        return "moderate"
    return "low"


def _deadline_from_correction_days(correction_days: Any) -> str | None:
    try:
        days = int(correction_days)
    except Exception:
        return None
    if days < 0:
        return None
    return (_utcnow() + timedelta(days=max(1, days))).date().isoformat()


def _default_deadline_for_item(item: dict[str, Any]) -> str | None:
    correction_days = item.get("correction_days")
    deadline = _deadline_from_correction_days(correction_days)
    if deadline:
        return deadline
    severity = _norm_lower(item.get("severity"))
    if severity == "life_threatening":
        return (_utcnow() + timedelta(days=1)).date().isoformat()
    if severity in {"severe", "moderate", "critical", "fail"}:
        return (_utcnow() + timedelta(days=30)).date().isoformat()
    return None


def _pdf_roots() -> list[Path]:
    raw = os.getenv("POLICY_PDFS_ROOT", "") or os.getenv("POLICY_PDF_ROOTS", "")
    parts = [p.strip() for p in raw.split(os.pathsep) if p.strip()]
    return [Path(p) for p in parts if Path(p).exists()]


def _find_related_pdf_evidence(prop: Property, profile_summary: dict[str, Any], effective_hqs: dict[str, Any]) -> list[dict[str, Any]]:
    roots = _pdf_roots()
    if not roots:
        return []
    city = _norm_lower(getattr(prop, "city", None)) or ""
    county = _norm_lower(getattr(prop, "county", None)) or ""
    pha = _norm_lower((profile_summary or {}).get("pha_name")) or ""
    rule_terms = []
    for item in (effective_hqs.get("items") or [])[:50]:
        text = _norm_lower(item.get("description")) or _norm_lower(item.get("code")) or ""
        if text:
            rule_terms.extend(text.replace("_", " ").split()[:3])
    tokens = {t for t in ([city, county, pha] + rule_terms) if t}
    hits: list[dict[str, Any]] = []
    for root in roots:
        for path in root.rglob("*.pdf"):
            score = 0
            lower = path.name.lower()
            for token in tokens:
                if token and token in lower:
                    score += 1
            if score > 0:
                hits.append({"path": str(path), "name": path.name, "match_score": score})
    hits.sort(key=lambda x: (-int(x["match_score"]), x["name"]))
    return hits[:20]


def _build_local_rules_from_profile(profile_summary: dict[str, Any]) -> list[dict[str, Any]]:
    policy = (profile_summary or {}).get("policy") or {}
    coverage = (profile_summary or {}).get("coverage") or {}
    out: list[dict[str, Any]] = []

    def _append(
        rule_key: str,
        label: str,
        *,
        status: str = STATUS_FAIL,
        severity: str = "fail",
        category: str = "jurisdiction",
        blocks_local: bool = True,
        blocks_voucher: bool = True,
        blocks_lease_up: bool = True,
        evidence: str | None = None,
        suggested_fix: str | None = None,
        source: str = "jurisdiction_policy",
    ) -> None:
        out.append(
            {
                "rule_key": rule_key,
                "label": label,
                "status": status,
                "severity": severity,
                "category": category,
                "blocks_local": blocks_local,
                "blocks_voucher": blocks_voucher,
                "blocks_lease_up": blocks_lease_up,
                "source": source,
                "evidence": evidence,
                "suggested_fix": suggested_fix,
            }
        )

    for raw in (profile_summary or {}).get("required_actions") or []:
        if not isinstance(raw, dict):
            continue
        _append(
            _norm_upper(raw.get("code") or raw.get("rule_key") or raw.get("title") or "REQUIRED_ACTION") or "REQUIRED_ACTION",
            _norm_text(raw.get("title") or raw.get("description") or raw.get("label")) or "Required jurisdiction action",
            severity=_norm_lower(raw.get("severity")) or "fail",
            category=_norm_lower(raw.get("category")) or "jurisdiction",
            evidence=_norm_text(raw.get("evidence")),
            suggested_fix=_norm_text(raw.get("suggested_fix") or raw.get("fix") or raw.get("description")),
            source=_norm_text(raw.get("source")) or "jurisdiction_policy",
        )

    for raw in (profile_summary or {}).get("blocking_items") or []:
        if not isinstance(raw, dict):
            continue
        _append(
            _norm_upper(raw.get("code") or raw.get("rule_key") or raw.get("title") or "BLOCKING_ITEM") or "BLOCKING_ITEM",
            _norm_text(raw.get("title") or raw.get("description") or raw.get("label")) or "Blocking jurisdiction item",
            severity=_norm_lower(raw.get("severity")) or "fail",
            category=_norm_lower(raw.get("category")) or "jurisdiction",
            evidence=_norm_text(raw.get("evidence")),
            suggested_fix=_norm_text(raw.get("suggested_fix") or raw.get("fix") or raw.get("description")),
            source=_norm_text(raw.get("source")) or "jurisdiction_policy",
        )

    confidence = _norm_lower(coverage.get("confidence_label")) or "low"
    readiness = _norm_lower(coverage.get("production_readiness")) or "needs_review"
    _append(
        "POLICY_CONFIDENCE_SUFFICIENT",
        "Jurisdiction policy confidence is sufficient for automation",
        status=STATUS_PASS if confidence in {"high", "medium"} and readiness == "ready" else STATUS_UNKNOWN,
        severity="fail",
        category="governance",
        evidence=f"coverage_confidence={confidence}, production_readiness={readiness}",
        suggested_fix="Review and verify more official sources before trusting automation for this jurisdiction.",
        source="policy_coverage",
    )
    return out


def _normalize_hqs_rule(item: dict[str, Any], *, checklist_by_code: dict[str, PropertyChecklistItem]) -> dict[str, Any]:
    code = _norm_upper(item.get("inspection_rule_code") or item.get("code")) or "HQS_ITEM"
    severity = _norm_lower(item.get("nspire_designation") or item.get("severity")) or "fail"
    status, evidence = _status_from_checklist(code, checklist_by_code, severity)
    return {
        "rule_key": code,
        "label": _norm_text(item.get("description") or item.get("label")) or code.replace("_", " ").title(),
        "status": status,
        "severity": severity,
        "category": _norm_lower(item.get("category")) or "inspection",
        "source": _norm_text((item.get("source") or {}).get("type") if isinstance(item.get("source"), dict) else item.get("source")) or "hqs_library",
        "evidence": evidence,
        "suggested_fix": _norm_text(item.get("suggested_fix")),
        "blocks_hqs": status in {STATUS_FAIL, STATUS_UNKNOWN} and _severity_rank(severity) >= _severity_rank("fail"),
        "blocks_local": status in {STATUS_FAIL, STATUS_UNKNOWN} and _severity_rank(severity) >= _severity_rank("fail"),
        "blocks_voucher": status in {STATUS_FAIL, STATUS_UNKNOWN} and _severity_rank(severity) >= _severity_rank("fail"),
        "blocks_lease_up": status == STATUS_FAIL and _severity_rank(severity) >= _severity_rank("fail"),
        "inspection_rule_code": code,
        "nspire_standard_key": item.get("nspire_standard_key"),
        "nspire_standard_code": item.get("nspire_standard_code"),
        "nspire_standard_label": item.get("nspire_standard_label"),
        "nspire_deficiency_description": item.get("nspire_deficiency_description"),
        "nspire_designation": item.get("nspire_designation"),
        "correction_days": item.get("correction_days"),
        "deadline": _default_deadline_for_item(item),
    }


def _normalize_inspection_result(item: InspectionItem) -> dict[str, Any]:
    severity = _inspection_item_severity(item)
    status = _inspection_item_status(item)
    code = _norm_upper(
        getattr(item, "inspection_rule_code", None)
        or getattr(item, "code", None)
        or getattr(item, "item_code", None)
    ) or "INSPECTION_ITEM"
    correction_days = getattr(item, "correction_days", None)
    return {
        "rule_key": f"INSPECTION_{code}",
        "label": _norm_text(getattr(item, "standard_label", None) or getattr(item, "fail_reason", None)) or code.replace("_", " ").title(),
        "status": status,
        "severity": severity,
        "category": _norm_lower(getattr(item, "category", None)) or "inspection",
        "source": "inspection_results",
        "evidence": _norm_text(getattr(item, "details", None) or getattr(item, "location", None)),
        "suggested_fix": _norm_text(getattr(item, "remediation_guidance", None) or getattr(item, "fail_reason", None)),
        "blocks_hqs": status == STATUS_FAIL and _severity_rank(severity) >= _severity_rank("moderate"),
        "blocks_local": status == STATUS_FAIL and _severity_rank(severity) >= _severity_rank("moderate"),
        "blocks_voucher": status == STATUS_FAIL and _severity_rank(severity) >= _severity_rank("moderate"),
        "blocks_lease_up": status == STATUS_FAIL and _severity_rank(severity) >= _severity_rank("moderate"),
        "inspection_rule_code": code,
        "nspire_standard_key": getattr(item, "nspire_standard_key", None),
        "nspire_standard_code": getattr(item, "nspire_standard_code", None),
        "nspire_standard_label": getattr(item, "nspire_standard_label", None),
        "nspire_deficiency_description": getattr(item, "nspire_deficiency_description", None),
        "nspire_designation": getattr(item, "nspire_designation", None),
        "correction_days": correction_days,
        "deadline": _deadline_from_correction_days(correction_days),
    }


def _proof_obligations_from_documents(documents: dict[str, Any], profile_summary: dict[str, Any]) -> list[dict[str, Any]]:
    stack = documents if isinstance(documents, dict) else {}
    proof_obligations = list(stack.get("proof_obligations") or [])
    if proof_obligations:
        return [dict(item) for item in proof_obligations if isinstance(item, dict)]
    out: list[dict[str, Any]] = []
    policy = (profile_summary or {}).get("policy") or {}
    compliance = policy.get("compliance") or {}
    if str(compliance.get("local_agent_required") or "").strip().lower() in {"yes", "true", "required", "1"}:
        out.append(
            {
                "rule_key": "LOCAL_AGENT_DOCUMENTATION",
                "proof_label": "Local agent / responsible party proof",
                "proof_status": "missing",
                "blocking": True,
                "evidence_gap": "Jurisdiction requires local agent information but proof is not yet attached.",
                "category": "documents",
            }
        )
    return out


def _property_data_requirements(prop: Property) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if getattr(prop, "year_built", None) in {None, 0, ""}:
        out.append(
            {
                "rule_key": "PROPERTY_YEAR_BUILT_PRESENT",
                "label": "Property year built should be present",
                "status": STATUS_UNKNOWN,
                "severity": "warn",
                "category": "property_data",
                "source": "property_record",
                "evidence": "year_built missing",
                "suggested_fix": "Add year built so lead-risk and age-based rules can be evaluated accurately.",
                "blocks_hqs": False,
                "blocks_local": False,
                "blocks_voucher": False,
                "blocks_lease_up": False,
            }
        )
    return out


def _dedupe_rules(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = _norm_upper(row.get("rule_key")) or ""
        if not key:
            continue
        existing = out.get(key)
        if existing is None or _severity_rank(row.get("severity")) > _severity_rank(existing.get("severity")):
            out[key] = row
    return list(out.values())


def _sorted_actions(rows: list[dict[str, Any]], *, limit: int = 12) -> list[dict[str, Any]]:
    order = {STATUS_FAIL: 0, STATUS_UNKNOWN: 1, STATUS_WARN: 2, STATUS_PASS: 3, STATUS_NA: 4}
    return sorted(
        rows,
        key=lambda x: (
            order.get(str(x.get("status") or "").lower(), 9),
            0 if x.get("blocks_lease_up") else 1,
            0 if x.get("blocks_voucher") else 1,
            0 if x.get("blocks_local") else 1,
            -_severity_rank(x.get("severity")),
            str(x.get("category") or ""),
            str(x.get("rule_key") or ""),
        ),
    )[: max(1, int(limit))]


def _compute_deadlines(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        if str(row.get("status") or "").lower() not in {STATUS_FAIL, STATUS_UNKNOWN, STATUS_WARN}:
            continue
        deadline = row.get("deadline")
        if not deadline:
            deadline = _default_deadline_for_item(row)
        if not deadline:
            continue
        out.append(
            {
                "rule_key": row.get("rule_key"),
                "label": row.get("label"),
                "deadline": deadline,
                "severity": row.get("severity"),
                "category": row.get("category"),
                "source": row.get("source"),
                "reason": row.get("suggested_fix") or row.get("evidence"),
            }
        )
    out.sort(key=lambda x: (str(x.get("deadline") or ""), -_severity_rank(x.get("severity"))))
    return out


def _required_actions(rows: list[dict[str, Any]], *, limit: int = 15) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    for row in _sorted_actions(rows, limit=limit):
        if str(row.get("status") or "").lower() == STATUS_PASS:
            continue
        actions.append(
            {
                "rule_key": row.get("rule_key"),
                "title": row.get("label"),
                "category": row.get("category"),
                "priority": "high" if _severity_rank(row.get("severity")) >= _severity_rank("severe") else "medium" if _severity_rank(row.get("severity")) >= _severity_rank("moderate") else "low",
                "source": row.get("source"),
                "suggested_fix": row.get("suggested_fix"),
                "evidence": row.get("evidence"),
                "blocks_hqs": bool(row.get("blocks_hqs")),
                "blocks_local": bool(row.get("blocks_local")),
                "blocks_voucher": bool(row.get("blocks_voucher")),
                "blocks_lease_up": bool(row.get("blocks_lease_up")),
            }
        )
    return actions


def resolve_property_compliance(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    prop = _property_or_raise(db, org_id=org_id, property_id=property_id)
    profile_summary = _safe_call(
        {},
        resolve_operational_policy,
        db,
        org_id=org_id,
        state=getattr(prop, "state", None) or "MI",
        county=getattr(prop, "county", None),
        city=getattr(prop, "city", None),
    )
    effective_hqs = _safe_call(
        {"items": [], "sources": [], "counts": {}},
        get_effective_hqs_items,
        db,
        org_id=org_id,
        prop=prop,
        profile_summary=profile_summary or {},
    )
    checklist_rows = _safe_call([], _checklist_rows, db, org_id=org_id, property_id=property_id)
    checklist_by_code = {}
    for row in checklist_rows:
        code = _norm_upper(getattr(row, "inspection_rule_code", None) or getattr(row, "item_code", None) or getattr(row, "code", None))
        if code:
            checklist_by_code[code] = row

    latest_inspection = _safe_call(None, _latest_inspection, db, org_id=org_id, property_id=property_id)
    latest_inspection_id = int(getattr(latest_inspection, "id", 0) or 0) or None
    inspection_rows = _safe_call([], _inspection_rows, db, inspection_id=latest_inspection_id)
    document_stack = _safe_call({}, build_property_document_stack, db, org_id=org_id, property_id=property_id)
    jurisdiction_blocker = _safe_call({}, build_property_jurisdiction_blocker, db, org_id=org_id, property_id=property_id)

    projection = _safe_call({}, rebuild_property_projection, db, org_id=org_id, property_id=property_id, property=prop)
    projection_snapshot = _safe_call({}, build_property_projection_snapshot, db, org_id=org_id, property_id=property_id)
    policy_brief = _safe_call(
        {},
        build_property_compliance_brief,
        db,
        org_id=org_id,
        state=getattr(prop, "state", None) or "MI",
        county=getattr(prop, "county", None),
        city=getattr(prop, "city", None),
        pha_name=(profile_summary or {}).get("pha_name"),
        property_id=int(getattr(prop, "id")),
        property=prop,
    )

    hqs_rules = [_normalize_hqs_rule(item, checklist_by_code=checklist_by_code) for item in (effective_hqs.get("items") or [])]
    local_rules = _build_local_rules_from_profile(profile_summary or {})
    inspection_rules = [_normalize_inspection_result(item) for item in inspection_rows]
    proof_obligations = _proof_obligations_from_documents(document_stack, profile_summary or {})
    proof_rules = []
    for item in proof_obligations:
        proof_status = _norm_lower(item.get("proof_status")) or "missing"
        proof_rules.append(
            {
                "rule_key": _norm_upper(item.get("rule_key") or item.get("proof_label") or "PROOF_REQUIRED") or "PROOF_REQUIRED",
                "label": _norm_text(item.get("proof_label") or item.get("rule_key")) or "Required proof missing",
                "status": STATUS_FAIL if proof_status in {"missing", "expired", "mismatched"} else STATUS_PASS,
                "severity": "fail" if bool(item.get("blocking")) else "warn",
                "category": _norm_lower(item.get("category")) or "documents",
                "source": "document_stack",
                "evidence": _norm_text(item.get("evidence_gap")),
                "suggested_fix": f"Upload or correct proof for {item.get('proof_label') or item.get('rule_key')}.",
                "blocks_hqs": False,
                "blocks_local": bool(item.get("blocking")),
                "blocks_voucher": bool(item.get("blocking")),
                "blocks_lease_up": bool(item.get("blocking")),
            }
        )
    property_data_rules = _property_data_requirements(prop)

    all_rules = _dedupe_rules(hqs_rules + local_rules + inspection_rules + proof_rules + property_data_rules)
    blocking_items = [
        row
        for row in all_rules
        if str(row.get("status") or "").lower() in {STATUS_FAIL, STATUS_UNKNOWN}
        and any(bool(row.get(flag)) for flag in ("blocks_hqs", "blocks_local", "blocks_voucher", "blocks_lease_up"))
    ]
    missing_requirements = [
        {
            "rule_key": row.get("rule_key"),
            "label": row.get("label"),
            "category": row.get("category"),
            "source": row.get("source"),
            "severity": row.get("severity"),
            "status": row.get("status"),
            "reason": row.get("evidence") or row.get("suggested_fix"),
        }
        for row in blocking_items
    ]
    required_actions = _required_actions(all_rules)
    deadlines = _compute_deadlines(all_rules)
    pdf_context = _find_related_pdf_evidence(prop, profile_summary or {}, effective_hqs)

    blocker_reasons = list(jurisdiction_blocker.get("blocking_reasons") or [])
    trust_blocked = bool(jurisdiction_blocker.get("blocking")) or bool(jurisdiction_blocker.get("lockout_active"))
    inspection_passed = bool(getattr(latest_inspection, "passed", False)) if latest_inspection is not None else False
    is_compliant = (not blocking_items) and (not trust_blocked) and inspection_passed

    pass_fail_state = STATUS_PASS if is_compliant else STATUS_FAIL if blocking_items or trust_blocked else STATUS_WARN
    status = "compliant" if is_compliant else "blocked" if blocking_items or trust_blocked else "attention"

    severity_counts = {
        "life_threatening": sum(1 for row in all_rules if _norm_lower(row.get("severity")) == "life_threatening"),
        "severe": sum(1 for row in all_rules if _norm_lower(row.get("severity")) in {"severe", "critical", "fail"}),
        "moderate": sum(1 for row in all_rules if _norm_lower(row.get("severity")) in {"moderate", "warn"}),
        "low": sum(1 for row in all_rules if _norm_lower(row.get("severity")) in {"low", "info"}),
    }

    return {
        "ok": True,
        "property_id": int(property_id),
        "property": {
            "id": int(getattr(prop, "id")),
            "address": getattr(prop, "address", None),
            "city": getattr(prop, "city", None),
            "county": getattr(prop, "county", None),
            "state": getattr(prop, "state", None),
            "zip": getattr(prop, "zip", None),
            "year_built": getattr(prop, "year_built", None),
            "property_type": getattr(prop, "property_type", None),
        },
        "status": status,
        "pass_fail_state": pass_fail_state,
        "is_compliant": bool(is_compliant),
        "missing_requirements": missing_requirements,
        "required_actions": required_actions,
        "deadlines": deadlines,
        "blocking_items": blocking_items,
        "counts": {
            "total_rules": len(all_rules),
            "blocking_items": len(blocking_items),
            "required_actions": len(required_actions),
            "deadlines": len(deadlines),
            "inspection_items": len(inspection_rules),
            "hqs_items": len(hqs_rules),
            "jurisdiction_rules": len(local_rules),
            "proof_rules": len(proof_rules),
            "pdf_context_files": len(pdf_context),
            **severity_counts,
        },
        "severity_counts": severity_counts,
        "latest_inspection": {
            "id": latest_inspection_id,
            "passed": inspection_passed if latest_inspection is not None else None,
            "inspection_date": getattr(latest_inspection, "inspection_date", None) if latest_inspection is not None else None,
            "reinspect_required": bool(getattr(latest_inspection, "reinspect_required", False)) if latest_inspection is not None else None,
        },
        "jurisdiction": profile_summary or {},
        "jurisdiction_blocker": jurisdiction_blocker or {},
        "trust_blocked": trust_blocked,
        "trust_blocker_reasons": blocker_reasons,
        "policy_brief": policy_brief or {},
        "projection": projection_snapshot or {},
        "document_stack": document_stack or {},
        "proof_obligations": proof_obligations,
        "effective_hqs": {
            "sources": effective_hqs.get("sources") or [],
            "counts": effective_hqs.get("counts") or {},
        },
        "results": all_rules,
        "pdf_context": {
            "roots": [str(p) for p in _pdf_roots()],
            "matched_files": pdf_context,
            "note": "PDF context is used from configured roots, backend/data/pdfs, and the uploaded NSPIRE ZIP extraction when available.",
        },
        "resolved_at": _utcnow().isoformat(),
    }
