# backend/app/services/compliance_service.py
from __future__ import annotations

import json
import os
from pathlib import Path
from datetime import datetime
from typing import Any

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.domain.compliance.hqs import summarize_items, top_fix_candidates
from app.domain.compliance.hqs_library import get_effective_hqs_items
from app.models import (
    AuditEvent,
    Inspection,
    InspectionItem,
    Property,
    PropertyChecklistItem,
    RehabTask,
    WorkflowEvent,
)
from app.services.inspections.failure_task_service import (
    build_failure_next_actions,
    create_failure_tasks_from_inspection,
)
from app.services.inspections.readiness_service import (
    build_property_readiness_summary,
    compute_property_readiness_score,
)
from app.services.inspections.template_service import (
    apply_raw_inspection_payload,
    build_inspection_template,
    ensure_template_backed_checklist,
)
from app.services.jurisdiction_profile_service import resolve_operational_policy
from app.services.compliance_engine.projection_service import (
    build_property_compliance_brief,
    build_property_projection_snapshot,
    rebuild_property_projection,
)
from app.services.compliance_document_service import build_property_document_stack
from app.services.workflow_gate_service import build_property_jurisdiction_blocker


STATUS_PASS = "pass"
STATUS_FAIL = "fail"
STATUS_WARN = "warn"
STATUS_UNKNOWN = "unknown"
STATUS_NA = "not_applicable"


def _now() -> datetime:
    return datetime.utcnow()


def _j(v: Any) -> str:
    return json.dumps(v, separators=(",", ":"), ensure_ascii=False, default=str)


def _rollback_quietly(db: Session) -> None:
    try:
        db.rollback()
    except Exception:
        pass


def _safe_projection_snapshot(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    property: Property | None = None,
) -> dict[str, Any]:
    try:
        rebuild_property_projection(
            db,
            org_id=org_id,
            property_id=property_id,
            property=property,
        )
        snapshot = build_property_projection_snapshot(
            db,
            org_id=org_id,
            property_id=property_id,
        )
        return snapshot if isinstance(snapshot, dict) else {}
    except TypeError:
        try:
            rebuild_property_projection(db, org_id=org_id, property_id=property_id)
            snapshot = build_property_projection_snapshot(
                db,
                org_id=org_id,
                property_id=property_id,
            )
            return snapshot if isinstance(snapshot, dict) else {}
        except Exception:
            _rollback_quietly(db)
            return {}
    except Exception:
        _rollback_quietly(db)
        return {}


def _safe_document_stack_snapshot(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    try:
        snapshot = build_property_document_stack(db, org_id=org_id, property_id=property_id)
        return snapshot if isinstance(snapshot, dict) else {}
    except Exception:
        _rollback_quietly(db)
        return {}


def _safe_jurisdiction_blocker(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    try:
        blocker = build_property_jurisdiction_blocker(
            db,
            org_id=org_id,
            property_id=property_id,
        )
        return blocker if isinstance(blocker, dict) else {}
    except Exception:
        _rollback_quietly(db)
        return {}


def _get_property(db: Session, *, org_id: int, property_id: int) -> Property:
    prop = db.scalar(
        select(Property).where(
            Property.org_id == org_id,
            Property.id == property_id,
        )
    )
    if not prop:
        raise ValueError("property not found")
    return prop

def _safe_get_property(db: Session, *, org_id: int, property_id: int) -> Property | None:
    try:
        return _get_property(db, org_id=org_id, property_id=property_id)
    except Exception:
        _rollback_quietly(db)
        try:
            return _get_property(db, org_id=org_id, property_id=property_id)
        except Exception:
            _rollback_quietly(db)
            return None


def _safe_checklist_rows(db: Session, *, org_id: int, property_id: int) -> list[PropertyChecklistItem]:
    try:
        return _checklist_rows(db, org_id=org_id, property_id=property_id)
    except Exception:
        _rollback_quietly(db)
        return []


def _safe_latest_inspection(db: Session, *, org_id: int, property_id: int) -> Inspection | None:
    try:
        return _latest_inspection(db, org_id=org_id, property_id=property_id)
    except Exception:
        _rollback_quietly(db)
        return None


def _safe_inspection_rows(db: Session, *, inspection_id: int | None) -> list[InspectionItem]:
    if inspection_id is None:
        return []
    try:
        return _inspection_rows(db, inspection_id=int(inspection_id))
    except Exception:
        _rollback_quietly(db)
        return []


def _safe_profile_summary_for_property(
    db: Session,
    *,
    org_id: int,
    prop: Property | None,
    fallback: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if isinstance(fallback, dict) and fallback:
        return fallback
    if prop is None:
        return {}
    try:
        resolved = resolve_operational_policy(
            db,
            org_id=org_id,
            state=getattr(prop, "state", None) or "MI",
            county=getattr(prop, "county", None),
            city=getattr(prop, "city", None),
        )
        return resolved if isinstance(resolved, dict) else {}
    except Exception:
        _rollback_quietly(db)
        return {}


def _safe_effective_hqs_items(
    db: Session,
    *,
    org_id: int,
    prop: Property | None,
    profile_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    if prop is None:
        return {"items": [], "sources": [], "counts": {}}
    try:
        effective = get_effective_hqs_items(
            db,
            org_id=org_id,
            prop=prop,
            profile_summary=profile_summary or {},
        )
        return effective if isinstance(effective, dict) else {"items": [], "sources": [], "counts": {}}
    except Exception:
        _rollback_quietly(db)
        return {"items": [], "sources": [], "counts": {}}


def _safe_readiness_score(db: Session, *, org_id: int, property_id: int):
    try:
        return compute_property_readiness_score(
            db,
            org_id=org_id,
            property_id=property_id,
        )
    except Exception:
        _rollback_quietly(db)
        return compute_property_readiness_score(
            db,
            org_id=org_id,
            property_id=property_id,
        )


def _safe_readiness_summary(db: Session, *, org_id: int, property_id: int) -> dict[str, Any]:
    try:
        summary = build_property_readiness_summary(
            db,
            org_id=org_id,
            property_id=property_id,
        )
        return summary if isinstance(summary, dict) else {}
    except Exception:
        _rollback_quietly(db)
        return {}


def _safe_failure_actions(db: Session, *, org_id: int, property_id: int, inspection_id: int | None) -> dict[str, Any]:
    try:
        actions = build_failure_next_actions(
            db,
            org_id=org_id,
            property_id=property_id,
            inspection_id=inspection_id,
            limit=10,
        )
        return actions if isinstance(actions, dict) else {"recommended_actions": []}
    except Exception:
        _rollback_quietly(db)
        return {"recommended_actions": []}


def _latest_inspection(db: Session, *, org_id: int, property_id: int) -> Inspection | None:
    return db.scalar(
        select(Inspection)
        .where(Inspection.org_id == org_id, Inspection.property_id == property_id)
        .order_by(
            desc(Inspection.inspection_date),
            desc(Inspection.created_at),
            desc(Inspection.id),
        )
        .limit(1)
    )


def _inspection_rows(db: Session, *, inspection_id: int) -> list[InspectionItem]:
    return list(
        db.scalars(
            select(InspectionItem)
            .where(InspectionItem.inspection_id == inspection_id)
            .order_by(InspectionItem.id.asc())
        ).all()
    )

def _projection_jurisdiction_trust(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(snapshot, dict):
        return {}
    projection = snapshot.get("projection") or {}
    if not isinstance(projection, dict):
        return {}
    projection_reason = projection.get("projection_reason") or {}
    if not isinstance(projection_reason, dict):
        return {}
    trust = projection_reason.get("jurisdiction_trust") or {}
    return trust if isinstance(trust, dict) else {}


def _trust_blocked_for_compliance(trust: dict[str, Any]) -> bool:
    if not isinstance(trust, dict) or not trust:
        return False
    if not bool(trust.get("safe_for_projection", True)):
        return True
    manual_review_reasons = trust.get("manual_review_reasons") or []
    return bool(manual_review_reasons)


def _trust_reason_payload(trust: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(trust, dict):
        return {}
    return {
        "decision_code": trust.get("decision_code") or trust.get("decision") or trust.get("code"),
        "safe_for_projection": bool(trust.get("safe_for_projection", False)),
        "safe_for_user_reliance": bool(trust.get("safe_for_user_reliance", False)),
        "blocker_reasons": [str(x) for x in (trust.get("blocker_reasons") or []) if str(x).strip()],
        "manual_review_reasons": [str(x) for x in (trust.get("manual_review_reasons") or []) if str(x).strip()],
        "missing_critical_categories": list(trust.get("missing_critical_categories") or []),
        "stale_authoritative_categories": list(trust.get("stale_authoritative_categories") or []),
        "incomplete_required_tiers": list(trust.get("incomplete_required_tiers") or []),
    }

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


def _rehab_task_exists(db: Session, *, org_id: int, property_id: int, title: str) -> bool:
    row = db.scalar(
        select(RehabTask).where(
            RehabTask.org_id == org_id,
            RehabTask.property_id == property_id,
            RehabTask.title == title,
        )
    )
    return row is not None


def _ensure_policy_task(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    title: str,
    category: str,
    priority: str,
    notes: str,
) -> bool:
    if _rehab_task_exists(db, org_id=org_id, property_id=property_id, title=title):
        return False

    now = _now()

    row = RehabTask(
        org_id=org_id,
        property_id=property_id,
        title=title,
        category=category or "compliance",
        status="todo",
        notes=notes,
        created_at=now,
    )

    if hasattr(row, "inspection_relevant"):
        row.inspection_relevant = True
    if hasattr(row, "priority"):
        row.priority = priority
    if hasattr(row, "cost_estimate"):
        row.cost_estimate = None
    if hasattr(row, "updated_at"):
        row.updated_at = now

    db.add(row)
    return True


def _status_from_checklist(
    code: str,
    by_code: dict[str, PropertyChecklistItem],
    severity: str,
) -> tuple[str, str | None]:
    row = by_code.get(code.upper())
    if row is None:
        return STATUS_UNKNOWN, "No checklist evidence recorded yet."

    completed = getattr(row, "is_completed", None)
    status = getattr(row, "status", None)
    notes = getattr(row, "notes", None)

    status_norm = str(status or "").strip().lower()

    if completed is True or status_norm in {"done", "pass", "passed", "complete", "completed"}:
        return STATUS_PASS, notes

    if completed is False or status_norm in {"fail", "failed", "open", "blocked"}:
        return (STATUS_FAIL if severity in {"fail", "critical"} else STATUS_WARN), notes

    if status_norm in {"todo", "in_progress"}:
        return STATUS_UNKNOWN, notes

    return STATUS_UNKNOWN, notes


def _rule_result(
    *,
    key: str,
    label: str,
    source: str,
    status: str,
    severity: str,
    category: str,
    blocks_hqs: bool = False,
    blocks_local: bool = False,
    blocks_voucher: bool = False,
    blocks_lease_up: bool = False,
    suggested_fix: str | None = None,
    evidence: str | None = None,
) -> dict[str, Any]:
    return {
        "rule_key": key,
        "label": label,
        "source": source,
        "status": status,
        "severity": severity,
        "category": category,
        "blocks_hqs": bool(blocks_hqs),
        "blocks_local": bool(blocks_local),
        "blocks_voucher": bool(blocks_voucher),
        "blocks_lease_up": bool(blocks_lease_up),
        "suggested_fix": suggested_fix,
        "evidence": evidence,
    }


def _policy_item_to_rule(
    raw: dict[str, Any],
    *,
    default_source: str,
    default_category: str,
    default_severity: str,
    default_blocks_local: bool = False,
    default_blocks_voucher: bool = False,
    default_blocks_lease_up: bool = False,
) -> dict[str, Any]:
    code = str(
        raw.get("code")
        or raw.get("rule_key")
        or raw.get("title")
        or raw.get("description")
        or "POLICY_ITEM"
    ).strip().upper().replace(" ", "_")

    label = str(
        raw.get("title")
        or raw.get("description")
        or raw.get("label")
        or code.replace("_", " ").title()
    ).strip()

    severity = str(raw.get("severity") or default_severity).strip().lower()
    if severity not in {"fail", "warn", "unknown", "info", "critical"}:
        try:
            sev_num = int(raw.get("severity"))
            severity = "critical" if sev_num >= 4 else "fail" if sev_num == 3 else "warn"
        except Exception:
            severity = default_severity

    status = str(raw.get("status") or STATUS_FAIL).strip().lower()
    if status not in {STATUS_PASS, STATUS_FAIL, STATUS_WARN, STATUS_UNKNOWN, STATUS_NA}:
        status = STATUS_FAIL if severity in {"fail", "critical"} else STATUS_WARN

    return _rule_result(
        key=code,
        label=label,
        source=str(raw.get("source") or default_source),
        status=status,
        severity=severity,
        category=str(raw.get("category") or default_category),
        blocks_hqs=bool(raw.get("blocks_hqs", False)),
        blocks_local=bool(raw.get("blocks_local", default_blocks_local)),
        blocks_voucher=bool(raw.get("blocks_voucher", default_blocks_voucher)),
        blocks_lease_up=bool(raw.get("blocks_lease_up", default_blocks_lease_up)),
        suggested_fix=raw.get("suggested_fix") or raw.get("fix") or raw.get("description"),
        evidence=raw.get("evidence"),
    )




def _ensure_proof_gap_tasks(db: Session, *, org_id: int, property_id: int, proof_obligations: list[dict[str, Any]]) -> dict[str, Any]:
    created = []
    for item in proof_obligations or []:
        status = str(item.get("proof_status") or "").strip().lower()
        if status not in {"missing", "expired", "mismatched"}:
            continue
        title = f"Upload proof: {item.get('proof_label') or item.get('rule_key') or 'compliance evidence'}"
        notes = item.get("evidence_gap") or f"Resolve required property proof for rule {item.get('rule_key')}."
        if _ensure_policy_task(db, org_id=org_id, property_id=property_id, title=title, category="compliance_proof", priority="high" if item.get("blocking") else "med", notes=notes):
            created.append(title)
    return {"created_count": len(created), "created_titles": created}

def _build_warren_fallback_rules(profile_summary: dict[str, Any]) -> list[dict[str, Any]]:
    policy = profile_summary.get("policy") or {}
    if not isinstance(policy, dict):
        policy = {}

    compliance = policy.get("compliance") or {}
    state_rules = policy.get("state_rules") or {}

    def _yes(v: Any) -> bool:
        return str(v or "").strip().lower() in {"yes", "true", "required", "1"}

    def _no(v: Any) -> bool:
        return str(v or "").strip().lower() in {"no", "false", "not_allowed", "not_required", "0"}

    rental_license_required = _yes(compliance.get("rental_license_required"))
    inspection_required = _yes(compliance.get("inspection_required"))
    all_fees_paid_required = _yes(compliance.get("all_fees_must_be_paid"))
    city_debts_block_license = _yes(compliance.get("city_debts_block_license"))
    local_agent_required = _yes(compliance.get("local_agent_required"))
    local_agent_radius = compliance.get("local_agent_radius_miles")
    owner_po_box_allowed = not _no(compliance.get("owner_po_box_allowed"))
    soi_protected = _yes(state_rules.get("source_of_income_discrimination_prohibited"))

    out: list[dict[str, Any]] = []

    if rental_license_required:
        out.append(
            _rule_result(
                key="WARREN_RENTAL_LICENSE_REQUIRED",
                label="Warren rental license required",
                source="warren_profile",
                status=STATUS_FAIL,
                severity="fail",
                category="licensing",
                blocks_local=True,
                blocks_lease_up=True,
                blocks_voucher=True,
                suggested_fix="Complete Warren rental license application and obtain license approval.",
            )
        )

    if inspection_required:
        inspection_frequency = str(compliance.get("inspection_frequency") or "required").strip().lower()
        frequency_label = (
            "Warren biennial rental inspection required"
            if inspection_frequency == "biennial"
            else "Warren rental inspection required"
        )
        out.append(
            _rule_result(
                key="WARREN_BIENNIAL_INSPECTION_REQUIRED",
                label=frequency_label,
                source="warren_profile",
                status=STATUS_FAIL,
                severity="fail",
                category="inspection",
                blocks_local=True,
                blocks_lease_up=True,
                blocks_voucher=True,
                suggested_fix="Schedule and pass Warren's required rental inspection.",
            )
        )

    if all_fees_paid_required:
        out.append(
            _rule_result(
                key="WARREN_ALL_FEES_PAID_REQUIRED",
                label="Warren requires rental fees to be paid before license issuance",
                source="warren_profile",
                status=STATUS_FAIL,
                severity="fail",
                category="fees",
                blocks_local=True,
                blocks_lease_up=True,
                blocks_voucher=True,
                suggested_fix="Pay all required rental registration / licensing / inspection fees.",
            )
        )

    if city_debts_block_license:
        out.append(
            _rule_result(
                key="WARREN_CITY_DEBTS_BLOCK_LICENSE",
                label="Warren blocks license issuance when listed city debts remain unpaid",
                source="warren_profile",
                status=STATUS_FAIL,
                severity="fail",
                category="fees",
                blocks_local=True,
                blocks_lease_up=True,
                blocks_voucher=True,
                suggested_fix="Clear listed taxes, assessments, utility balances, blight-related debts, and related city obligations.",
            )
        )

    if local_agent_required:
        radius_text = f" within {local_agent_radius} miles" if local_agent_radius else ""
        out.append(
            _rule_result(
                key="WARREN_LOCAL_AGENT_REQUIRED",
                label=f"Warren local agent required{radius_text}",
                source="warren_profile",
                status=STATUS_FAIL,
                severity="fail",
                category="jurisdiction",
                blocks_local=True,
                blocks_lease_up=True,
                blocks_voucher=True,
                suggested_fix="Designate a qualified local agent that meets Warren requirements.",
            )
        )

    if local_agent_radius:
        out.append(
            _rule_result(
                key="WARREN_LOCAL_AGENT_MAX_RADIUS_MILES",
                label=f"Warren local agent must be within {local_agent_radius} miles",
                source="warren_profile",
                status=STATUS_FAIL,
                severity="fail",
                category="jurisdiction",
                blocks_local=True,
                blocks_lease_up=True,
                blocks_voucher=True,
                suggested_fix=f"Confirm your local agent is an individual located within {local_agent_radius} miles of Warren.",
            )
        )

    if owner_po_box_allowed is False:
        out.append(
            _rule_result(
                key="WARREN_OWNER_PO_BOX_ALLOWED",
                label="Warren does not allow P.O. boxes for required legal/home address fields",
                source="warren_profile",
                status=STATUS_FAIL,
                severity="fail",
                category="documents",
                blocks_local=True,
                blocks_lease_up=True,
                blocks_voucher=True,
                suggested_fix="Use a valid physical legal/home address where Warren requires one; do not use a P.O. box.",
            )
        )

    if soi_protected:
        out.append(
            _rule_result(
                key="MI_SOURCE_OF_INCOME_DISCRIMINATION_PROHIBITED",
                label="Michigan source-of-income discrimination protections apply",
                source="mi_state_rule",
                status=STATUS_WARN,
                severity="warn",
                category="fair_housing",
                blocks_local=False,
                blocks_voucher=False,
                blocks_lease_up=False,
                suggested_fix="Ensure screening, leasing, and rejection logic do not discriminate based on lawful source of income where applicable.",
            )
        )

    return out


def _dedupe_rules(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    rank = {
        "critical": 5,
        "fail": 4,
        "warn": 3,
        "unknown": 2,
        "info": 1,
        "pass": 0,
        "not_applicable": -1,
    }

    for item in rows:
        key = str(item.get("rule_key") or "").strip().upper()
        if not key:
            continue

        existing = out.get(key)
        if existing is None:
            out[key] = item
            continue

        current_rank = rank.get(str(item.get("severity") or "").lower(), 0)
        existing_rank = rank.get(str(existing.get("severity") or "").lower(), 0)
        if current_rank > existing_rank:
            out[key] = item

    return list(out.values())


def _build_local_rules_from_profile(profile_summary: dict[str, Any]) -> list[dict[str, Any]]:
    rules: list[dict[str, Any]] = []

    for raw in profile_summary.get("required_actions") or []:
        if not isinstance(raw, dict):
            continue
        rules.append(
            _policy_item_to_rule(
                raw,
                default_source="jurisdiction_policy",
                default_category="jurisdiction",
                default_severity="fail",
                default_blocks_local=True,
                default_blocks_voucher=True,
                default_blocks_lease_up=True,
            )
        )

    for raw in profile_summary.get("blocking_items") or []:
        if not isinstance(raw, dict):
            continue
        rules.append(
            _policy_item_to_rule(
                raw,
                default_source="jurisdiction_policy",
                default_category="jurisdiction_blocker",
                default_severity="fail",
                default_blocks_local=True,
                default_blocks_voucher=True,
                default_blocks_lease_up=True,
            )
        )

    for raw in profile_summary.get("rules") or []:
        if not isinstance(raw, dict):
            continue
        rules.append(
            _policy_item_to_rule(
                raw,
                default_source="jurisdiction_rule",
                default_category="jurisdiction_rule",
                default_severity="warn",
                default_blocks_local=False,
                default_blocks_voucher=False,
                default_blocks_lease_up=False,
            )
        )

    coverage = profile_summary.get("coverage") or {}
    confidence = str(coverage.get("confidence_label") or "low").lower()
    readiness = str(coverage.get("production_readiness") or "needs_review").lower()

    rules.append(
        _rule_result(
            key="POLICY_CONFIDENCE_SUFFICIENT",
            label="Jurisdiction policy confidence is sufficient for automation",
            source="policy_coverage",
            status=STATUS_PASS if confidence in {"high", "medium"} and readiness == "ready" else STATUS_UNKNOWN,
            severity="fail",
            category="governance",
            blocks_local=not (confidence in {"high", "medium"} and readiness == "ready"),
            blocks_lease_up=not (confidence in {"high", "medium"} and readiness == "ready"),
            blocks_voucher=not (confidence in {"high", "medium"} and readiness == "ready"),
            suggested_fix="Review and verify more official sources before trusting automation for this jurisdiction.",
            evidence=f"coverage_confidence={confidence}, production_readiness={readiness}",
        )
    )

    fallback_rules = _build_warren_fallback_rules(profile_summary)
    return _dedupe_rules(rules + fallback_rules)


def _safe_property_policy_brief(
    db: Session,
    *,
    org_id: int,
    prop: Property,
    profile_summary: dict[str, Any],
) -> dict[str, Any]:
    try:
        return build_property_compliance_brief(
            db,
            org_id=org_id,
            state=getattr(prop, "state", None) or "MI",
            county=getattr(prop, "county", None),
            city=getattr(prop, "city", None),
            pha_name=profile_summary.get("pha_name"),
            property_id=int(getattr(prop, "id")),
            property=prop,
        ) or {}
    except TypeError:
        try:
            return build_property_compliance_brief(
                db,
                org_id=None,
                state=getattr(prop, "state", None) or "MI",
                county=getattr(prop, "county", None),
                city=getattr(prop, "city", None),
                pha_name=profile_summary.get("pha_name"),
                property_id=int(getattr(prop, "id")),
                property=prop,
            ) or {}
        except Exception:
            _rollback_quietly(db)
            return {}
    except Exception:
        _rollback_quietly(db)
        return {}


def _sorted_actions(rows: list[dict[str, Any]], *, limit: int = 8) -> list[dict[str, Any]]:
    order = {"fail": 0, "unknown": 1, "warn": 2, "pass": 3, "not_applicable": 4}
    return sorted(
        rows,
        key=lambda x: (
            order.get(str(x.get("status") or "").lower(), 9),
            0 if x.get("blocks_lease_up") else 1,
            0 if x.get("blocks_voucher") else 1,
            0 if x.get("blocks_local") else 1,
            str(x.get("category") or ""),
            str(x.get("rule_key") or ""),
        ),
    )[: max(1, int(limit))]


def _inspection_item_to_rule(item: InspectionItem) -> dict[str, Any]:
    status = str(getattr(item, "result_status", "") or "").strip().lower()
    severity_num = int(getattr(item, "severity", 3) or 3)
    severity = "critical" if severity_num >= 4 else "fail" if severity_num == 3 else "warn" if severity_num == 2 else "info"
    code = str(getattr(item, "code", "") or "").strip().upper() or "INSPECTION_ITEM"

    is_failing = status in {"fail", "blocked", "inconclusive"}
    return _rule_result(
        key=f"INSPECTION_{code}",
        label=str(getattr(item, "standard_label", None) or getattr(item, "fail_reason", None) or code),
        source="inspection_results",
        status=STATUS_FAIL if is_failing else STATUS_PASS if status == "pass" else STATUS_UNKNOWN,
        severity=severity,
        category=str(getattr(item, "category", None) or "inspection"),
        blocks_hqs=is_failing and severity in {"fail", "critical"},
        blocks_local=is_failing and severity in {"fail", "critical"},
        blocks_voucher=is_failing and severity in {"fail", "critical"},
        blocks_lease_up=is_failing and severity in {"fail", "critical"},
        suggested_fix=str(getattr(item, "remediation_guidance", None) or getattr(item, "fail_reason", None) or "").strip() or None,
        evidence=str(getattr(item, "details", None) or getattr(item, "location", None) or "").strip() or None,
    )


def ensure_property_compliance_template(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    return ensure_template_backed_checklist(
        db,
        org_id=org_id,
        property_id=property_id,
    )


def build_property_inspection_readiness(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    prop = _safe_get_property(db, org_id=org_id, property_id=property_id)
    if prop is None:
        raise ValueError("property not found")

    try:
        checklist_sync = ensure_template_backed_checklist(
            db,
            org_id=org_id,
            property_id=property_id,
        )
    except Exception as e:
        _rollback_quietly(db)
        checklist_sync = {
            "ok": False,
            "sync_error": str(e),
            "template_key": "hud_52580a",
            "template_version": "hud_52580a_2019",
            "checklist_id": None,
            "created_checklist": False,
            "created_items": 0,
            "updated_items": 0,
            "template": {
                "ok": False,
                "template_key": "hud_52580a",
                "template_version": "hud_52580a_2019",
                "property_id": property_id,
                "profile_summary": {},
                "items": [],
                "sources": [],
                "counts": {"total": 0},
            },
        }
        prop = _safe_get_property(db, org_id=org_id, property_id=property_id) or prop

    checklist_rows = _safe_checklist_rows(db, org_id=org_id, property_id=property_id)

    by_code: dict[str, PropertyChecklistItem] = {}
    for r in checklist_rows:
        code = str(getattr(r, "item_code", None) or getattr(r, "code", None) or "").strip().upper()
        if code:
            by_code[code] = r

    profile_summary = _safe_profile_summary_for_property(
        db,
        org_id=org_id,
        prop=prop,
        fallback=(checklist_sync.get("template") or {}).get("profile_summary"),
    )

    effective_hqs = _safe_effective_hqs_items(
        db,
        org_id=org_id,
        prop=prop,
        profile_summary=profile_summary,
    )
    hqs_items = effective_hqs.get("items") or []

    hqs_results: list[dict[str, Any]] = []
    for item in hqs_items:
        code = str(item.get("code") or "").strip().upper()
        if not code:
            continue

        severity = str(item.get("severity") or "fail").lower()
        status, evidence = _status_from_checklist(code, by_code, severity)

        hqs_results.append(
            _rule_result(
                key=code,
                label=str(item.get("description") or code.replace("_", " ").title()),
                source=str((item.get("source") or {}).get("type") or "hqs_library"),
                status=status,
                severity=severity,
                category=str(item.get("category") or "other"),
                blocks_hqs=(status in {STATUS_FAIL, STATUS_UNKNOWN} and severity in {"fail", "critical"}),
                blocks_voucher=(status in {STATUS_FAIL, STATUS_UNKNOWN} and severity in {"fail", "critical"}),
                blocks_lease_up=(status == STATUS_FAIL and severity in {"fail", "critical"}),
                suggested_fix=item.get("suggested_fix"),
                evidence=evidence,
            )
        )

    local_rules = _build_local_rules_from_profile(profile_summary)

    latest_inspection = _safe_latest_inspection(db, org_id=org_id, property_id=property_id)
    inspection_result_rules: list[dict[str, Any]] = []
    latest_inspection_id = getattr(latest_inspection, "id", None) if latest_inspection else None
    if latest_inspection is not None:
        passed = bool(getattr(latest_inspection, "passed", False))
        local_rules.append(
            _rule_result(
                key="LATEST_INSPECTION_PASSED",
                label="Latest recorded inspection passed",
                source="inspection_history",
                status=STATUS_PASS if passed else STATUS_FAIL,
                severity="fail",
                category="inspection_history",
                blocks_hqs=not passed,
                blocks_voucher=not passed,
                blocks_lease_up=not passed,
                suggested_fix="Address failing items and pass reinspection.",
                evidence=f"inspection_id={latest_inspection_id}" if latest_inspection_id is not None else None,
            )
        )
        if bool(getattr(latest_inspection, "reinspect_required", False)):
            local_rules.append(
                _rule_result(
                    key="LATEST_INSPECTION_REINSPECTION_REQUIRED",
                    label="Latest inspection requires reinspection",
                    source="inspection_history",
                    status=STATUS_FAIL,
                    severity="fail",
                    category="inspection_history",
                    blocks_hqs=True,
                    blocks_local=True,
                    blocks_voucher=True,
                    blocks_lease_up=True,
                    suggested_fix="Resolve all failing or blocked items and schedule reinspection.",
                    evidence=f"inspection_id={latest_inspection_id}",
                )
            )

        for inspection_item in _safe_inspection_rows(db, inspection_id=latest_inspection_id):
            inspection_result_rules.append(_inspection_item_to_rule(inspection_item))

    all_results = _dedupe_rules(hqs_results + local_rules + inspection_result_rules)

    blockers = [
        r
        for r in all_results
        if r["status"] in {STATUS_FAIL, STATUS_UNKNOWN}
        and (r["blocks_hqs"] or r["blocks_local"] or r["blocks_voucher"] or r["blocks_lease_up"])
    ]
    warnings = [r for r in all_results if r["status"] == STATUS_WARN]
    failing = [r for r in all_results if r["status"] == STATUS_FAIL]
    unknowns = [r for r in all_results if r["status"] == STATUS_UNKNOWN]
    passed_items = [r for r in all_results if r["status"] == STATUS_PASS]

    readiness_score = _safe_readiness_score(
        db,
        org_id=org_id,
        property_id=property_id,
    )
    _safe_projection_snapshot(
        db,
        org_id=org_id,
        property_id=property_id,
        property=prop,
    )
    readiness_summary = _safe_readiness_summary(
        db,
        org_id=org_id,
        property_id=property_id,
    )

    brief = _safe_property_policy_brief(
        db,
        org_id=org_id,
        prop=prop,
        profile_summary=profile_summary,
    )

    failure_actions = _safe_failure_actions(
        db,
        org_id=org_id,
        property_id=property_id,
        inspection_id=latest_inspection_id,
    )

    recommended_actions = _sorted_actions(
        [r for r in all_results if r["status"] in {STATUS_FAIL, STATUS_UNKNOWN, STATUS_WARN}],
        limit=10,
    )

    for action in failure_actions.get("recommended_actions") or []:
        recommended_actions.append(
            {
                "rule_key": action.get("code"),
                "label": action.get("title"),
                "source": "inspection_failure",
                "status": STATUS_FAIL,
                "severity": "critical" if action.get("priority") == "high" else "fail",
                "category": action.get("rehab_category") or action.get("category") or "compliance_repair",
                "blocks_hqs": bool(action.get("requires_reinspection", True)),
                "blocks_local": False,
                "blocks_voucher": bool(action.get("requires_reinspection", True)),
                "blocks_lease_up": bool(action.get("requires_reinspection", True)),
                "suggested_fix": action.get("notes"),
                "evidence": None,
            }
        )

    recommended_actions = _sorted_actions(recommended_actions, limit=10)

    projection = _safe_projection_snapshot(
        db,
        org_id=org_id,
        property_id=property_id,
    )
    document_stack = _safe_document_stack_snapshot(
        db,
        org_id=org_id,
        property_id=property_id,
    )
    jurisdiction_blocker = _safe_jurisdiction_blocker(
        db,
        org_id=org_id,
        property_id=property_id,
    )
    jurisdiction_trust = jurisdiction_blocker.get("jurisdiction_trust") or _projection_jurisdiction_trust(projection)
    trust_reason_payload = _trust_reason_payload(jurisdiction_trust)
    trust_blocked = bool(jurisdiction_blocker.get("blocking"))

    overall_status = "ready"
    if trust_blocked:
        overall_status = "blocked"
    elif readiness_score.posture in {"critical_failures", "needs_remediation", "not_ready", "reinspection_required", "jurisdiction_blocked"}:
        overall_status = "blocked"
    elif warnings:
        overall_status = "attention"

    latest_inspection_payload = {
        "id": latest_inspection_id,
        "passed": bool(getattr(latest_inspection, "passed", False)) if latest_inspection else None,
        "template_key": getattr(latest_inspection, "template_key", None) if latest_inspection else None,
        "template_version": getattr(latest_inspection, "template_version", None) if latest_inspection else None,
        "result_status": getattr(latest_inspection, "result_status", None) if latest_inspection else None,
        "readiness_status": getattr(latest_inspection, "readiness_status", None) if latest_inspection else None,
        "readiness_score": float(getattr(latest_inspection, "readiness_score", 0.0) or 0.0) if latest_inspection else None,
        "reinspect_required": bool(getattr(latest_inspection, "reinspect_required", False)) if latest_inspection else None,
        "inspection_date": getattr(latest_inspection, "inspection_date", None) if latest_inspection else None,
        "inspector": getattr(latest_inspection, "inspector", None) if latest_inspection else None,
        "jurisdiction": getattr(latest_inspection, "jurisdiction", None) if latest_inspection else None,
    }

    return {
        "ok": True,
        "property": {
            "id": int(getattr(prop, "id")),
            "address": getattr(prop, "address", None),
            "city": getattr(prop, "city", None),
            "county": getattr(prop, "county", None),
            "state": getattr(prop, "state", None),
            "zip": getattr(prop, "zip", None),
            "pha_name": profile_summary.get("pha_name"),
        },
        "market": {
            "scope": profile_summary.get("scope"),
            "match_level": profile_summary.get("match_level"),
            "profile_id": profile_summary.get("profile_id"),
            "friction_multiplier": profile_summary.get("friction_multiplier"),
        },
        "coverage": profile_summary.get("coverage") or {},
        "jurisdiction_trust": trust_reason_payload,
        "jurisdiction_blocker": jurisdiction_blocker,
        "trust_blocked": trust_blocked,
        "trust_blocked_reason": jurisdiction_blocker.get("blocked_reason"),
        "jurisdiction_lockout_active": bool(jurisdiction_blocker.get("lockout_active")),
        "jurisdiction_validation_pending": bool(jurisdiction_blocker.get("validation_pending")),
        "overall_status": overall_status,
        "score_pct": float(readiness_score.readiness_score),
        "completion_pct": float(readiness_score.completion_pct),
        "completion_projection_pct": float(readiness_score.completion_projection_pct),
        "posture": readiness_score.posture,
        "readiness": {
            "hqs_ready": bool(readiness_score.hqs_ready),
            "local_ready": bool(readiness_score.local_ready),
            "voucher_ready": bool(readiness_score.voucher_ready),
            "lease_up_ready": bool(readiness_score.lease_up_ready),
            "status": readiness_score.readiness_status,
            "result_status": readiness_score.result_status,
            "latest_inspection_passed": bool(readiness_score.latest_inspection_passed),
            "is_compliant": bool(readiness_score.is_compliant),
            "jurisdiction_safe_for_projection": bool(trust_reason_payload.get("safe_for_projection", False)),
            "jurisdiction_safe_for_user_reliance": bool(trust_reason_payload.get("safe_for_user_reliance", False)),
            "jurisdiction_manual_review_required": bool(trust_reason_payload.get("manual_review_reasons")),
            "jurisdiction_lockout_active": bool(jurisdiction_blocker.get("lockout_active")),
            "jurisdiction_validation_pending": bool(jurisdiction_blocker.get("validation_pending")),
            "jurisdiction_blocked_reason": jurisdiction_blocker.get("blocked_reason"),
            "reinspect_required": bool(readiness_score.reinspect_required),
        },
        "counts": {
            "total_rules": len(all_results),
            "passed": len(passed_items),
            "failing": len(failing),
            "unknown": len(unknowns),
            "warnings": len(warnings),
            "blocking": len(blockers),
            "inspection_items_total": int(readiness_score.total_items),
            "inspection_failed_items": int(readiness_score.failed_items),
            "inspection_blocked_items": int(readiness_score.blocked_items),
            "inspection_unknown_items": int(readiness_score.unknown_items),
            "inspection_failed_critical_items": int(readiness_score.failed_critical_items),
            "checklist_failed_count": int(readiness_score.checklist_failed_count),
            "checklist_blocked_count": int(readiness_score.checklist_blocked_count),
            "unresolved_failure_count": int(readiness_score.unresolved_failure_count),
            "unresolved_blocked_count": int(readiness_score.unresolved_blocked_count),
            "unresolved_critical_count": int(readiness_score.unresolved_critical_count),
            "evidence_blocking_count": int(readiness_score.evidence_blocking_count),
            "evidence_unknown_count": int(readiness_score.evidence_unknown_count),
            "evidence_conflicting_count": int(readiness_score.evidence_conflicting_count),
            "evidence_stale_count": int(readiness_score.evidence_stale_count),
            "projection_items_total": int(len(projection.get("items") or [])),
        },
        "results": all_results,
        "blocking_items": blockers,
        "warning_items": warnings,
        "recommended_actions": recommended_actions,
        "inspection_failure_actions": failure_actions,
        "effective_hqs_sources": effective_hqs.get("sources") or [],
        "effective_hqs_counts": effective_hqs.get("counts") or {},
        "policy_brief": brief,
        "projection": projection,
        "projection_jurisdiction_trust": trust_reason_payload,
        "documents": document_stack,
        "jurisdiction": profile_summary,
        "jurisdiction_blockers": list(jurisdiction_blocker.get("blocking_reasons") or []),
        "jurisdiction_manual_review_reasons": list(jurisdiction_blocker.get("manual_review_reasons") or []),
        "latest_inspection": latest_inspection_payload,
        "template": {
            "template_key": checklist_sync["template_key"],
            "template_version": checklist_sync["template_version"],
            "checklist_id": checklist_sync["checklist_id"],
            "created_checklist": checklist_sync["created_checklist"],
            "created_items": checklist_sync["created_items"],
            "updated_items": checklist_sync["updated_items"],
        },
        "run_summary": {
            "passed": len(passed_items),
            "failed": len(failing),
            "blocked": len(blockers),
            "not_yet": len(unknowns),
            "score_pct": float(readiness_score.readiness_score),
            "completion_pct": float(readiness_score.completion_pct),
            "completion_projection_pct": float(readiness_score.completion_projection_pct),
            "posture": readiness_score.posture,
            "jurisdiction_trust_blocked": trust_blocked,
            "jurisdiction_trust_decision_code": trust_reason_payload.get("decision_code"),
            "reinspect_required": bool(readiness_score.reinspect_required),
        },
        "readiness_summary": readiness_summary,
    }


def apply_inspection_form_results(
    db: Session,
    *,
    org_id: int,
    actor_user_id: int,
    property_id: int,
    inspection_id: int,
    raw_payload: dict[str, Any] | list[dict[str, Any]] | None,
    sync_checklist: bool = True,
    create_failure_tasks: bool = True,
) -> dict[str, Any]:
    result = apply_raw_inspection_payload(
        db,
        org_id=org_id,
        property_id=property_id,
        inspection_id=inspection_id,
        raw_payload=raw_payload,
        sync_checklist=sync_checklist,
    )

    failure_task_result = {
        "ok": True,
        "inspection_id": inspection_id,
        "property_id": property_id,
        "created": 0,
        "skipped_existing": 0,
        "titles": [],
    }
    if create_failure_tasks:
        failure_task_result = create_failure_tasks_from_inspection(
            db,
            org_id=org_id,
            property_id=property_id,
            inspection_id=inspection_id,
        )

    readiness_summary = build_property_readiness_summary(
        db,
        org_id=org_id,
        property_id=property_id,
    )

    now = _now()
    db.add(
        WorkflowEvent(
            org_id=org_id,
            property_id=property_id,
            actor_user_id=actor_user_id,
            event_type="inspection.form.applied",
            payload_json=_j(
                {
                    "inspection_id": inspection_id,
                    "template_key": result["template_key"],
                    "template_version": result["template_version"],
                    "mapped_count": result["mapped_count"],
                    "created_items": result["created_items"],
                    "updated_items": result["updated_items"],
                    "readiness": result["readiness"],
                    "history": result.get("history"),
                    "failure_tasks": failure_task_result,
                    "readiness_summary": readiness_summary,
                    "workflow_effect": {
                        "blocks_compliance": not bool(
                            (readiness_summary.get("completion") or {}).get("is_compliant", False)
                        ),
                        "jurisdiction_trust_blocked": bool(
                            (readiness_summary.get("trust_blocked") or False)
                        ),
                        "jurisdiction_trust_decision_code": (
                            (readiness_summary.get("jurisdiction_trust") or {}).get("decision_code")
                        ),
                        "jurisdiction_blocker_reasons": (
                            (readiness_summary.get("jurisdiction_trust") or {}).get("blocker_reasons") or []
                        ),
                        "posture": ((readiness_summary.get("readiness") or {}).get("posture")),
                    },
                }
            ),
            created_at=now,
        )
    )
    db.add(
        AuditEvent(
            org_id=org_id,
            actor_user_id=actor_user_id,
            action="inspection.form.applied",
            entity_type="inspection",
            entity_id=str(inspection_id),
            before_json=None,
            after_json=_j(
                {
                    **result,
                    "failure_tasks": failure_task_result,
                    "readiness_summary": readiness_summary,
                }
            ),
            created_at=now,
        )
    )

    return {
        **result,
        "failure_tasks": failure_task_result,
        "readiness_summary": readiness_summary,
    }


def generate_policy_tasks_for_property(
    db: Session,
    *,
    org_id: int,
    actor_user_id: int,
    property_id: int,
) -> dict[str, Any]:
    readiness = build_property_inspection_readiness(
        db,
        org_id=org_id,
        property_id=property_id,
    )
    _get_property(db, org_id=org_id, property_id=property_id)

    created = 0
    created_titles: list[str] = []

    for item in readiness["blocking_items"]:
        title = f"Compliance: {item['label']}"
        notes = (
            f"Auto-generated from compliance engine.\n"
            f"Rule: {item['rule_key']}\n"
            f"Status: {item['status']}\n"
            f"Source: {item['source']}\n"
            f"Suggested fix: {item.get('suggested_fix') or 'Review official requirement and resolve blocker.'}\n"
            f"Evidence: {item.get('evidence') or ''}"
        )
        if _ensure_policy_task(
            db,
            org_id=org_id,
            property_id=property_id,
            title=title,
            category="compliance",
            priority="high",
            notes=notes,
        ):
            created += 1
            created_titles.append(title)

    for item in readiness["warning_items"]:
        title = f"Review: {item['label']}"
        notes = (
            f"Compliance warning / review item.\n"
            f"Rule: {item['rule_key']}\n"
            f"Status: {item['status']}\n"
            f"Source: {item['source']}\n"
            f"Suggested fix: {item.get('suggested_fix') or 'Review and document.'}"
        )
        if _ensure_policy_task(
            db,
            org_id=org_id,
            property_id=property_id,
            title=title,
            category="compliance_review",
            priority="med",
            notes=notes,
        ):
            created += 1
            created_titles.append(title)

    latest_inspection = _latest_inspection(db, org_id=org_id, property_id=property_id)
    failure_task_result = create_failure_tasks_from_inspection(
        db,
        org_id=org_id,
        property_id=property_id,
        inspection_id=getattr(latest_inspection, "id", None) if latest_inspection else None,
    )
    created += int(failure_task_result.get("created", 0))
    created_titles.extend(failure_task_result.get("titles", []))

    readiness_summary = build_property_readiness_summary(
        db,
        org_id=org_id,
        property_id=property_id,
    )

    now = _now()
    db.add(
        WorkflowEvent(
            org_id=org_id,
            property_id=property_id,
            event_type="compliance.tasks.generated",
            payload_json=_j(
                {
                    "created": created,
                    "titles": created_titles,
                    "overall_status": readiness["overall_status"],
                    "readiness": readiness["readiness"],
                    "score_pct": readiness["score_pct"],
                    "inspection_failure_tasks": failure_task_result,
                    "readiness_summary": readiness_summary,
                }
            ),
            created_at=now,
        )
    )
    db.add(
        AuditEvent(
            org_id=org_id,
            actor_user_id=actor_user_id,
            action="compliance.tasks.generated",
            entity_type="property",
            entity_id=str(property_id),
            before_json=None,
            after_json=_j(
                {
                    "created": created,
                    "titles": created_titles,
                    "overall_status": readiness["overall_status"],
                    "score_pct": readiness["score_pct"],
                    "inspection_failure_tasks": failure_task_result,
                    "readiness_summary": readiness_summary,
                }
            ),
            created_at=now,
        )
    )

    return {
        "ok": True,
        "property_id": property_id,
        "created": created,
        "titles": created_titles,
        "overall_status": readiness["overall_status"],
        "readiness": readiness["readiness"],
        "score_pct": readiness["score_pct"],
        "inspection_failure_tasks": failure_task_result,
        "jurisdiction_trust": readiness.get("jurisdiction_trust") or {},
        "trust_blocked": bool(readiness.get("trust_blocked")),
        "jurisdiction_blockers": readiness.get("jurisdiction_blockers") or [],
        "readiness_summary": readiness_summary,
    }


def run_hqs(
    db: Session,
    *,
    org_id: int,
    actor_user_id: int,
    property_id: int,
    actor_email: str | None = None,
    auto_create_rehab_tasks: bool | None = None,
    create_tasks: bool | None = None,
) -> dict[str, Any]:
    should_create_tasks = create_tasks
    if should_create_tasks is None:
        should_create_tasks = bool(auto_create_rehab_tasks) if auto_create_rehab_tasks is not None else True

    readiness = build_property_inspection_readiness(
        db,
        org_id=org_id,
        property_id=property_id,
    )
    checklist_rows = _checklist_rows(db, org_id=org_id, property_id=property_id)

    latest_inspection = _latest_inspection(db, org_id=org_id, property_id=property_id)
    summary = summarize_items(
        checklist_rows,
        latest_inspection_passed=bool(getattr(latest_inspection, "passed", False)) if latest_inspection else False,
    )
    fix_candidates = top_fix_candidates(checklist_rows)

    task_info = {"ok": True, "created": 0, "titles": []}
    if should_create_tasks:
        task_info = generate_policy_tasks_for_property(
            db,
            org_id=org_id,
            actor_user_id=actor_user_id,
            property_id=property_id,
        )

    readiness_summary = build_property_readiness_summary(
        db,
        org_id=org_id,
        property_id=property_id,
    )

    now = _now()
    db.add(
        WorkflowEvent(
            org_id=org_id,
            property_id=property_id,
            actor_user_id=actor_user_id,
            event_type="compliance.automation.run",
            payload_json=_j(
                {
                    "overall_status": readiness["overall_status"],
                    "score_pct": readiness["score_pct"],
                    "counts": readiness["counts"],
                    "tasks_created": task_info.get("created", 0),
                    "template": readiness.get("template"),
                    "inspection_failure_actions": readiness.get("inspection_failure_actions"),
                    "readiness_summary": readiness_summary,
                    "workflow_effect": {
                        "blocks_compliance": not bool(
                            (readiness_summary.get("completion") or {}).get("is_compliant", False)
                        ),
                        "jurisdiction_trust_blocked": bool(
                            (readiness_summary.get("trust_blocked") or False)
                        ),
                        "jurisdiction_trust_decision_code": (
                            (readiness_summary.get("jurisdiction_trust") or {}).get("decision_code")
                        ),
                        "jurisdiction_blocker_reasons": (
                            (readiness_summary.get("jurisdiction_trust") or {}).get("blocker_reasons") or []
                        ),
                        "posture": ((readiness_summary.get("readiness") or {}).get("posture")),
                    },
                }
            ),
            created_at=now,
        )
    )

    return {
        "ok": True,
        "property_id": property_id,
        "legacy_summary": summary,
        "top_fix_candidates": fix_candidates,
        "inspection_readiness": readiness,
        "jurisdiction_trust": readiness_summary.get("jurisdiction_trust") or {},
        "trust_blocked": bool(readiness_summary.get("trust_blocked")),
        "jurisdiction_blockers": readiness_summary.get("trust_blocker_reasons") or [],
        "task_generation": task_info,
        "readiness_summary": readiness_summary,
    }


def preview_property_inspection_template(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    return build_inspection_template(
        db,
        org_id=org_id,
        property_id=property_id,
    )

def build_property_schedule_snapshot(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    readiness = build_property_readiness_summary(
        db,
        org_id=org_id,
        property_id=property_id,
    )
    schedule = build_property_schedule_summary(
        db,
        org_id=org_id,
        property_id=property_id,
    )
    return {
        "ok": True,
        "property_id": int(property_id),
        "schedule": schedule,
        "readiness_summary": readiness,
    }


def build_property_compliance_timeline(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    limit: int = 100,
) -> dict[str, Any]:
    readiness = build_property_readiness_summary(
        db,
        org_id=org_id,
        property_id=property_id,
    )
    timeline = build_property_inspection_timeline(
        db,
        org_id=org_id,
        property_id=property_id,
        limit=limit,
    )
    return {
        "ok": True,
        "property_id": int(property_id),
        "timeline": timeline,
        "readiness_summary": readiness,
    }


def build_property_document_stack_snapshot(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    readiness = build_property_readiness_summary(
        db,
        org_id=org_id,
        property_id=property_id,
    )
    documents = build_property_document_stack(
        db,
        org_id=org_id,
        property_id=property_id,
    )
    return {
        "ok": True,
        "property_id": int(property_id),
        "documents": documents,
        "readiness_summary": readiness,
    }


# --- Step 6 additive property-level compliance engine ---
from datetime import timedelta as _pc_timedelta


def _step6_pdf_dataset_status() -> dict[str, Any]:
    roots: list[str] = []
    env_raw = os.getenv("POLICY_PDFS_ROOT", "") or os.getenv("POLICY_PDF_ROOTS", "") or os.getenv("POLICY_PDF_ROOT", "") or os.getenv("NSPIRE_PDF_ROOT", "")
    for piece in str(env_raw).split(os.pathsep):
        piece = str(piece).strip()
        if not piece:
            continue
        if Path(piece).exists():
            roots.append(str(Path(piece)))
    for fallback in (
        Path("backend/data/pdfs").resolve(),
        Path("/app/backend/data/pdfs"),
        Path("/mnt/data/pdfs"),
        Path("/mnt/data/PDFs"),
        Path("/mnt/data/pfs"),
        Path(r"/mnt/data/step67_pdf_zip/pdfs"),
    ):
        if fallback.exists() and str(fallback) not in roots:
            roots.append(str(fallback))
    return {"roots": roots, "available": bool(roots)}



def _safe_property_compliance_resolution(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    try:
        from app.services.property_compliance_resolution_service import resolve_property_compliance

        payload = resolve_property_compliance(
            db,
            org_id=org_id,
            property_id=property_id,
        )
        return payload if isinstance(payload, dict) else {}
    except Exception:
        _rollback_quietly(db)
        return {}


def build_property_compliance_resolution(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    payload = _safe_property_compliance_resolution(
        db,
        org_id=org_id,
        property_id=property_id,
    )
    if not payload:
        prop = _safe_get_property(db, org_id=org_id, property_id=property_id)
        if prop is None:
            raise ValueError("property not found")
        return {
            "ok": False,
            "property_id": int(property_id),
            "property": {
                "id": int(getattr(prop, "id")),
                "address": getattr(prop, "address", None),
                "city": getattr(prop, "city", None),
                "county": getattr(prop, "county", None),
                "state": getattr(prop, "state", None),
                "zip": getattr(prop, "zip", None),
            },
            "status": "unknown",
            "pass_fail_state": "unknown",
            "is_compliant": False,
            "missing_requirements": [],
            "required_actions": [],
            "deadlines": [],
            "pdf_dataset_status": _step6_pdf_dataset_status(),
        }
    if isinstance(payload, dict) and "pdf_dataset_status" not in payload:
        payload["pdf_dataset_status"] = _step6_pdf_dataset_status()
    return payload


def answer_property_compliance_question(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    resolution = build_property_compliance_resolution(
        db,
        org_id=org_id,
        property_id=property_id,
    )
    return {
        "ok": True,
        "property_id": int(property_id),
        "question": "Is this property compliant and what is missing?",
        "answer": {
            "is_compliant": bool(resolution.get("is_compliant")),
            "pass_fail_state": resolution.get("pass_fail_state"),
            "status": resolution.get("status"),
            "missing_requirements": resolution.get("missing_requirements") or [],
            "required_actions": resolution.get("required_actions") or [],
            "deadlines": resolution.get("deadlines") or [],
            "pdf_dataset_status": resolution.get("pdf_dataset_status") or _step6_pdf_dataset_status(),
        },
        "resolution": resolution,
    }
