# backend/app/services/compliance_service.py
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..domain.compliance.hqs import summarize_items, top_fix_candidates
from ..domain.compliance.hqs_library import get_effective_hqs_items
from ..models import AuditEvent, Inspection, Property, PropertyChecklistItem, RehabTask, WorkflowEvent
from ..services.jurisdiction_profile_service import summarize_profile
from ..services.policy_projection_service import build_property_compliance_brief


STATUS_PASS = "pass"
STATUS_FAIL = "fail"
STATUS_WARN = "warn"
STATUS_UNKNOWN = "unknown"
STATUS_NA = "not_applicable"


def _now() -> datetime:
    return datetime.utcnow()


def _j(v: Any) -> str:
    return json.dumps(v, separators=(",", ":"), ensure_ascii=False)


def _get_property(db: Session, *, org_id: int, property_id: int) -> Property:
    prop = db.scalar(select(Property).where(Property.org_id == org_id, Property.id == property_id))
    if not prop:
        raise ValueError("property not found")
    return prop


def _latest_inspection(db: Session, *, property_id: int) -> Inspection | None:
    return db.scalar(
        select(Inspection)
        .where(Inspection.property_id == property_id)
        .order_by(Inspection.id.desc())
        .limit(1)
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


def _normalize_policy_task_title(title: str) -> str:
    return " ".join((title or "").strip().lower().split())


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
    db.add(
        RehabTask(
            org_id=org_id,
            property_id=property_id,
            title=title,
            category=category or "compliance",
            status="open",
            priority=priority or "med",
            estimated_cost=None,
            actual_cost=None,
            notes=notes,
            created_at=now,
            updated_at=now,
        )
    )
    return True


def _status_from_checklist(code: str, by_code: dict[str, PropertyChecklistItem], severity: str) -> tuple[str, str | None]:
    row = by_code.get(code.upper())
    if row is None:
        return STATUS_UNKNOWN, "No checklist evidence recorded yet."

    completed = getattr(row, "is_completed", None)
    status = getattr(row, "status", None)
    notes = getattr(row, "notes", None)

    if completed is True or str(status or "").lower() in {"done", "pass", "passed", "complete", "completed"}:
        return STATUS_PASS, notes
    if completed is False or str(status or "").lower() in {"fail", "failed", "open", "blocked"}:
        return (STATUS_FAIL if severity == "fail" else STATUS_WARN), notes

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


def _yn_unknown(v: Any) -> str:
    s = str(v or "").strip().lower()
    if s in {"yes", "true", "required"}:
        return "yes"
    if s in {"no", "false", "not_required"}:
        return "no"
    return "unknown"


def _build_local_rules_from_profile(profile_summary: dict[str, Any]) -> list[dict[str, Any]]:
    coverage = profile_summary.get("coverage") or {}
    profile_json = profile_summary.get("profile_json") or {}
    compliance = (profile_json.get("compliance") or {}) if isinstance(profile_json, dict) else {}
    voucher = (profile_json.get("voucher") or {}) if isinstance(profile_json, dict) else {}
    inspections = (profile_json.get("inspections") or {}) if isinstance(profile_json, dict) else {}
    documents = (profile_json.get("documents") or {}) if isinstance(profile_json, dict) else {}
    lead = (profile_json.get("lead") or {}) if isinstance(profile_json, dict) else {}

    confidence = str(coverage.get("confidence_label") or "low").lower()
    readiness = str(coverage.get("production_readiness") or "needs_review").lower()

    rules: list[dict[str, Any]] = []

    registration_required = _yn_unknown(compliance.get("registration_required"))
    inspection_required = _yn_unknown(compliance.get("inspection_required"))
    certificate_required = _yn_unknown(compliance.get("certificate_required_before_occupancy"))
    lead_required = _yn_unknown(lead.get("lead_safe_required") or lead.get("lead_clearance_required"))
    landlord_packet_required = _yn_unknown(voucher.get("landlord_packet_required"))
    hap_required = _yn_unknown(voucher.get("hap_contract_and_tenancy_addendum_required"))
    local_reinspection = _yn_unknown(inspections.get("reinspection_required_after_fail"))
    business_license_required = _yn_unknown(compliance.get("business_license_required") or compliance.get("rental_license_required"))
    fee_schedule_known = "yes" if (profile_json.get("fees") or {}) else "unknown"
    document_stack_known = "yes" if documents else "unknown"

    def requirement_status(v: str, *, hard_block: bool, unknown_blocks: bool = True) -> str:
        if v == "yes":
            return STATUS_FAIL
        if v == "no":
            return STATUS_PASS
        return STATUS_UNKNOWN if unknown_blocks else STATUS_WARN

    rules.append(
        _rule_result(
            key="local_registration_cleared",
            label="Local rental registration / registration equivalent cleared",
            source="jurisdiction_profile",
            status=requirement_status(registration_required, hard_block=True),
            severity="fail",
            category="jurisdiction",
            blocks_local=registration_required != "no",
            blocks_lease_up=registration_required != "no",
            blocks_voucher=registration_required != "no",
            suggested_fix="Complete local rental registration / registration-equivalent workflow and record proof.",
        )
    )
    rules.append(
        _rule_result(
            key="local_inspection_cleared",
            label="Local municipal inspection workflow cleared",
            source="jurisdiction_profile",
            status=requirement_status(inspection_required, hard_block=True),
            severity="fail",
            category="jurisdiction",
            blocks_local=inspection_required != "no",
            blocks_lease_up=inspection_required != "no",
            blocks_voucher=inspection_required != "no",
            suggested_fix="Book and pass required local rental inspection and record outcome.",
        )
    )
    rules.append(
        _rule_result(
            key="certificate_before_occupancy_cleared",
            label="Certificate / compliance approval before occupancy cleared",
            source="jurisdiction_profile",
            status=requirement_status(certificate_required, hard_block=True),
            severity="fail",
            category="jurisdiction",
            blocks_local=certificate_required != "no",
            blocks_lease_up=certificate_required != "no",
            blocks_voucher=certificate_required != "no",
            suggested_fix="Obtain certificate of compliance / occupancy approval before lease-up.",
        )
    )
    rules.append(
        _rule_result(
            key="rental_license_cleared",
            label="Rental / business / landlord license requirement cleared",
            source="jurisdiction_profile",
            status=requirement_status(business_license_required, hard_block=True),
            severity="fail",
            category="licensing",
            blocks_local=business_license_required != "no",
            blocks_lease_up=business_license_required != "no",
            blocks_voucher=business_license_required != "no",
            suggested_fix="Confirm and complete any local rental or business licensing requirement.",
        )
    )
    rules.append(
        _rule_result(
            key="fee_schedule_known",
            label="Local fees / recurring charges are known",
            source="jurisdiction_profile",
            status=STATUS_PASS if fee_schedule_known == "yes" else STATUS_UNKNOWN,
            severity="warn",
            category="fees",
            blocks_local=False,
            blocks_lease_up=False,
            suggested_fix="Verify all local registration, inspection, certification, and renewal fees.",
        )
    )
    rules.append(
        _rule_result(
            key="document_stack_known",
            label="Required local documents are known",
            source="jurisdiction_profile",
            status=STATUS_PASS if document_stack_known == "yes" else STATUS_UNKNOWN,
            severity="warn",
            category="documents",
            blocks_local=False,
            blocks_lease_up=False,
            suggested_fix="Confirm all required uploads/forms/disclosures for this market.",
        )
    )
    rules.append(
        _rule_result(
            key="lead_safe_cleared",
            label="Lead-safe / deteriorated paint requirements cleared",
            source="jurisdiction_profile",
            status=requirement_status(lead_required, hard_block=False),
            severity="warn",
            category="lead",
            blocks_local=False,
            blocks_lease_up=lead_required == "yes",
            blocks_voucher=lead_required == "yes",
            suggested_fix="Complete any required lead-safe stabilization / clearance process.",
        )
    )
    rules.append(
        _rule_result(
            key="pha_landlord_packet_cleared",
            label="PHA landlord packet requirements cleared",
            source="jurisdiction_profile",
            status=requirement_status(landlord_packet_required, hard_block=True),
            severity="fail",
            category="voucher",
            blocks_voucher=landlord_packet_required != "no",
            suggested_fix="Complete any required PHA landlord packet / owner registration items.",
        )
    )
    rules.append(
        _rule_result(
            key="pha_hap_documents_cleared",
            label="HAP contract / tenancy addendum workflow cleared",
            source="jurisdiction_profile",
            status=requirement_status(hap_required, hard_block=True),
            severity="fail",
            category="voucher",
            blocks_voucher=hap_required != "no",
            suggested_fix="Prepare and complete HAP contract / tenancy addendum requirements.",
        )
    )
    rules.append(
        _rule_result(
            key="local_reinspection_logic_known",
            label="Local reinspection / fail-followup logic is known",
            source="jurisdiction_profile",
            status=STATUS_PASS if local_reinspection in {"yes", "no"} else STATUS_UNKNOWN,
            severity="warn",
            category="jurisdiction",
            blocks_local=False,
            suggested_fix="Confirm local reinspection timing and fail-remediation deadlines.",
        )
    )
    rules.append(
        _rule_result(
            key="policy_confidence_sufficient",
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

    return rules


def build_property_inspection_readiness(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    prop = _get_property(db, org_id=org_id, property_id=property_id)
    checklist_rows = _checklist_rows(db, org_id=org_id, property_id=property_id)
    by_code = {
        str(getattr(r, "code", "") or "").strip().upper(): r
        for r in checklist_rows
        if str(getattr(r, "code", "") or "").strip()
    }

    effective_hqs = get_effective_hqs_items(db, org_id=org_id, prop=prop)
    hqs_items = effective_hqs.get("items") or []

    hqs_results: list[dict[str, Any]] = []
    for item in hqs_items:
        code = str(item.get("code") or "").strip().upper()
        severity = str(item.get("severity") or "fail").lower()
        status, evidence = _status_from_checklist(code, by_code, severity)
        hqs_results.append(
            _rule_result(
                key=code,
                label=str(item.get("description") or code.replace("_", " ").title()),
                source="hqs_library",
                status=status,
                severity=severity,
                category=str(item.get("category") or "other"),
                blocks_hqs=(status == STATUS_FAIL and severity == "fail") or status == STATUS_UNKNOWN,
                blocks_voucher=(status == STATUS_FAIL and severity == "fail") or status == STATUS_UNKNOWN,
                blocks_lease_up=(status == STATUS_FAIL and severity == "fail"),
                suggested_fix=item.get("suggested_fix"),
                evidence=evidence,
            )
        )

    profile_summary = summarize_profile(
        db,
        org_id=org_id,
        state=getattr(prop, "state", None),
        county=getattr(prop, "county", None),
        city=getattr(prop, "city", None),
        pha_name=getattr(prop, "pha_name", None),
    )

    local_rules = _build_local_rules_from_profile(profile_summary)

    latest_inspection = _latest_inspection(db, property_id=property_id)
    if latest_inspection is not None:
        passed = bool(getattr(latest_inspection, "passed", False))
        local_rules.append(
            _rule_result(
                key="latest_inspection_passed",
                label="Latest recorded inspection passed",
                source="inspection_history",
                status=STATUS_PASS if passed else STATUS_FAIL,
                severity="fail",
                category="inspection_history",
                blocks_hqs=not passed,
                blocks_voucher=not passed,
                blocks_lease_up=not passed,
                suggested_fix="Address failing items and pass reinspection.",
                evidence=f"inspection_id={int(getattr(latest_inspection, 'id'))}",
            )
        )

    all_results = hqs_results + local_rules

    blockers = [r for r in all_results if r["status"] in {STATUS_FAIL, STATUS_UNKNOWN} and (r["blocks_hqs"] or r["blocks_local"] or r["blocks_voucher"] or r["blocks_lease_up"])]
    warnings = [r for r in all_results if r["status"] == STATUS_WARN]
    failing = [r for r in all_results if r["status"] == STATUS_FAIL]
    unknowns = [r for r in all_results if r["status"] == STATUS_UNKNOWN]

    hqs_ready = not any(r["blocks_hqs"] and r["status"] in {STATUS_FAIL, STATUS_UNKNOWN} for r in all_results)
    local_ready = not any(r["blocks_local"] and r["status"] in {STATUS_FAIL, STATUS_UNKNOWN} for r in all_results)
    voucher_ready = not any(r["blocks_voucher"] and r["status"] in {STATUS_FAIL, STATUS_UNKNOWN} for r in all_results)
    lease_up_ready = not any(r["blocks_lease_up"] and r["status"] in {STATUS_FAIL, STATUS_UNKNOWN} for r in all_results)

    overall_status = "ready"
    if not lease_up_ready or not voucher_ready:
        overall_status = "blocked"
    elif warnings:
        overall_status = "attention"

    coverage = profile_summary.get("coverage") or {}
    brief = {}
    try:
        brief = build_property_compliance_brief(db, org_id=org_id, property_id=property_id) or {}
    except Exception:
        brief = {}

    return {
        "ok": True,
        "property": {
            "id": int(getattr(prop, "id")),
            "address": getattr(prop, "address", None),
            "city": getattr(prop, "city", None),
            "county": getattr(prop, "county", None),
            "state": getattr(prop, "state", None),
            "zip": getattr(prop, "zip", None),
            "pha_name": getattr(prop, "pha_name", None),
        },
        "market": profile_summary.get("market") or {},
        "coverage": coverage,
        "overall_status": overall_status,
        "readiness": {
            "hqs_ready": hqs_ready,
            "local_ready": local_ready,
            "voucher_ready": voucher_ready,
            "lease_up_ready": lease_up_ready,
        },
        "counts": {
            "total_rules": len(all_results),
            "failing": len(failing),
            "unknown": len(unknowns),
            "warnings": len(warnings),
            "blocking": len(blockers),
        },
        "results": all_results,
        "blocking_items": blockers,
        "warning_items": warnings,
        "effective_hqs_sources": effective_hqs.get("sources") or [],
        "policy_brief": brief,
    }


def generate_policy_tasks_for_property(
    db: Session,
    *,
    org_id: int,
    actor_user_id: int,
    property_id: int,
) -> dict[str, Any]:
    readiness = build_property_inspection_readiness(db, org_id=org_id, property_id=property_id)
    prop = _get_property(db, org_id=org_id, property_id=property_id)

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
                }
            ),
            created_at=now,
        )
    )
    db.commit()

    return {
        "ok": True,
        "property_id": property_id,
        "created": created,
        "titles": created_titles,
        "overall_status": readiness["overall_status"],
        "readiness": readiness["readiness"],
    }


def run_hqs(
    db: Session,
    *,
    org_id: int,
    actor_user_id: int,
    property_id: int,
    create_tasks: bool = True,
) -> dict[str, Any]:
    """
    Step-11 upgraded implementation:
      - still supports legacy HQS summary expectations
      - now drives from effective HQS + jurisdiction profile + readiness blockers
    """
    readiness = build_property_inspection_readiness(db, org_id=org_id, property_id=property_id)
    checklist_rows = _checklist_rows(db, org_id=org_id, property_id=property_id)

    summary = summarize_items(checklist_rows)
    fix_candidates = top_fix_candidates(checklist_rows)

    task_info = {"ok": True, "created": 0, "titles": []}
    if create_tasks:
        task_info = generate_policy_tasks_for_property(
            db,
            org_id=org_id,
            actor_user_id=actor_user_id,
            property_id=property_id,
        )

    return {
        "ok": True,
        "property_id": property_id,
        "legacy_summary": summary,
        "top_fix_candidates": fix_candidates,
        "inspection_readiness": readiness,
        "task_generation": task_info,
    }