# backend/app/services/workflow_gate_service.py
from __future__ import annotations

from typing import Any

from ..domain.workflow.stages import (
    STAGES,
    clamp_stage,
    next_stage,
    stage_catalog,
    stage_label,
    stage_meta,
    stage_rank,
)
from .pane_routing_service import build_pane_context
from .policy_projection_service import build_property_projection_snapshot
from .property_state_machine import get_state_payload, get_transition_payload


PRE_CLOSE_STAGES = {
    "deal",
    "underwritten",
    "pursuing",
    "offer_prep",
    "offer_ready",
    "offer_submitted",
    "negotiating",
    "under_contract",
    "due_diligence",
    "closing",
    "compliance",
}

POST_CLOSE_STAGES = {
    "owned",
    "tenant",
    "lease_up",
    "stabilized",
    "operations",
    "management",
}


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _projection_payload(db, *, org_id: int, property_id: int) -> dict[str, Any] | None:
    if db is None:
        return None
    try:
        snapshot = build_property_projection_snapshot(
            db,
            org_id=int(org_id),
            property_id=int(property_id),
        )
        if not snapshot or not snapshot.get("projection"):
            return None
        return snapshot
    except Exception:
        return None


def _jurisdiction_trust_payload(projection_snapshot: dict[str, Any] | None) -> dict[str, Any]:
    if not projection_snapshot:
        return {}

    projection = projection_snapshot.get("projection") or {}
    projection_reason = projection.get("projection_reason") or {}
    trust = projection_reason.get("jurisdiction_trust") or {}
    if isinstance(trust, dict):
        return trust
    return {}


def _trust_blocker_reasons(trust: dict[str, Any]) -> list[str]:
    return [str(x) for x in (trust.get("blocker_reasons") or []) if str(x).strip()]


def _trust_manual_review_reasons(trust: dict[str, Any]) -> list[str]:
    return [str(x) for x in (trust.get("manual_review_reasons") or []) if str(x).strip()]


def _build_compliance_gate(
    projection_snapshot: dict[str, Any] | None,
    *,
    current_stage: str,
) -> dict[str, Any]:
    if not projection_snapshot or not projection_snapshot.get("projection"):
        return {
            "ok": True,
            "severity": "info",
            "status": "not_available",
            "blocked_reason": None,
            "warning_reason": None,
            "warning_count": 0,
            "blocking_count": 0,
            "unknown_count": 0,
            "stale_count": 0,
            "conflicting_count": 0,
            "readiness_score": None,
            "confidence_score": None,
            "projected_compliance_cost": None,
            "projected_days_to_rent": None,
            "blockers": [],
            "warnings": [],
            "impacted_rules": [],
            "unresolved_evidence_gaps": [],
            "post_close_recheck_needed": False,
            "jurisdiction_trust": {},
            "jurisdiction_blocking": False,
            "critical_missing_categories": [],
            "critical_stale_categories": [],
            "critical_inferred_categories": [],
            "critical_conflicting_categories": [],
            "trust_decision_code": None,
            "trust_blocker_reasons": [],
            "manual_review_reasons": [],
            "safe_for_projection": None,
            "safe_for_user_reliance": None,
        }

    projection = projection_snapshot.get("projection") or {}
    blockers = list(projection_snapshot.get("blockers") or [])
    blocking_count = _safe_int(projection.get("blocking_count"))
    unknown_count = _safe_int(projection.get("unknown_count"))
    stale_count = _safe_int(projection.get("stale_count"))
    conflicting_count = _safe_int(projection.get("conflicting_count"))
    readiness_score = _safe_float(projection.get("readiness_score"), 0.0)
    confidence_score = _safe_float(projection.get("confidence_score"), 0.0)
    projected_cost = projection.get("projected_compliance_cost")
    projected_days = projection.get("projected_days_to_rent")
    impacted_rules = list(projection.get("impacted_rules") or [])
    unresolved_gaps = list(projection.get("unresolved_evidence_gaps") or [])

    jurisdiction_trust = _jurisdiction_trust_payload(projection_snapshot)
    completeness_status = str(jurisdiction_trust.get("completeness_status") or "").strip().lower()
    coverage_confidence = str(jurisdiction_trust.get("coverage_confidence") or "").strip().lower()
    production_readiness = str(jurisdiction_trust.get("production_readiness") or "").strip().lower()
    completeness_score = _safe_float(jurisdiction_trust.get("completeness_score"), 0.0)

    critical_missing_categories = list(jurisdiction_trust.get("critical_missing_categories") or [])
    critical_stale_categories = list(jurisdiction_trust.get("critical_stale_categories") or [])
    critical_inferred_categories = list(jurisdiction_trust.get("critical_inferred_categories") or [])
    critical_conflicting_categories = list(jurisdiction_trust.get("critical_conflicting_categories") or [])

    safe_for_projection = bool(jurisdiction_trust.get("safe_for_projection", False))
    safe_for_user_reliance = bool(jurisdiction_trust.get("safe_for_user_reliance", False))
    trust_decision_code = str(jurisdiction_trust.get("decision_code") or "").strip() or None
    trust_blocker_reasons = _trust_blocker_reasons(jurisdiction_trust)
    manual_review_reasons = _trust_manual_review_reasons(jurisdiction_trust)

    warnings: list[str] = []
    blocked_reason = None
    warning_reason = None

    jurisdiction_blocking = bool(
        critical_missing_categories
        or critical_conflicting_categories
        or not safe_for_projection
    )

    hard_block = (
        blocking_count > 0
        or conflicting_count > 0
        or readiness_score < 45.0
        or jurisdiction_blocking
        or production_readiness in {"blocked", "needs_review"}
    )

    soft_warn = (
        unknown_count > 0
        or stale_count > 0
        or confidence_score < 0.65
        or readiness_score < 70.0
        or len(unresolved_gaps) > 0
        or bool(critical_stale_categories)
        or bool(critical_inferred_categories)
        or coverage_confidence == "low"
        or completeness_status in {"missing", "partial", "stale", "conflicting"}
        or completeness_score < 0.80
        or bool(manual_review_reasons)
        or not safe_for_user_reliance
    )

    if blocking_count > 0:
        warnings.append(f"{blocking_count} blocking compliance requirement(s) remain unresolved.")
    if conflicting_count > 0:
        warnings.append(f"{conflicting_count} conflicting compliance rule state(s) require review.")
    if stale_count > 0:
        warnings.append(f"{stale_count} stale compliance proof item(s) need refresh.")
    if unknown_count > 0:
        warnings.append(f"{unknown_count} unknown compliance requirement(s) still need proof.")
    if confidence_score < 0.65:
        warnings.append(f"Compliance confidence is low ({confidence_score:.2f}).")
    if readiness_score < 70.0:
        warnings.append(f"Compliance readiness is below target ({readiness_score:.1f}).")
    if projected_cost is not None:
        warnings.append(f"Projected compliance cost is ${_safe_float(projected_cost):,.0f}.")
    if projected_days is not None:
        warnings.append(f"Projected days to rent impact is {_safe_int(projected_days)} day(s).")

    if critical_missing_categories:
        warnings.append(
            "Critical jurisdiction coverage is missing for: "
            + ", ".join(critical_missing_categories)
            + "."
        )
    if critical_stale_categories:
        warnings.append(
            "Critical jurisdiction coverage is stale for: "
            + ", ".join(critical_stale_categories)
            + "."
        )
    if critical_inferred_categories:
        warnings.append(
            "Critical jurisdiction coverage is inferred-only for: "
            + ", ".join(critical_inferred_categories)
            + "."
        )
    if critical_conflicting_categories:
        warnings.append(
            "Critical jurisdiction coverage is conflicting for: "
            + ", ".join(critical_conflicting_categories)
            + "."
        )
    if coverage_confidence == "low":
        warnings.append("Jurisdiction coverage confidence is low.")
    if completeness_status in {"missing", "partial", "stale", "conflicting"}:
        warnings.append(
            f"Jurisdiction completeness is {completeness_status} "
            f"({completeness_score:.2f})."
        )
    for reason in trust_blocker_reasons:
        warnings.append(f"Jurisdiction trust blocker: {reason}.")
    for reason in manual_review_reasons:
        warnings.append(f"Jurisdiction manual review required: {reason}.")

    if hard_block and current_stage in PRE_CLOSE_STAGES:
        if critical_missing_categories:
            blocked_reason = "Critical local jurisdiction coverage is missing before close."
        elif critical_conflicting_categories:
            blocked_reason = "Critical jurisdiction conflicts must be resolved before close."
        elif trust_decision_code == "blocked_due_to_stale_authoritative_sources":
            blocked_reason = "Authoritative jurisdiction sources are stale and cannot be trusted."
        elif trust_decision_code == "blocked_due_to_incomplete_required_tiers":
            blocked_reason = "Required jurisdiction tiers are incomplete for safe compliance use."
        elif trust_decision_code == "manual_review_required":
            blocked_reason = "Critical jurisdiction rules still require manual review before close."
        elif blocking_count > 0:
            blocked_reason = "Pre-close compliance blocker(s) remain unresolved."
        elif conflicting_count > 0:
            blocked_reason = "Conflicting compliance evidence must be resolved before closing."
        elif not safe_for_projection or production_readiness in {"blocked", "needs_review"}:
            blocked_reason = "Jurisdiction trust is too weak for a safe pre-close decision."
        else:
            blocked_reason = "Compliance readiness is too low to proceed safely."
    elif soft_warn and current_stage in PRE_CLOSE_STAGES:
        if critical_stale_categories:
            warning_reason = "Critical jurisdiction proof is stale and should be refreshed before close."
        elif critical_inferred_categories:
            warning_reason = "Critical jurisdiction coverage is inferred-only and needs stronger verification."
        elif manual_review_reasons:
            warning_reason = "Jurisdiction trust requires manual review before relying on compliance automation."
        elif coverage_confidence == "low":
            warning_reason = "Jurisdiction trust is weak and should be reviewed before close."
        elif stale_count > 0:
            warning_reason = "Compliance proof is stale and should be refreshed before close."
        elif unknown_count > 0:
            warning_reason = "Compliance proof is incomplete before close."
        elif confidence_score < 0.65:
            warning_reason = "Compliance confidence is too low for a clean pre-close decision."
        else:
            warning_reason = "Compliance risk needs review before close."

    post_close_recheck_needed = current_stage in POST_CLOSE_STAGES and (
        stale_count > 0
        or unknown_count > 0
        or conflicting_count > 0
        or blocking_count > 0
        or bool(critical_stale_categories)
        or bool(critical_inferred_categories)
        or bool(critical_conflicting_categories)
        or bool(manual_review_reasons)
        or not safe_for_user_reliance
    )

    ok = not (current_stage in PRE_CLOSE_STAGES and hard_block)
    severity = "high" if not ok else ("warning" if soft_warn else "info")
    status = "blocked" if not ok else ("warning" if soft_warn else "ok")

    return {
        "ok": ok,
        "severity": severity,
        "status": status,
        "blocked_reason": blocked_reason,
        "warning_reason": warning_reason,
        "warning_count": len(warnings),
        "blocking_count": blocking_count,
        "unknown_count": unknown_count,
        "stale_count": stale_count,
        "conflicting_count": conflicting_count,
        "readiness_score": readiness_score,
        "confidence_score": confidence_score,
        "projected_compliance_cost": projected_cost,
        "projected_days_to_rent": projected_days,
        "blockers": blockers,
        "warnings": warnings,
        "impacted_rules": impacted_rules,
        "unresolved_evidence_gaps": unresolved_gaps,
        "post_close_recheck_needed": post_close_recheck_needed,
        "jurisdiction_trust": jurisdiction_trust,
        "jurisdiction_blocking": jurisdiction_blocking,
        "critical_missing_categories": critical_missing_categories,
        "critical_stale_categories": critical_stale_categories,
        "critical_inferred_categories": critical_inferred_categories,
        "critical_conflicting_categories": critical_conflicting_categories,
        "trust_decision_code": trust_decision_code,
        "trust_blocker_reasons": trust_blocker_reasons,
        "manual_review_reasons": manual_review_reasons,
        "safe_for_projection": safe_for_projection,
        "safe_for_user_reliance": safe_for_user_reliance,
    }


def _build_pre_close_risk_summary(compliance_gate: dict[str, Any], *, current_stage: str) -> dict[str, Any]:
    if current_stage not in PRE_CLOSE_STAGES:
        return {
            "active": False,
            "status": "not_applicable",
            "severity": "info",
            "blocking": False,
            "warnings": [],
            "summary": None,
        }

    blocking = not bool(compliance_gate.get("ok", True))
    warnings = list(compliance_gate.get("warnings") or [])
    if blocking:
        summary = compliance_gate.get("blocked_reason") or "Compliance risk blocks pre-close progression."
        status = "blocked"
        severity = "high"
    elif warnings:
        summary = compliance_gate.get("warning_reason") or "Compliance risk should be reviewed before close."
        status = "warning"
        severity = "warning"
    else:
        summary = "No material pre-close compliance gating issues detected."
        status = "ok"
        severity = "info"

    return {
        "active": True,
        "status": status,
        "severity": severity,
        "blocking": blocking,
        "warnings": warnings,
        "summary": summary,
        "projected_compliance_cost": compliance_gate.get("projected_compliance_cost"),
        "projected_days_to_rent": compliance_gate.get("projected_days_to_rent"),
        "jurisdiction_trust": compliance_gate.get("jurisdiction_trust") or {},
        "trust_decision_code": compliance_gate.get("trust_decision_code"),
        "trust_blocker_reasons": compliance_gate.get("trust_blocker_reasons") or [],
        "manual_review_reasons": compliance_gate.get("manual_review_reasons") or [],
    }


def _build_post_close_recheck_summary(compliance_gate: dict[str, Any], *, current_stage: str) -> dict[str, Any]:
    if current_stage not in POST_CLOSE_STAGES:
        return {
            "active": False,
            "status": "not_applicable",
            "needed": False,
            "reason": None,
        }

    needed = bool(compliance_gate.get("post_close_recheck_needed"))
    if not needed:
        return {
            "active": True,
            "status": "ok",
            "needed": False,
            "reason": None,
        }

    if compliance_gate.get("critical_conflicting_categories"):
        reason = "Post-close jurisdiction trust contains critical conflicting coverage."
    elif compliance_gate.get("critical_stale_categories"):
        reason = "Post-close critical jurisdiction proof has gone stale and should be re-evaluated."
    elif compliance_gate.get("critical_inferred_categories"):
        reason = "Post-close critical jurisdiction coverage is still inferred-only."
    elif compliance_gate.get("manual_review_reasons"):
        reason = "Post-close jurisdiction trust still requires manual review."
    elif _safe_int(compliance_gate.get("stale_count")) > 0:
        reason = "Post-close compliance proof has gone stale and should be re-evaluated."
    elif _safe_int(compliance_gate.get("unknown_count")) > 0:
        reason = "Post-close compliance still includes unknown requirements."
    elif _safe_int(compliance_gate.get("conflicting_count")) > 0:
        reason = "Post-close compliance contains conflicting evidence."
    else:
        reason = "Post-close compliance blockers still exist."

    return {
        "active": True,
        "status": "recheck_required",
        "needed": True,
        "reason": reason,
        "warnings": list(compliance_gate.get("warnings") or []),
        "jurisdiction_trust": compliance_gate.get("jurisdiction_trust") or {},
    }


def _effective_gate(
    state_gate: dict[str, Any],
    compliance_gate: dict[str, Any],
    *,
    current_stage: str,
) -> dict[str, Any]:
    base = dict(state_gate or {})
    if current_stage in PRE_CLOSE_STAGES and not compliance_gate.get("ok", True):
        base["ok"] = False
        base["blocked_reason"] = compliance_gate.get("blocked_reason") or base.get("blocked_reason")
        base["compliance_gate"] = compliance_gate
        if not base.get("allowed_next_stage"):
            base["allowed_next_stage"] = "compliance"
        base["code"] = "compliance_projection_blocked"
    elif compliance_gate.get("warnings"):
        base["compliance_gate"] = compliance_gate
        base["warning_reason"] = compliance_gate.get("warning_reason")
    return base


def build_workflow_summary(db, *, org_id: int, property_id: int, principal: Any = None, recompute: bool = True) -> dict[str, Any]:
    state = get_state_payload(db, org_id=org_id, property_id=property_id, recompute=recompute)
    tx = get_transition_payload(db, org_id=org_id, property_id=property_id)
    cur = clamp_stage(state.get("current_stage"))
    cur_rank = stage_rank(cur)
    nxt = next_stage(cur)
    state_gate = tx.get("gate") or {}
    next_actions = state.get("next_actions") or []
    stage_completion_summary = state.get("stage_completion_summary") or {}
    constraints = state.get("constraints") or {}
    outstanding_tasks = state.get("outstanding_tasks") or {}
    pane = build_pane_context(current_stage=cur, constraints=constraints, principal=principal, org_id=org_id)

    projection_snapshot = _projection_payload(db, org_id=org_id, property_id=property_id)
    compliance_gate = _build_compliance_gate(projection_snapshot, current_stage=cur)
    gate = _effective_gate(state_gate, compliance_gate, current_stage=cur)
    pre_close_risk = _build_pre_close_risk_summary(compliance_gate, current_stage=cur)
    post_close_recheck = _build_post_close_recheck_summary(compliance_gate, current_stage=cur)

    completed_lookup = {
        str(item.get("stage")): bool(item.get("is_complete"))
        for item in (stage_completion_summary.get("by_stage") or [])
        if isinstance(item, dict)
    }
    rows = []
    for stage in STAGES:
        rank = stage_rank(stage)
        meta = stage_meta(stage)
        is_completed = bool(completed_lookup.get(stage, False))
        is_current = rank == cur_rank
        is_next = nxt == stage
        rows.append(
            {
                "key": stage,
                "rank": rank,
                "label": stage_label(stage),
                "group": meta.get("group"),
                "is_completed": is_completed,
                "is_current": is_current,
                "is_next": is_next,
            }
        )

    return {
        "ok": True,
        "current_stage": cur,
        "next_stage": nxt,
        "current_stage_label": stage_label(cur),
        "current_stage_meta": stage_meta(cur),
        "pane": pane,
        "stage_rows": rows,
        "gate": gate,
        "pre_close_risk": pre_close_risk,
        "post_close_recheck": post_close_recheck,
        "next_actions": next_actions,
        "constraints": constraints,
        "outstanding_tasks": outstanding_tasks,
        "transition": tx,
        "compliance_gate": compliance_gate,
        "jurisdiction_trust": compliance_gate.get("jurisdiction_trust") or {},
    }