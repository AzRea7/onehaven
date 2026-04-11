from __future__ import annotations

from typing import Any

from ..domain.workflow.stages import STAGES, clamp_stage, next_stage, stage_catalog, stage_label, stage_meta, stage_rank
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

    warnings: list[str] = []
    blocked_reason = None
    warning_reason = None

    hard_block = (
        blocking_count > 0
        or conflicting_count > 0
        or readiness_score < 45.0
    )

    soft_warn = (
        unknown_count > 0
        or stale_count > 0
        or confidence_score < 0.65
        or readiness_score < 70.0
        or len(unresolved_gaps) > 0
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

    if hard_block and current_stage in PRE_CLOSE_STAGES:
        if blocking_count > 0:
            blocked_reason = "Pre-close compliance blocker(s) remain unresolved."
        elif conflicting_count > 0:
            blocked_reason = "Conflicting compliance evidence must be resolved before closing."
        else:
            blocked_reason = "Compliance readiness is too low to proceed safely."
    elif soft_warn and current_stage in PRE_CLOSE_STAGES:
        if stale_count > 0:
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

    if _safe_int(compliance_gate.get("stale_count")) > 0:
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
        rows.append({
            "key": stage,
            "rank": rank,
            "label": meta["label"],
            "description": meta["description"],
            "primary_action": meta["primary_action"],
            "status": "completed" if is_completed and rank < cur_rank else "current" if is_current else "next" if is_next else "locked",
            "is_completed": is_completed,
            "is_current": is_current,
            "is_next": is_next,
            "is_locked": rank > cur_rank and nxt != stage,
        })

    start_gate = ((constraints.get("acquisition") or {}).get("start_gate") or {}) if isinstance(constraints, dict) else {}
    if cur == "underwritten" and start_gate:
        primary_action = {
            "stage": "pursuing",
            "stage_label": stage_label("pursuing"),
            "pane": "acquisition",
            "pane_label": pane["current_pane_label" if pane["current_pane"] == "acquisition" else "suggested_pane_label"],
            "title": "Start acquisition" if start_gate.get("ok") else "Finish pre-offer criteria",
            "kind": "start_acquisition" if start_gate.get("ok") else "blocked",
        }
    elif not gate.get("ok") and compliance_gate.get("blocked_reason"):
        primary_action = {
            "stage": "compliance",
            "stage_label": stage_label("compliance"),
            "pane": "compliance",
            "pane_label": "Compliance",
            "title": compliance_gate.get("blocked_reason") or "Resolve compliance blockers",
            "kind": "blocked",
        }
    elif next_actions:
        primary_action = {
            "stage": cur,
            "stage_label": stage_label(cur),
            "pane": pane["current_pane"],
            "pane_label": pane["current_pane_label"],
            "title": next_actions[0],
            "kind": "next_action",
        }
    elif gate.get("ok") and gate.get("allowed_next_stage"):
        allowed = clamp_stage(gate.get("allowed_next_stage"))
        primary_action = {
            "stage": allowed,
            "stage_label": stage_label(allowed),
            "pane": pane["current_pane"],
            "pane_label": pane["current_pane_label"],
            "title": f"Advance to {stage_label(allowed)}",
            "kind": "advance",
        }
    else:
        primary_action = {
            "stage": cur,
            "stage_label": stage_label(cur),
            "pane": pane["current_pane"],
            "pane_label": pane["current_pane_label"],
            "title": "Workflow blocked",
            "kind": "blocked",
        }

    return {
        "property_id": property_id,
        "current_stage": cur,
        "current_stage_label": stage_label(cur),
        "current_stage_rank": cur_rank,
        "next_stage": nxt,
        "next_stage_label": stage_label(nxt) if nxt else None,
        "current_pane": pane["current_pane"],
        "current_pane_label": pane["current_pane_label"],
        "suggested_pane": pane["suggested_pane"],
        "suggested_pane_label": pane["suggested_pane_label"],
        "suggested_next_pane": pane.get("suggested_next_pane"),
        "suggested_next_pane_label": pane.get("suggested_next_pane_label"),
        "visible_pane": pane["visible_pane"],
        "visible_pane_label": pane["visible_pane_label"],
        "is_current_pane_visible": pane["is_current_pane_visible"],
        "allowed_panes": pane["allowed_panes"],
        "allowed_pane_labels": pane["allowed_pane_labels"],
        "route_reason": pane["route_reason"],
        "transition_reason": state.get("transition_reason"),
        "transition_at": state.get("transition_at") or state.get("last_transitioned_at"),
        "is_auto_routed": state.get("is_auto_routed", True),
        "normalized_decision": state.get("normalized_decision"),
        "gate": gate,
        "gate_status": state.get("gate_status"),
        "primary_action": primary_action,
        "next_actions": next_actions,
        "constraints": constraints,
        "outstanding_tasks": outstanding_tasks,
        "stage_completion_summary": stage_completion_summary,
        "stages": rows,
        "catalog": stage_catalog(),
        "pane_catalog": pane["catalog"],
        "updated_at": state.get("updated_at"),
        "compliance_projection": projection_snapshot.get("projection") if projection_snapshot else None,
        "compliance_gate": compliance_gate,
        "pre_close_risk": pre_close_risk,
        "post_close_recheck": post_close_recheck,
    }