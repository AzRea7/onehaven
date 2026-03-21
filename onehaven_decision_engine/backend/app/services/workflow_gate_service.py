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
from ..services.property_state_machine import get_state_payload, get_transition_payload


def build_workflow_summary(
    db,
    *,
    org_id: int,
    property_id: int,
    recompute: bool = True,
) -> dict[str, Any]:
    state = get_state_payload(
        db,
        org_id=org_id,
        property_id=property_id,
        recompute=recompute,
    )
    tx = get_transition_payload(
        db,
        org_id=org_id,
        property_id=property_id,
    )

    cur = clamp_stage(state.get("current_stage"))
    cur_rank = stage_rank(cur)
    nxt = next_stage(cur)
    gate = tx.get("gate") or {}
    next_actions = state.get("next_actions") or []
    stage_completion_summary = state.get("stage_completion_summary") or {}
    constraints = state.get("constraints") or {}
    outstanding_tasks = state.get("outstanding_tasks") or {}

    jurisdiction = constraints.get("jurisdiction") or {}
    jurisdiction_tasks = outstanding_tasks.get("jurisdiction_tasks") or []
    readiness = constraints.get("readiness") or {}
    completion = constraints.get("completion") or {}
    inspection = constraints.get("inspection") or {}
    inspection_failure_actions = outstanding_tasks.get("inspection_failure_actions") or []

    rows: list[dict[str, Any]] = []
    completed_lookup = {
        str(item.get("stage")): bool(item.get("is_complete"))
        for item in (stage_completion_summary.get("by_stage") or [])
        if isinstance(item, dict)
    }

    for stage in STAGES:
        rank = stage_rank(stage)
        meta = stage_meta(stage)
        is_completed = bool(completed_lookup.get(stage, False))
        is_current = rank == cur_rank
        is_next = nxt == stage
        is_locked = rank > cur_rank and nxt != stage

        row = {
            "key": stage,
            "rank": rank,
            "label": meta["label"],
            "description": meta["description"],
            "primary_action": meta["primary_action"],
            "status": (
                "completed"
                if is_completed and rank < cur_rank
                else "current"
                if is_current
                else "next"
                if is_next
                else "locked"
            ),
            "is_completed": is_completed,
            "is_current": is_current,
            "is_next": is_next,
            "is_locked": is_locked,
        }

        if stage == "compliance":
            row["jurisdiction_gate_ok"] = bool(jurisdiction.get("gate_ok", True))
            row["jurisdiction_completeness_status"] = jurisdiction.get("completeness_status")
            row["jurisdiction_is_stale"] = jurisdiction.get("is_stale")
            row["jurisdiction_missing_categories"] = jurisdiction.get("missing_categories") or []
            row["inspection_readiness_status"] = readiness.get("status")
            row["inspection_result_status"] = readiness.get("result_status")
            row["inspection_posture"] = completion.get("posture")
            row["latest_inspection_passed"] = completion.get("latest_inspection_passed")
            row["compliance_failed_count"] = completion.get("failed_count")
            row["compliance_blocked_count"] = completion.get("blocked_count")
            row["open_failed_items"] = inspection.get("open_failed_items")

        rows.append(row)

    primary_action: dict[str, Any]
    if next_actions:
        primary_action = {
            "stage": cur,
            "stage_label": stage_label(cur),
            "title": next_actions[0],
            "kind": "next_action",
        }
    elif gate.get("ok") and gate.get("allowed_next_stage"):
        allowed = clamp_stage(gate.get("allowed_next_stage"))
        primary_action = {
            "stage": allowed,
            "stage_label": stage_label(allowed),
            "title": f"Advance to {stage_label(allowed)}",
            "kind": "advance",
        }
    else:
        primary_action = {
            "stage": cur,
            "stage_label": stage_label(cur),
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
        "normalized_decision": state.get("normalized_decision"),
        "gate": gate,
        "gate_status": state.get("gate_status"),
        "primary_action": primary_action,
        "next_actions": next_actions,
        "constraints": constraints,
        "outstanding_tasks": outstanding_tasks,
        "jurisdiction": {
            "exists": bool(jurisdiction.get("exists")),
            "profile_id": jurisdiction.get("profile_id"),
            "gate_ok": bool(jurisdiction.get("gate_ok", True)),
            "gate_reason": jurisdiction.get("gate_reason"),
            "completeness_status": jurisdiction.get("completeness_status"),
            "completeness_score": jurisdiction.get("completeness_score"),
            "missing_categories": jurisdiction.get("missing_categories") or [],
            "is_stale": jurisdiction.get("is_stale"),
            "stale_reason": jurisdiction.get("stale_reason"),
            "tasks": jurisdiction_tasks,
        },
        "compliance": {
            "inspection_readiness_status": readiness.get("status"),
            "inspection_result_status": readiness.get("result_status"),
            "hqs_ready": readiness.get("hqs_ready"),
            "local_ready": readiness.get("local_ready"),
            "voucher_ready": readiness.get("voucher_ready"),
            "lease_up_ready": readiness.get("lease_up_ready"),
            "is_compliant": completion.get("is_compliant"),
            "completion_pct": completion.get("completion_pct"),
            "completion_projection_pct": completion.get("completion_projection_pct"),
            "posture": completion.get("posture"),
            "latest_inspection_passed": completion.get("latest_inspection_passed"),
            "failed_count": completion.get("failed_count"),
            "blocked_count": completion.get("blocked_count"),
            "latest_readiness_score": completion.get("latest_readiness_score"),
            "open_failed_items": inspection.get("open_failed_items"),
            "failure_actions": inspection_failure_actions,
        },
        "stage_completion_summary": stage_completion_summary,
        "stages": rows,
        "catalog": stage_catalog(),
        "updated_at": state.get("updated_at"),
    }