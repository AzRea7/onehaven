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

    rows: list[dict[str, Any]] = []
    completed_lookup = {
        str(item.get("stage")): bool(item.get("is_complete"))
        for item in (stage_completion_summary.get("by_stage") or [])
        if isinstance(item, dict)
    }

    for stage in STAGES:
        rank = stage_rank(stage)
        meta = stage_meta(stage)
        rows.append(
            {
                "key": stage,
                "rank": rank,
                "label": meta["label"],
                "description": meta["description"],
                "primary_action": meta["primary_action"],
                "status": (
                    "completed"
                    if completed_lookup.get(stage, False) and rank < cur_rank
                    else "current"
                    if rank == cur_rank
                    else "next"
                    if nxt == stage
                    else "locked"
                ),
                "is_completed": bool(completed_lookup.get(stage, False)),
                "is_current": rank == cur_rank,
                "is_next": nxt == stage,
                "is_locked": rank > cur_rank and nxt != stage,
            }
        )

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
        "constraints": state.get("constraints") or {},
        "outstanding_tasks": state.get("outstanding_tasks") or {},
        "stage_completion_summary": stage_completion_summary,
        "stages": rows,
        "catalog": stage_catalog(),
        "updated_at": state.get("updated_at"),
    }
