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

    rows: list[dict[str, Any]] = []
    for s in STAGES:
        rank = stage_rank(s)
        rows.append(
            {
                "key": s,
                "rank": rank,
                "label": stage_label(s),
                "description": stage_meta(s).get("description", ""),
                "primary_action": stage_meta(s).get("primary_action", ""),
                "status": (
                    "completed"
                    if rank < cur_rank
                    else "current"
                    if rank == cur_rank
                    else "next"
                    if nxt == s
                    else "locked"
                ),
                "is_completed": rank < cur_rank,
                "is_current": rank == cur_rank,
                "is_next": nxt == s,
                "is_locked": rank > cur_rank and nxt != s,
            }
        )

    completed_count = cur_rank
    total_count = len(STAGES)
    pct_complete = round(completed_count / total_count, 4) if total_count else 0.0

    primary_action = None
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
            "title": "Workflow complete",
            "kind": "complete",
        }

    return {
        "property_id": property_id,
        "current_stage": cur,
        "current_stage_label": stage_label(cur),
        "current_stage_rank": cur_rank,
        "next_stage": nxt,
        "next_stage_label": stage_label(nxt) if nxt else None,
        "progress": {
            "completed_count": completed_count,
            "total_count": total_count,
            "pct_complete": pct_complete,
        },
        "gate": gate,
        "primary_action": primary_action,
        "next_actions": next_actions,
        "constraints": state.get("constraints") or {},
        "outstanding_tasks": state.get("outstanding_tasks") or {},
        "stages": rows,
        "catalog": stage_catalog(),
        "updated_at": state.get("updated_at"),
    }