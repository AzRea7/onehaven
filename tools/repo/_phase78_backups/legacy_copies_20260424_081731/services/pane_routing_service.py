from __future__ import annotations

from typing import Any, Optional

from app.domain.workflow.panes import (
    allowed_panes_for_principal,
    next_stage_to_pane,
    pane_catalog,
    pane_label,
    pane_meta,
    principal_roles,
    stage_to_pane,
)
from app.domain.workflow.stages import next_stage, stage_label


def _turnover_target_from_constraints(constraints: dict[str, Any]) -> str:
    completion = constraints.get("completion") or {}
    jurisdiction = constraints.get("jurisdiction") or {}
    inspection = constraints.get("inspection") or {}
    rehab = constraints.get("rehab") or {}
    decision_bucket = str(constraints.get("decision_bucket") or "REVIEW").strip().upper()

    compliance_issue = bool(
        jurisdiction.get("gate_ok") is False
        or (inspection.get("open_failed_items") or 0) > 0
        or (completion.get("failed_count") or 0) > 0
        or (completion.get("blocked_count") or 0) > 0
        or (rehab.get("blocked") or 0) > 0
    )
    if compliance_issue:
        return "compliance"
    if decision_bucket in {"REVIEW", "REJECT"}:
        return "investor"
    return "management"


def build_pane_context(*, current_stage: Optional[str], constraints: Optional[dict[str, Any]] = None, principal: Any = None, org_id: Optional[int] = None) -> dict[str, Any]:
    constraints = constraints or {}
    stage_key = str(current_stage or "").strip().lower()
    turnover_target = _turnover_target_from_constraints(constraints)
    current_pane = stage_to_pane(stage_key, turnover_target=turnover_target)
    next_stage_key = next_stage(stage_key)
    suggested_next_pane = next_stage_to_pane(next_stage_key, turnover_target=turnover_target)
    allowed_panes = allowed_panes_for_principal(principal)
    roles = principal_roles(principal)
    is_visible = current_pane in allowed_panes
    default_visible_pane = current_pane if is_visible else (allowed_panes[0] if allowed_panes else "investor")

    if stage_key in {"pursuing", "offer_prep", "offer_ready", "offer_submitted", "negotiating", "under_contract", "due_diligence", "closing", "owned"}:
        route_reason = f"{stage_label(stage_key)} belongs in Acquire because the property is in pre-offer or active purchase execution."
    elif stage_key == "turnover":
        route_reason = "Turnover is routed by the blocker profile so the next operator lands in the right pane immediately."
    else:
        route_reason = f"{stage_label(stage_key)} currently belongs to the {pane_label(current_pane)} pane."

    return {
        "org_id": org_id,
        "current_stage": stage_key,
        "current_pane": current_pane,
        "current_pane_label": pane_label(current_pane),
        "suggested_pane": current_pane,
        "suggested_pane_label": pane_label(current_pane),
        "suggested_next_pane": suggested_next_pane,
        "suggested_next_pane_label": pane_label(suggested_next_pane) if suggested_next_pane else None,
        "route_reason": route_reason,
        "turnover_target": turnover_target if stage_key == "turnover" else None,
        "allowed_panes": allowed_panes,
        "allowed_pane_labels": [pane_label(x) for x in allowed_panes],
        "visible_pane": default_visible_pane,
        "visible_pane_label": pane_label(default_visible_pane),
        "is_current_pane_visible": is_visible,
        "principal_roles": roles,
        "catalog": pane_catalog(),
        "meta": pane_meta(current_pane),
    }
