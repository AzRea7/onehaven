from __future__ import annotations

from typing import Any, Optional

from ..domain.workflow.panes import (
    allowed_panes_for_principal,
    pane_catalog,
    pane_label,
    pane_meta,
    principal_roles,
    stage_to_pane,
)
from ..domain.workflow.stages import stage_label


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


def build_pane_context(
    *,
    current_stage: Optional[str],
    constraints: Optional[dict[str, Any]] = None,
    principal: Any = None,
    org_id: Optional[int] = None,
) -> dict[str, Any]:
    constraints = constraints or {}
    stage_key = str(current_stage or "").strip().lower()

    turnover_target = _turnover_target_from_constraints(constraints)
    current_pane = stage_to_pane(stage_key, turnover_target=turnover_target)
    allowed_panes = allowed_panes_for_principal(principal)
    roles = principal_roles(principal)

    is_visible = current_pane in allowed_panes
    default_visible_pane = current_pane if is_visible else (allowed_panes[0] if allowed_panes else "investor")

    if stage_key == "turnover":
        route_reason = (
            "Turnover routed to Compliance because inspection, rehab, or jurisdiction blockers are still open."
            if turnover_target == "compliance"
            else "Turnover routed to Investor because the unit needs re-evaluation before the next placement cycle."
            if turnover_target == "investor"
            else "Turnover routed to Management because the unit is operationally ready for standard turnover handling."
        )
    elif stage_key == "acquired":
        route_reason = "Property has been acquired and is now routed into post-close compliance workflow."
    elif stage_key == "inspection_pending":
        route_reason = "Property is waiting on inspection completion before tenant placement can continue."
    elif stage_key == "leased":
        route_reason = "Lease is active and the property is transitioning into managed occupancy."
    else:
        route_reason = f"{stage_label(stage_key)} currently belongs to the {pane_label(current_pane)} pane."

    return {
        "org_id": org_id,
        "current_stage": stage_key,
        "current_pane": current_pane,
        "current_pane_label": pane_label(current_pane),
        "suggested_pane": current_pane,
        "suggested_pane_label": pane_label(current_pane),
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