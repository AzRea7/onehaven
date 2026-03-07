from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

# Canonical ordered stages (single source of truth)
STAGES: list[str] = [
    "import",
    "deal",
    "decision",
    "acquisition",
    "rehab_plan",
    "rehab_exec",
    "compliance",
    "tenant",
    "lease",
    "cash",
    "equity",
]

_RANK: dict[str, int] = {s: i for i, s in enumerate(STAGES)}

_STAGE_META: dict[str, dict[str, str]] = {
    "import": {
        "label": "Import",
        "description": "Property exists in the system and is ready for deal setup.",
        "primary_action": "Create deal",
    },
    "deal": {
        "label": "Deal Analysis",
        "description": "Create and analyze the deal with underwriting inputs.",
        "primary_action": "Run underwriting",
    },
    "decision": {
        "label": "Decision",
        "description": "Make a buy / watch / pass decision from underwriting results.",
        "primary_action": "Finalize decision",
    },
    "acquisition": {
        "label": "Acquisition",
        "description": "Record acquisition facts such as purchase price and closing date.",
        "primary_action": "Add acquisition details",
    },
    "rehab_plan": {
        "label": "Rehab Planning",
        "description": "Define the rehab scope, tasks, and expected work plan.",
        "primary_action": "Create rehab plan",
    },
    "rehab_exec": {
        "label": "Rehab Execution",
        "description": "Execute rehab work and clear all open or blocked tasks.",
        "primary_action": "Complete rehab tasks",
    },
    "compliance": {
        "label": "Compliance / Inspection",
        "description": "Generate HQS/compliance checklist, pass inspection, and clear failures.",
        "primary_action": "Pass compliance",
    },
    "tenant": {
        "label": "Tenant Placement",
        "description": "Prepare the property for occupancy by selecting or creating a tenant.",
        "primary_action": "Create tenant and lease",
    },
    "lease": {
        "label": "Lease Active",
        "description": "Make sure an active lease exists for the property.",
        "primary_action": "Activate lease",
    },
    "cash": {
        "label": "Cashflow Tracking",
        "description": "Track transactions and reconcile rent and expenses.",
        "primary_action": "Add transactions",
    },
    "equity": {
        "label": "Equity Monitoring",
        "description": "Track valuation snapshots and portfolio equity over time.",
        "primary_action": "Add valuation",
    },
}


def clamp_stage(stage: Optional[str]) -> str:
    s = (stage or "").strip().lower()
    if s in _RANK:
        return s
    return "import"


def stage_rank(stage: Optional[str]) -> int:
    return _RANK.get(clamp_stage(stage), _RANK["import"])


def stage_gte(a: Optional[str], b: str) -> bool:
    return stage_rank(a) >= stage_rank(b)


def stage_lte(a: Optional[str], b: str) -> bool:
    return stage_rank(a) <= stage_rank(b)


def next_stage(stage: Optional[str]) -> Optional[str]:
    s = clamp_stage(stage)
    i = stage_rank(s)
    if i >= len(STAGES) - 1:
        return None
    return STAGES[i + 1]


def prev_stage(stage: Optional[str]) -> Optional[str]:
    s = clamp_stage(stage)
    i = stage_rank(s)
    if i <= 0:
        return None
    return STAGES[i - 1]


def stage_label(stage: Optional[str]) -> str:
    s = clamp_stage(stage)
    return _STAGE_META.get(s, {}).get("label", s.replace("_", " ").title())


def stage_meta(stage: Optional[str]) -> dict[str, str]:
    s = clamp_stage(stage)
    meta = _STAGE_META.get(s, {})
    return {
        "key": s,
        "label": meta.get("label", s.replace("_", " ").title()),
        "description": meta.get("description", ""),
        "primary_action": meta.get("primary_action", ""),
    }


def stage_catalog() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for idx, s in enumerate(STAGES):
        meta = stage_meta(s)
        out.append(
            {
                "key": s,
                "rank": idx,
                "label": meta["label"],
                "description": meta["description"],
                "primary_action": meta["primary_action"],
            }
        )
    return out


@dataclass(frozen=True)
class GateResult:
    ok: bool
    blocked_reason: Optional[str] = None
    allowed_next_stage: Optional[str] = None


def gate_for_next_stage(
    *,
    current_stage: str,
    has_property: bool,
    has_deal: bool,
    has_underwriting: bool,
    decision_is_buy: bool,
    has_acquisition_fields: bool,
    has_rehab_plan_tasks: bool,
    rehab_blockers_open: bool,
    rehab_open_tasks: bool,
    compliance_passed: bool,
    tenant_selected: bool,
    lease_active: bool,
    has_cash_txns: bool,
    has_valuation: bool,
) -> GateResult:
    """
    Canonical one-step transition gate.
    Used by /workflow/advance and stage guard helpers.
    """

    cur = clamp_stage(current_stage)
    nxt = next_stage(cur)

    if not nxt:
        return GateResult(ok=False, blocked_reason="Already at final stage.", allowed_next_stage=None)

    if nxt == "deal":
        if not has_property:
            return GateResult(ok=False, blocked_reason="Property must exist first.", allowed_next_stage=None)
        return GateResult(ok=True, allowed_next_stage=nxt)

    if nxt == "decision":
        if not has_deal:
            return GateResult(ok=False, blocked_reason="Create a deal first.", allowed_next_stage=None)
        if not has_underwriting:
            return GateResult(ok=False, blocked_reason="Run underwriting evaluation first.", allowed_next_stage=None)
        return GateResult(ok=True, allowed_next_stage=nxt)

    if nxt == "acquisition":
        if not has_underwriting:
            return GateResult(ok=False, blocked_reason="Underwriting result is required first.", allowed_next_stage=None)
        if not decision_is_buy:
            return GateResult(
                ok=False,
                blocked_reason="Only BUY-approved deals can move to acquisition.",
                allowed_next_stage=None,
            )
        return GateResult(ok=True, allowed_next_stage=nxt)

    if nxt == "rehab_plan":
        if not has_acquisition_fields:
            return GateResult(
                ok=False,
                blocked_reason="Add acquisition fields (purchase price / closing date / loan info).",
                allowed_next_stage=None,
            )
        return GateResult(ok=True, allowed_next_stage=nxt)

    if nxt == "rehab_exec":
        if not has_rehab_plan_tasks:
            return GateResult(ok=False, blocked_reason="Create rehab plan tasks first.", allowed_next_stage=None)
        return GateResult(ok=True, allowed_next_stage=nxt)

    if nxt == "compliance":
        if rehab_blockers_open:
            return GateResult(
                ok=False,
                blocked_reason="Rehab blockers are still open.",
                allowed_next_stage=None,
            )
        if rehab_open_tasks:
            return GateResult(
                ok=False,
                blocked_reason="Complete rehab execution tasks first.",
                allowed_next_stage=None,
            )
        return GateResult(ok=True, allowed_next_stage=nxt)

    if nxt == "tenant":
        if not compliance_passed:
            return GateResult(
                ok=False,
                blocked_reason="Compliance is not passed yet.",
                allowed_next_stage=None,
            )
        return GateResult(ok=True, allowed_next_stage=nxt)

    if nxt == "lease":
        if not tenant_selected:
            return GateResult(
                ok=False,
                blocked_reason="Create/select a tenant first.",
                allowed_next_stage=None,
            )
        return GateResult(ok=True, allowed_next_stage=nxt)

    if nxt == "cash":
        if not lease_active:
            return GateResult(
                ok=False,
                blocked_reason="Activate a lease first.",
                allowed_next_stage=None,
            )
        return GateResult(ok=True, allowed_next_stage=nxt)

    if nxt == "equity":
        if not has_cash_txns:
            return GateResult(
                ok=False,
                blocked_reason="Add cash transactions first.",
                allowed_next_stage=None,
            )
        if not has_valuation:
            return GateResult(
                ok=False,
                blocked_reason="Add a valuation snapshot first.",
                allowed_next_stage=None,
            )
        return GateResult(ok=True, allowed_next_stage=nxt)

    return GateResult(
        ok=False,
        blocked_reason=f"Unknown gate transition: {cur} -> {nxt}",
        allowed_next_stage=None,
    )


def distinct_stages() -> list[str]:
    return list(STAGES)