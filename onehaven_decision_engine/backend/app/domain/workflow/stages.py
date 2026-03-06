from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

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
