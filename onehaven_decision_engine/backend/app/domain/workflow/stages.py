# onehaven_decision_engine/backend/app/domain/workflow/stages.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Iterable

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
    # Default legacy fallbacks
    if not s:
        return "deal"
    return "deal"


def stage_rank(stage: Optional[str]) -> int:
    return _RANK.get(clamp_stage(stage), _RANK["deal"])


def stage_gte(a: Optional[str], b: str) -> bool:
    return stage_rank(a) >= stage_rank(b)


def next_stage(stage: Optional[str]) -> Optional[str]:
    s = clamp_stage(stage)
    i = stage_rank(s)
    if i >= len(STAGES) - 1:
        return None
    return STAGES[i + 1]


@dataclass(frozen=True)
class GateResult:
    ok: bool
    blocked_reason: Optional[str] = None
    allowed_next_stage: Optional[str] = None


def gate_for_next_stage(
    *,
    current_stage: str,
    has_deal: bool,
    has_underwriting: bool,
    decision_is_buy: bool,
    has_acquisition_fields: bool,
    has_rehab_plan_tasks: bool,
    rehab_blockers_open: bool,
    compliance_passed: bool,
    tenant_selected: bool,
    lease_active: bool,
    has_cash_txns: bool,
    has_valuation: bool,
) -> GateResult:
    """
    Returns whether you can advance ONE step beyond current_stage.
    This is the enforcement engine used by:
      - /workflow/advance
      - StageGuard in downstream endpoints
      - UI 'primary CTA' enable/disable
    """

    cur = clamp_stage(current_stage)
    nxt = next_stage(cur)
    if not nxt:
        return GateResult(ok=False, blocked_reason="Already at final stage.", allowed_next_stage=None)

    # Gates for each stage transition:
    if nxt == "deal":
        # leaving import -> deal: needs nothing besides existence
        return GateResult(ok=True, allowed_next_stage=nxt)

    if nxt == "decision":
        if not has_underwriting and not has_deal:
            return GateResult(ok=False, blocked_reason="Run Deal Intake / create a Deal first.", allowed_next_stage=None)
        if not has_underwriting:
            return GateResult(ok=False, blocked_reason="Run underwriting evaluation to produce a deal score.", allowed_next_stage=None)
        return GateResult(ok=True, allowed_next_stage=nxt)

    if nxt == "acquisition":
        if not has_underwriting:
            return GateResult(ok=False, blocked_reason="Decision requires underwriting score first.", allowed_next_stage=None)
        if not decision_is_buy:
            return GateResult(ok=False, blocked_reason="Decision is not BUY. Acquisition is locked.", allowed_next_stage=None)
        return GateResult(ok=True, allowed_next_stage=nxt)

    if nxt == "rehab_plan":
        if not has_acquisition_fields:
            return GateResult(ok=False, blocked_reason="Add acquisition details (purchase/close/loan).", allowed_next_stage=None)
        return GateResult(ok=True, allowed_next_stage=nxt)

    if nxt == "rehab_exec":
        if not has_rehab_plan_tasks:
            return GateResult(ok=False, blocked_reason="Create rehab plan tasks first.", allowed_next_stage=None)
        return GateResult(ok=True, allowed_next_stage=nxt)

    if nxt == "compliance":
        if rehab_blockers_open:
            return GateResult(ok=False, blocked_reason="Rehab blockers still open. Clear blocking tasks first.", allowed_next_stage=None)
        return GateResult(ok=True, allowed_next_stage=nxt)

    if nxt == "tenant":
        if not compliance_passed:
            return GateResult(ok=False, blocked_reason="Compliance not passed. Finish HQS checklist / inspection.", allowed_next_stage=None)
        return GateResult(ok=True, allowed_next_stage=nxt)

    if nxt == "lease":
        if not tenant_selected:
            return GateResult(ok=False, blocked_reason="Select/approve a tenant first.", allowed_next_stage=None)
        return GateResult(ok=True, allowed_next_stage=nxt)

    if nxt == "cash":
        if not lease_active:
            return GateResult(ok=False, blocked_reason="No active lease. Activate a lease first.", allowed_next_stage=None)
        return GateResult(ok=True, allowed_next_stage=nxt)

    if nxt == "equity":
        # equity is storytelling: valuation snapshot required
        if not has_valuation:
            return GateResult(ok=False, blocked_reason="Add a valuation snapshot to unlock equity tracking.", allowed_next_stage=None)
        return GateResult(ok=True, allowed_next_stage=nxt)

    return GateResult(ok=False, blocked_reason=f"Unknown gate transition: {cur} -> {nxt}", allowed_next_stage=None)


def distinct_stages() -> list[str]:
    return list(STAGES)