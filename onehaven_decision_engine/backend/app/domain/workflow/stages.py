from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

# Canonical investor-facing workflow used everywhere in Step 2.
# We keep only the 6 business stages the user actually cares about.
STAGES: list[str] = [
    "deal",
    "rehab",
    "compliance",
    "tenant",
    "cash",
    "equity",
]

_RANK: dict[str, int] = {stage: idx for idx, stage in enumerate(STAGES)}

# Backward-compatible aliases so older DB rows / route inputs still normalize
# into the new 6-stage workflow instead of breaking.
_ALIASES: dict[str, str] = {
    "import": "deal",
    "intake": "deal",
    "deal": "deal",
    "decision": "deal",
    "acquisition": "deal",
    "procurement": "deal",
    "sourcing": "deal",
    "rehab": "rehab",
    "rehab_plan": "rehab",
    "rehab_exec": "rehab",
    "renovation": "rehab",
    "construction": "rehab",
    "compliance": "compliance",
    "inspection": "compliance",
    "licensing": "compliance",
    "tenant": "tenant",
    "lease": "tenant",
    "leasing": "tenant",
    "cash": "cash",
    "cashflow": "cash",
    "management": "cash",
    "operations": "cash",
    "equity": "equity",
    "portfolio": "equity",
}

_STAGE_META: dict[str, dict[str, str]] = {
    "deal": {
        "label": "Deal",
        "description": "Underwriting, rent logic, and the normalized GOOD / REVIEW / REJECT deal decision.",
        "primary_action": "Run underwriting",
    },
    "rehab": {
        "label": "Rehab",
        "description": "Build the rehab scope, complete the rehab work, and clear all rehab blockers.",
        "primary_action": "Complete rehab tasks",
    },
    "compliance": {
        "label": "Compliance",
        "description": "Complete inspection and compliance readiness before tenant placement.",
        "primary_action": "Pass inspection",
    },
    "tenant": {
        "label": "Tenant",
        "description": "Place the tenant and make the lease active.",
        "primary_action": "Create tenant + lease",
    },
    "cash": {
        "label": "Cash",
        "description": "Track actual income and expenses for the occupied asset.",
        "primary_action": "Record transactions",
    },
    "equity": {
        "label": "Equity",
        "description": "Track valuation and monitor the property as an occupied cashflow asset.",
        "primary_action": "Add valuation",
    },
}


def clamp_stage(stage: Optional[str]) -> str:
    raw = (stage or "").strip().lower()
    if raw in _RANK:
        return raw
    if raw in _ALIASES:
        return _ALIASES[raw]
    return "deal"


def distinct_stages(values: list[str] | tuple[str, ...]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        stage = clamp_stage(value)
        if stage in seen:
            continue
        seen.add(stage)
        out.append(stage)
    return out


def stage_rank(stage: Optional[str]) -> int:
    return _RANK[clamp_stage(stage)]


def stage_gte(a: Optional[str], b: str) -> bool:
    return stage_rank(a) >= stage_rank(b)


def stage_lte(a: Optional[str], b: str) -> bool:
    return stage_rank(a) <= stage_rank(b)


def next_stage(stage: Optional[str]) -> Optional[str]:
    idx = stage_rank(stage)
    if idx >= len(STAGES) - 1:
        return None
    return STAGES[idx + 1]


def prev_stage(stage: Optional[str]) -> Optional[str]:
    idx = stage_rank(stage)
    if idx <= 0:
        return None
    return STAGES[idx - 1]


def stage_label(stage: Optional[str]) -> str:
    key = clamp_stage(stage)
    return _STAGE_META.get(key, {}).get("label", key.title())


def stage_meta(stage: Optional[str]) -> dict[str, str]:
    key = clamp_stage(stage)
    meta = _STAGE_META.get(key, {})
    return {
        "key": key,
        "label": meta.get("label", key.title()),
        "description": meta.get("description", ""),
        "primary_action": meta.get("primary_action", ""),
    }


def stage_catalog() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for idx, stage in enumerate(STAGES):
        meta = stage_meta(stage)
        out.append(
            {
                "key": stage,
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
    blockers: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "blocked_reason": self.blocked_reason,
            "allowed_next_stage": self.allowed_next_stage,
            "blockers": list(self.blockers),
        }


def gate_for_next_stage(
    *,
    current_stage: str,
    decision_bucket: str,
    deal_complete: bool,
    rehab_complete: bool,
    compliance_complete: bool,
    tenant_complete: bool,
    cash_complete: bool,
    equity_complete: bool,
) -> GateResult:
    cur = clamp_stage(current_stage)
    decision = (decision_bucket or "REVIEW").strip().upper()

    if cur == "equity":
        return GateResult(ok=False, blocked_reason="Already at final stage.", blockers=[])

    if cur == "deal":
        if decision == "REJECT":
            return GateResult(
                ok=False,
                blocked_reason="Rejected deals cannot advance until assumptions change and the deal is re-underwritten.",
                allowed_next_stage=None,
                blockers=["decision_reject"],
            )
        if not deal_complete:
            return GateResult(
                ok=False,
                blocked_reason="Complete underwriting and reach a GOOD decision first.",
                allowed_next_stage="rehab",
                blockers=["deal_incomplete"],
            )
        return GateResult(ok=True, allowed_next_stage="rehab")

    if cur == "rehab":
        if not rehab_complete:
            return GateResult(
                ok=False,
                blocked_reason="Complete all rehab tasks and clear rehab blockers first.",
                allowed_next_stage="compliance",
                blockers=["rehab_incomplete"],
            )
        return GateResult(ok=True, allowed_next_stage="compliance")

    if cur == "compliance":
        if not compliance_complete:
            return GateResult(
                ok=False,
                blocked_reason="Pass inspection and clear compliance blockers first.",
                allowed_next_stage="tenant",
                blockers=["compliance_incomplete"],
            )
        return GateResult(ok=True, allowed_next_stage="tenant")

    if cur == "tenant":
        if not tenant_complete:
            return GateResult(
                ok=False,
                blocked_reason="An active lease is required before moving into cashflow.",
                allowed_next_stage="cash",
                blockers=["tenant_incomplete"],
            )
        return GateResult(ok=True, allowed_next_stage="cash")

    if cur == "cash":
        if not cash_complete:
            return GateResult(
                ok=False,
                blocked_reason="Record actual transactions before moving into equity tracking.",
                allowed_next_stage="equity",
                blockers=["cash_incomplete"],
            )
        return GateResult(ok=True, allowed_next_stage="equity")

    return GateResult(ok=False, blocked_reason="Unknown workflow state.", blockers=["unknown_state"])