from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

STAGES: list[str] = [
    "discovered",
    "shortlisted",
    "underwritten",
    "offer",
    "acquired",
    "rehab",
    "compliance_readying",
    "inspection_pending",
    "tenant_marketing",
    "tenant_screening",
    "leased",
    "occupied",
    "turnover",
    "maintenance",
]

_RANK: dict[str, int] = {stage: idx for idx, stage in enumerate(STAGES)}

_ALIASES: dict[str, str] = {
    "import": "discovered",
    "intake": "discovered",
    "deal": "shortlisted",
    "decision": "underwritten",
    "acquisition": "offer",
    "procurement": "offer",
    "sourcing": "discovered",
    "rehab": "rehab",
    "rehab_plan": "rehab",
    "rehab_exec": "rehab",
    "renovation": "rehab",
    "construction": "rehab",
    "compliance": "compliance_readying",
    "inspection": "inspection_pending",
    "licensing": "compliance_readying",
    "tenant": "tenant_screening",
    "lease": "leased",
    "leasing": "tenant_screening",
    "cash": "occupied",
    "cashflow": "occupied",
    "management": "maintenance",
    "operations": "maintenance",
    "equity": "occupied",
    "portfolio": "occupied",
    "discovered": "discovered",
    "shortlisted": "shortlisted",
    "underwritten": "underwritten",
    "offer": "offer",
    "acquired": "acquired",
    "compliance_readying": "compliance_readying",
    "inspection_pending": "inspection_pending",
    "tenant_marketing": "tenant_marketing",
    "tenant_screening": "tenant_screening",
    "leased": "leased",
    "occupied": "occupied",
    "turnover": "turnover",
    "maintenance": "maintenance",
}

_STAGE_META: dict[str, dict[str, str]] = {
    "discovered": {
        "label": "Discovered",
        "description": "Property exists in the system but has not been shortlisted into an active acquisition review.",
        "primary_action": "Save or shortlist property",
    },
    "shortlisted": {
        "label": "Shortlisted",
        "description": "Property is in the investor review queue and should be underwritten next.",
        "primary_action": "Run underwriting",
    },
    "underwritten": {
        "label": "Underwritten",
        "description": "Underwriting exists, but the property is not yet actively in offer / purchase execution.",
        "primary_action": "Decide whether to pursue",
    },
    "offer": {
        "label": "Offer",
        "description": "Property passed analysis and is now in active acquisition execution.",
        "primary_action": "Track offer / close status",
    },
    "acquired": {
        "label": "Acquired",
        "description": "Property has been purchased and is entering post-close operational setup.",
        "primary_action": "Start rehab/compliance setup",
    },
    "rehab": {
        "label": "Rehab",
        "description": "Rehab tasks exist and must be completed before compliance and leasing can move forward.",
        "primary_action": "Complete rehab tasks",
    },
    "compliance_readying": {
        "label": "Compliance Readying",
        "description": "Jurisdiction, compliance artifacts, and inspection readiness are being prepared.",
        "primary_action": "Complete compliance prep",
    },
    "inspection_pending": {
        "label": "Inspection Pending",
        "description": "Inspection exists or is expected, and the property must clear inspection blockers.",
        "primary_action": "Pass inspection",
    },
    "tenant_marketing": {
        "label": "Tenant Marketing",
        "description": "Property is ready to be marketed or matched for tenant placement.",
        "primary_action": "Open tenant pipeline",
    },
    "tenant_screening": {
        "label": "Tenant Screening",
        "description": "Applicants are being reviewed, matched, or progressed toward lease execution.",
        "primary_action": "Screen and assign tenant",
    },
    "leased": {
        "label": "Leased",
        "description": "Lease is active and the unit is transitioning into stable occupancy.",
        "primary_action": "Prepare move-in / first month ops",
    },
    "occupied": {
        "label": "Occupied",
        "description": "Property is occupied and should now be managed as a live asset.",
        "primary_action": "Manage operations and cashflow",
    },
    "turnover": {
        "label": "Turnover",
        "description": "A prior occupancy ended and the property needs either re-compliance or re-evaluation.",
        "primary_action": "Route turnover work",
    },
    "maintenance": {
        "label": "Maintenance",
        "description": "Property is in long-run management mode with maintenance and admin operations.",
        "primary_action": "Work maintenance / support queue",
    },
}


def clamp_stage(stage: Optional[str]) -> str:
    raw = str(stage or "").strip().lower()
    if raw in _RANK:
        return raw
    if raw in _ALIASES:
        return _ALIASES[raw]
    return "discovered"


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
    cur = clamp_stage(stage)
    if cur == "turnover":
        return "maintenance"
    idx = stage_rank(cur)
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
    return _STAGE_META.get(key, {}).get("label", key.replace("_", " ").title())


def stage_meta(stage: Optional[str]) -> dict[str, str]:
    key = clamp_stage(stage)
    meta = _STAGE_META.get(key, {})
    return {
        "key": key,
        "label": meta.get("label", key.replace("_", " ").title()),
        "description": meta.get("description", ""),
        "primary_action": meta.get("primary_action", ""),
    }


def stage_catalog() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for idx, stage in enumerate(STAGES):
        meta = stage_meta(stage)
        rows.append(
            {
                "key": stage,
                "rank": idx,
                "label": meta["label"],
                "description": meta["description"],
                "primary_action": meta["primary_action"],
            }
        )
    return rows


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
    deal_exists: bool,
    underwriting_complete: bool,
    offer_ready: bool,
    acquired_complete: bool,
    rehab_complete: bool,
    inspection_exists: bool,
    compliance_complete: bool,
    lease_exists: bool,
    tenant_complete: bool,
    cash_complete: bool,
    occupied_complete: bool,
    turnover_active: bool = False,
) -> GateResult:
    cur = clamp_stage(current_stage)
    decision = str(decision_bucket or "REVIEW").strip().upper()

    if cur == "maintenance":
        return GateResult(ok=False, blocked_reason="Already in final management stage.", blockers=[])

    if cur == "turnover":
        if compliance_complete:
            return GateResult(ok=True, allowed_next_stage="tenant_marketing")
        return GateResult(ok=True, allowed_next_stage="inspection_pending")

    if cur == "discovered":
        if not deal_exists:
            return GateResult(
                ok=False,
                blocked_reason="Property must be shortlisted before it can move deeper into the workflow.",
                allowed_next_stage="shortlisted",
                blockers=["not_shortlisted"],
            )
        return GateResult(ok=True, allowed_next_stage="shortlisted")

    if cur == "shortlisted":
        if not underwriting_complete:
            return GateResult(
                ok=False,
                blocked_reason="Run underwriting before the property can move forward.",
                allowed_next_stage="underwritten",
                blockers=["missing_underwriting"],
            )
        return GateResult(ok=True, allowed_next_stage="underwritten")

    if cur == "underwritten":
        if decision == "REJECT":
            return GateResult(
                ok=False,
                blocked_reason="Rejected properties cannot move into acquisition until assumptions change.",
                allowed_next_stage=None,
                blockers=["decision_reject"],
            )
        if not offer_ready:
            return GateResult(
                ok=False,
                blocked_reason="The property is not yet ready for active acquisition execution.",
                allowed_next_stage="offer",
                blockers=["offer_not_ready"],
            )
        return GateResult(ok=True, allowed_next_stage="offer")

    if cur == "offer":
        if not acquired_complete:
            return GateResult(
                ok=False,
                blocked_reason="Close the acquisition before moving into post-close workflow.",
                allowed_next_stage="acquired",
                blockers=["not_acquired"],
            )
        return GateResult(ok=True, allowed_next_stage="acquired")

    if cur == "acquired":
        if not rehab_complete:
            return GateResult(ok=True, allowed_next_stage="rehab")
        return GateResult(ok=True, allowed_next_stage="compliance_readying")

    if cur == "rehab":
        if not rehab_complete:
            return GateResult(
                ok=False,
                blocked_reason="Complete rehab tasks and clear blockers first.",
                allowed_next_stage="compliance_readying",
                blockers=["rehab_incomplete"],
            )
        return GateResult(ok=True, allowed_next_stage="compliance_readying")

    if cur == "compliance_readying":
        if not inspection_exists:
            return GateResult(
                ok=False,
                blocked_reason="Create or schedule inspection before advancing.",
                allowed_next_stage="inspection_pending",
                blockers=["missing_inspection"],
            )
        return GateResult(ok=True, allowed_next_stage="inspection_pending")

    if cur == "inspection_pending":
        if not compliance_complete:
            return GateResult(
                ok=False,
                blocked_reason="Pass inspection and clear compliance blockers first.",
                allowed_next_stage="tenant_marketing",
                blockers=["compliance_incomplete"],
            )
        return GateResult(ok=True, allowed_next_stage="tenant_marketing")

    if cur == "tenant_marketing":
        if not lease_exists:
            return GateResult(
                ok=False,
                blocked_reason="Open tenant workflow and create a placement candidate before lease-up.",
                allowed_next_stage="tenant_screening",
                blockers=["no_tenant_progress"],
            )
        return GateResult(ok=True, allowed_next_stage="tenant_screening")

    if cur == "tenant_screening":
        if not tenant_complete:
            return GateResult(
                ok=False,
                blocked_reason="An active lease is required before the unit becomes leased.",
                allowed_next_stage="leased",
                blockers=["tenant_incomplete"],
            )
        return GateResult(ok=True, allowed_next_stage="leased")

    if cur == "leased":
        if not occupied_complete:
            return GateResult(
                ok=False,
                blocked_reason="Record actual operating activity before the unit is treated as occupied ops.",
                allowed_next_stage="occupied",
                blockers=["occupancy_not_confirmed"],
            )
        return GateResult(ok=True, allowed_next_stage="occupied")

    if cur == "occupied":
        if turnover_active:
            return GateResult(ok=True, allowed_next_stage="turnover")
        return GateResult(ok=True, allowed_next_stage="maintenance")

    return GateResult(ok=False, blocked_reason="Unknown workflow state.", blockers=["unknown_state"])
