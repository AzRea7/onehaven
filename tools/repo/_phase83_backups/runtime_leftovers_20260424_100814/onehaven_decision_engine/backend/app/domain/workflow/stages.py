from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

STAGES: list[str] = [
    "discovered",
    "shortlisted",
    "underwritten",
    "pursuing",
    "offer_prep",
    "offer_ready",
    "offer_submitted",
    "negotiating",
    "under_contract",
    "due_diligence",
    "closing",
    "owned",
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
    "acquisition": "pursuing",
    "offer": "offer_ready",
    "procurement": "offer_prep",
    "buy": "pursuing",
    "owned": "owned",
    "acquired": "owned",
    "compliance": "compliance_readying",
    "inspection": "inspection_pending",
    "tenant": "tenant_screening",
    "lease": "leased",
    "cash": "occupied",
    "cashflow": "occupied",
    "management": "maintenance",
    "operations": "maintenance",
    **{stage: stage for stage in STAGES},
}

_STAGE_META: dict[str, dict[str, str]] = {
    "discovered": {"label": "Discovered", "description": "Property exists in the system but has not been shortlisted into active review.", "primary_action": "Save or shortlist property"},
    "shortlisted": {"label": "Shortlisted", "description": "Property is in the investor queue and needs underwriting.", "primary_action": "Run underwriting"},
    "underwritten": {"label": "Underwritten", "description": "Underwriting exists, but acquisition work has not started.", "primary_action": "Decide whether to pursue"},
    "pursuing": {"label": "Pursuing", "description": "The team intentionally started acquisition work before an offer is submitted.", "primary_action": "Open pre-offer work"},
    "offer_prep": {"label": "Offer Prep", "description": "Buyer-side pre-offer materials, agents, and numbers are being organized.", "primary_action": "Prepare offer packet"},
    "offer_ready": {"label": "Offer Ready", "description": "Minimum pre-offer criteria are complete and an offer can be sent.", "primary_action": "Submit offer"},
    "offer_submitted": {"label": "Offer Submitted", "description": "Offer has been sent and is awaiting response.", "primary_action": "Track seller response"},
    "negotiating": {"label": "Negotiating", "description": "Counteroffers or deal terms are actively being negotiated.", "primary_action": "Negotiate terms"},
    "under_contract": {"label": "Under Contract", "description": "Offer has been accepted and the property is now under contract.", "primary_action": "Start due diligence"},
    "due_diligence": {"label": "Due Diligence", "description": "Inspection, title, financing, and contingencies are being cleared.", "primary_action": "Clear diligence blockers"},
    "closing": {"label": "Closing", "description": "The deal is moving through final close steps.", "primary_action": "Close the purchase"},
    "owned": {"label": "Owned", "description": "The property has closed and is owned.", "primary_action": "Start post-close execution"},
    "rehab": {"label": "Rehab", "description": "Rehab tasks must be completed before compliance and leasing can move forward.", "primary_action": "Complete rehab tasks"},
    "compliance_readying": {"label": "Compliance Readying", "description": "Jurisdiction, compliance artifacts, and inspection readiness are being prepared.", "primary_action": "Complete compliance prep"},
    "inspection_pending": {"label": "Inspection Pending", "description": "Inspection exists or is expected, and the property must clear inspection blockers.", "primary_action": "Pass inspection"},
    "tenant_marketing": {"label": "Tenant Marketing", "description": "Property is ready to be marketed or matched for tenant placement.", "primary_action": "Open tenant pipeline"},
    "tenant_screening": {"label": "Tenant Screening", "description": "Applicants are being reviewed toward lease execution.", "primary_action": "Screen and assign tenant"},
    "leased": {"label": "Leased", "description": "Lease is active and the unit is transitioning into occupancy.", "primary_action": "Prepare move-in"},
    "occupied": {"label": "Occupied", "description": "Property is occupied and should now be managed as a live asset.", "primary_action": "Manage operations and cashflow"},
    "turnover": {"label": "Turnover", "description": "A prior occupancy ended and the property needs re-compliance or re-evaluation.", "primary_action": "Route turnover work"},
    "maintenance": {"label": "Maintenance", "description": "Property is in long-run management mode.", "primary_action": "Work maintenance queue"},
}

TRANSITION_REASONS: set[str] = {
    "shortlisted",
    "underwriting_complete",
    "start_acquisition",
    "offer_prep_ready",
    "offer_ready",
    "offer_submitted",
    "negotiation_started",
    "under_contract",
    "due_diligence_started",
    "due_diligence_complete",
    "closing_started",
    "acquisition_complete",
    "rehab_started",
    "rehab_complete",
    "inspection_scheduled",
    "inspection_passed",
    "tenant_marketing_ready",
    "tenant_screening_started",
    "lease_signed",
    "cashflow_started",
    "occupied_stabilized",
    "vacancy_detected",
    "maintenance_required",
    "manual_override",
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
    return {"key": key, "label": meta.get("label", key.replace("_", " ").title()), "description": meta.get("description", ""), "primary_action": meta.get("primary_action", "")}


def stage_catalog() -> list[dict[str, Any]]:
    return [{"key": s, "rank": i, **stage_meta(s)} for i, s in enumerate(STAGES)]


def infer_transition_reason(previous_stage: Optional[str], current_stage: Optional[str]) -> Optional[str]:
    prev_key = clamp_stage(previous_stage) if previous_stage is not None else None
    cur_key = clamp_stage(current_stage)
    if prev_key is None or prev_key == cur_key:
        return None
    mapping = {
        ("discovered", "shortlisted"): "shortlisted",
        ("shortlisted", "underwritten"): "underwriting_complete",
        ("underwritten", "pursuing"): "start_acquisition",
        ("pursuing", "offer_prep"): "offer_prep_ready",
        ("offer_prep", "offer_ready"): "offer_ready",
        ("offer_ready", "offer_submitted"): "offer_submitted",
        ("offer_submitted", "negotiating"): "negotiation_started",
        ("negotiating", "under_contract"): "under_contract",
        ("under_contract", "due_diligence"): "due_diligence_started",
        ("due_diligence", "closing"): "due_diligence_complete",
        ("closing", "owned"): "acquisition_complete",
        ("owned", "rehab"): "rehab_started",
        ("rehab", "compliance_readying"): "rehab_complete",
        ("compliance_readying", "inspection_pending"): "inspection_scheduled",
        ("inspection_pending", "tenant_marketing"): "inspection_passed",
        ("tenant_marketing", "tenant_screening"): "tenant_screening_started",
        ("tenant_screening", "leased"): "lease_signed",
        ("leased", "occupied"): "cashflow_started",
        ("occupied", "maintenance"): "maintenance_required",
        ("occupied", "turnover"): "vacancy_detected",
        ("leased", "turnover"): "vacancy_detected",
        ("tenant_screening", "turnover"): "vacancy_detected",
        ("turnover", "inspection_pending"): "inspection_scheduled",
        ("turnover", "tenant_marketing"): "tenant_marketing_ready",
    }
    return mapping.get((prev_key, cur_key), f"{prev_key}_to_{cur_key}")


@dataclass(frozen=True)
class GateResult:
    ok: bool
    blocked_reason: Optional[str] = None
    allowed_next_stage: Optional[str] = None
    blockers: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {"ok": self.ok, "blocked_reason": self.blocked_reason, "allowed_next_stage": self.allowed_next_stage, "blockers": list(self.blockers)}


def gate_for_next_stage(
    *,
    current_stage: str,
    decision_bucket: str,
    deal_exists: bool,
    underwriting_complete: bool,
    start_acquisition_ready: bool = False,
    acquisition_started: bool = False,
    offer_prep_complete: bool = False,
    offer_packet_ready: bool = False,
    offer_submitted_flag: bool = False,
    negotiation_started: bool = False,
    under_contract_flag: bool = False,
    due_diligence_complete: bool = False,
    acquired_complete: bool = False,
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
        return GateResult(ok=False, blocked_reason="Already in final management stage.")
    if cur == "turnover":
        return GateResult(ok=True, allowed_next_stage="inspection_pending" if not compliance_complete else "tenant_marketing")
    if cur == "discovered":
        return GateResult(ok=deal_exists, blocked_reason=None if deal_exists else "Property must be shortlisted first.", allowed_next_stage="shortlisted", blockers=[] if deal_exists else ["not_shortlisted"])
    if cur == "shortlisted":
        return GateResult(ok=underwriting_complete, blocked_reason=None if underwriting_complete else "Run underwriting before moving forward.", allowed_next_stage="underwritten", blockers=[] if underwriting_complete else ["missing_underwriting"])
    if cur == "underwritten":
        if decision == "REJECT":
            return GateResult(ok=False, blocked_reason="Rejected properties cannot enter acquisition.", blockers=["decision_reject"])
        return GateResult(ok=start_acquisition_ready, blocked_reason=None if start_acquisition_ready else "Minimum pre-offer pursuit criteria are not complete.", allowed_next_stage="pursuing", blockers=[] if start_acquisition_ready else ["start_acquisition_not_ready"])
    if cur == "pursuing":
        return GateResult(ok=acquisition_started, blocked_reason=None if acquisition_started else "Acquisition pursuit has not been opened.", allowed_next_stage="offer_prep", blockers=[] if acquisition_started else ["acquisition_not_started"])
    if cur == "offer_prep":
        return GateResult(ok=offer_prep_complete, blocked_reason=None if offer_prep_complete else "Offer prep is still incomplete.", allowed_next_stage="offer_ready", blockers=[] if offer_prep_complete else ["offer_prep_incomplete"])
    if cur == "offer_ready":
        return GateResult(ok=offer_packet_ready, blocked_reason=None if offer_packet_ready else "Offer package is not ready to send.", allowed_next_stage="offer_submitted", blockers=[] if offer_packet_ready else ["offer_packet_incomplete"])
    if cur == "offer_submitted":
        return GateResult(ok=offer_submitted_flag, blocked_reason=None if offer_submitted_flag else "Offer has not been recorded as submitted.", allowed_next_stage="negotiating", blockers=[] if offer_submitted_flag else ["offer_not_submitted"])
    if cur == "negotiating":
        return GateResult(ok=negotiation_started or under_contract_flag, blocked_reason=None if (negotiation_started or under_contract_flag) else "Negotiation has not started.", allowed_next_stage="under_contract", blockers=[] if (negotiation_started or under_contract_flag) else ["negotiation_not_started"])
    if cur == "under_contract":
        return GateResult(ok=under_contract_flag, blocked_reason=None if under_contract_flag else "Property is not under contract yet.", allowed_next_stage="due_diligence", blockers=[] if under_contract_flag else ["not_under_contract"])
    if cur == "due_diligence":
        return GateResult(ok=due_diligence_complete, blocked_reason=None if due_diligence_complete else "Due diligence is still open.", allowed_next_stage="closing", blockers=[] if due_diligence_complete else ["due_diligence_incomplete"])
    if cur == "closing":
        return GateResult(ok=acquired_complete, blocked_reason=None if acquired_complete else "Close the acquisition before moving on.", allowed_next_stage="owned", blockers=[] if acquired_complete else ["not_owned"])
    if cur == "owned":
        return GateResult(ok=True, allowed_next_stage="rehab")
    if cur == "rehab":
        return GateResult(ok=rehab_complete, blocked_reason=None if rehab_complete else "Complete rehab tasks and clear blockers first.", allowed_next_stage="compliance_readying", blockers=[] if rehab_complete else ["rehab_incomplete"])
    if cur == "compliance_readying":
        return GateResult(ok=inspection_exists, blocked_reason=None if inspection_exists else "Create or schedule inspection before advancing.", allowed_next_stage="inspection_pending", blockers=[] if inspection_exists else ["missing_inspection"])
    if cur == "inspection_pending":
        return GateResult(ok=compliance_complete, blocked_reason=None if compliance_complete else "Pass inspection and clear compliance blockers first.", allowed_next_stage="tenant_marketing", blockers=[] if compliance_complete else ["compliance_incomplete"])
    if cur == "tenant_marketing":
        return GateResult(ok=lease_exists, blocked_reason=None if lease_exists else "Open tenant workflow before lease-up.", allowed_next_stage="tenant_screening", blockers=[] if lease_exists else ["no_tenant_progress"])
    if cur == "tenant_screening":
        return GateResult(ok=tenant_complete, blocked_reason=None if tenant_complete else "An active lease is required before the unit becomes leased.", allowed_next_stage="leased", blockers=[] if tenant_complete else ["lease_not_active"])
    if cur == "leased":
        return GateResult(ok=cash_complete, blocked_reason=None if cash_complete else "Cashflow records must start before the property can be treated as occupied.", allowed_next_stage="occupied", blockers=[] if cash_complete else ["cash_not_started"])
    if cur == "occupied":
        return GateResult(ok=True, allowed_next_stage="turnover" if turnover_active else "maintenance")
    return GateResult(ok=False, blocked_reason="Unable to determine the next workflow stage.", blockers=["unknown_state"])
