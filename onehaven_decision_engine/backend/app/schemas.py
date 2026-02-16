from __future__ import annotations

import json
from datetime import datetime
from typing import Optional, List, Any, Literal, Dict

from pydantic import BaseModel, Field, ConfigDict, model_validator


# -------------------- Imports / Snapshots --------------------
class ImportSnapshotOut(BaseModel):
    id: int
    org_id: Optional[int] = None
    source: str
    notes: Optional[str] = None
    created_at: Optional[datetime] = None
    model_config = ConfigDict(from_attributes=True)


class ImportErrorRow(BaseModel):
    row: int
    error: str


class ImportResultOut(BaseModel):
    snapshot_id: int
    source: str
    imported: int
    skipped_duplicates: int
    errors: list[ImportErrorRow]


# -------------------- Evaluation / Survivors --------------------
class BatchEvalOut(BaseModel):
    snapshot_id: int
    total_deals: int
    pass_count: int
    review_count: int
    reject_count: int
    errors: List[str] = Field(default_factory=list)


class SurvivorOut(BaseModel):
    deal_id: int
    property_id: int
    address: str
    city: str
    zip: str

    decision: str
    score: int
    reasons: list[str]

    dscr: float
    cash_flow: float
    gross_rent_used: float
    asking_price: float

    model_config = ConfigDict(from_attributes=True)


# -------------------- Properties / Deals --------------------
class PropertyCreate(BaseModel):
    address: str
    city: str
    state: str = "MI"
    zip: str
    bedrooms: int
    bathrooms: float = 1.0
    square_feet: Optional[int] = None
    year_built: Optional[int] = None
    has_garage: bool = False
    property_type: str = "single_family"


class PropertyOut(PropertyCreate):
    id: int
    org_id: Optional[int] = None  # helpful for debugging tenancy
    rent_assumption: Optional["RentAssumptionOut"] = None
    rent_comps: List["RentCompOut"] = Field(default_factory=list)
    model_config = ConfigDict(from_attributes=True)


class DealCreate(BaseModel):
    property_id: int
    asking_price: float
    estimated_purchase_price: Optional[float] = None
    rehab_estimate: float = 0.0

    strategy: str = "section8"

    financing_type: str = "dscr"
    interest_rate: float = 0.07
    term_years: int = 30
    down_payment_pct: float = 0.20

    snapshot_id: Optional[int] = None
    source_fingerprint: Optional[str] = None
    source_raw_json: Optional[str] = None
    source: Optional[str] = None


class DealOut(DealCreate):
    id: int
    org_id: Optional[int] = None
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


class DealIntakeIn(BaseModel):
    address: str
    city: str
    state: str = "MI"
    zip: str
    bedrooms: int
    bathrooms: float = 1.0
    square_feet: Optional[int] = None
    year_built: Optional[int] = None
    has_garage: bool = False
    property_type: str = "single_family"

    purchase_price: float
    est_rehab: float = 0.0
    strategy: str = Field(default="section8", description="section8|market")

    financing_type: str = "dscr"
    interest_rate: float = 0.07
    term_years: int = 30
    down_payment_pct: float = 0.20

    snapshot_id: Optional[int] = None


class DealIntakeOut(BaseModel):
    property: PropertyOut
    deal: DealOut


# -------------------- Rent Assumptions / Jurisdiction --------------------
class RentAssumptionUpsert(BaseModel):
    market_rent_estimate: Optional[float] = None
    section8_fmr: Optional[float] = None
    approved_rent_ceiling: Optional[float] = None
    rent_reasonableness_comp: Optional[float] = None

    inventory_count: Optional[int] = None
    starbucks_minutes: Optional[int] = None


class RentAssumptionOut(RentAssumptionUpsert):
    id: int
    property_id: int
    org_id: Optional[int] = None
    created_at: Optional[datetime] = None

    rent_used: Optional[float] = None
    model_config = ConfigDict(from_attributes=True)


class JurisdictionRuleUpsert(BaseModel):
    city: str
    state: str = "MI"
    rental_license_required: bool = False

    inspection_frequency: Optional[str] = Field(default=None, description="annual|biennial|complaint")
    inspection_authority: Optional[str] = None

    typical_fail_points: Optional[List[str]] = None

    registration_fee: Optional[float] = None
    processing_days: Optional[int] = None
    tenant_waitlist_depth: Optional[str] = None

    jurisdiction_type: Optional[str] = None
    notes: Optional[str] = None


class JurisdictionRuleOut(JurisdictionRuleUpsert):
    id: int
    org_id: Optional[int] = None
    updated_at: Optional[datetime] = None
    model_config = ConfigDict(from_attributes=True)


# -------------------- Underwriting Results --------------------
class UnderwritingResultOut(BaseModel):
    id: int
    deal_id: int
    org_id: Optional[int] = None

    decision: str
    score: int
    reasons: List[str] = Field(default_factory=list)

    gross_rent_used: float
    mortgage_payment: float
    operating_expenses: float
    noi: float
    cash_flow: float
    dscr: float
    cash_on_cash: float

    break_even_rent: float
    min_rent_for_target_roi: float

    decision_version: Optional[str] = None
    payment_standard_pct_used: Optional[float] = None
    jurisdiction_multiplier: Optional[float] = None
    jurisdiction_reasons: Optional[List[str]] = None
    rent_cap_reason: Optional[str] = None
    fmr_adjusted: Optional[float] = None

    # join-derived (evaluate/results injects these)
    bedrooms: Optional[int] = None
    bathrooms: Optional[float] = None

    model_config = ConfigDict(from_attributes=True)

    @model_validator(mode="before")
    @classmethod
    def _coerce_reasons(cls, data: Any) -> Any:
        def _load_list(s: Any) -> List[str]:
            if s is None:
                return []
            if isinstance(s, list):
                return [str(x) for x in s]
            if isinstance(s, str):
                try:
                    v = json.loads(s)
                    if isinstance(v, list):
                        return [str(x) for x in v]
                except Exception:
                    pass
            return [str(s)]

        if isinstance(data, dict):
            rj = data.get("reasons_json")
            if "reasons" not in data:
                data["reasons"] = _load_list(rj)

            if "jurisdiction_reasons" not in data:
                data["jurisdiction_reasons"] = _load_list(data.get("jurisdiction_reasons_json"))

            data["jurisdiction_reasons"] = data.get("jurisdiction_reasons") or _load_list(
                data.get("jurisdiction_reasons_json")
            )
            return data

        rj = getattr(data, "reasons_json", "[]")
        jurj = getattr(data, "jurisdiction_reasons_json", None)

        return {
            "id": getattr(data, "id"),
            "deal_id": getattr(data, "deal_id"),
            "org_id": getattr(data, "org_id", None),
            "decision": getattr(data, "decision"),
            "score": getattr(data, "score"),
            "reasons": _load_list(rj),

            "gross_rent_used": getattr(data, "gross_rent_used"),
            "mortgage_payment": getattr(data, "mortgage_payment"),
            "operating_expenses": getattr(data, "operating_expenses"),
            "noi": getattr(data, "noi"),
            "cash_flow": getattr(data, "cash_flow"),
            "dscr": getattr(data, "dscr"),
            "cash_on_cash": getattr(data, "cash_on_cash"),

            "break_even_rent": getattr(data, "break_even_rent"),
            "min_rent_for_target_roi": getattr(data, "min_rent_for_target_roi"),

            "decision_version": getattr(data, "decision_version", None),
            "payment_standard_pct_used": getattr(data, "payment_standard_pct_used", None),
            "jurisdiction_multiplier": getattr(data, "jurisdiction_multiplier", None),
            "jurisdiction_reasons": _load_list(jurj),
            "rent_cap_reason": getattr(data, "rent_cap_reason", None),
            "fmr_adjusted": getattr(data, "fmr_adjusted", None),
        }


# -------------------- Compliance --------------------
class ChecklistItemOut(BaseModel):
    item_code: str
    category: str
    description: str
    severity: int = Field(default=2, ge=1, le=5)
    common_fail: bool = False
    applies_if: Optional[Dict[str, Any]] = None

    status: str = "todo"  # todo|in_progress|done|blocked
    marked_at: Optional[datetime] = None
    marked_by: Optional[str] = None
    proof_url: Optional[str] = None
    notes: Optional[str] = None


class ChecklistOut(BaseModel):
    property_id: int
    city: Optional[str] = None
    state: Optional[str] = None

    checklist_name: str = "section8_hqs_precheck"
    generated_at: datetime = Field(default_factory=datetime.utcnow)

    strategy: str = "section8"
    items: List[ChecklistItemOut] = Field(default_factory=list)


class ChecklistTemplateItemUpsert(BaseModel):
    strategy: str = "section8"
    version: str = "v1"
    code: str
    category: str
    description: str
    applies_if: Optional[Dict[str, Any]] = None
    severity: int = 2
    common_fail: bool = True


class ChecklistTemplateItemOut(ChecklistTemplateItemUpsert):
    id: int
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


class PropertyChecklistOut(BaseModel):
    id: int
    org_id: Optional[int] = None
    property_id: int
    strategy: str
    version: str
    generated_at: datetime
    items: List[ChecklistItemOut]
    model_config = ConfigDict(from_attributes=True)


class ChecklistItemUpdateIn(BaseModel):
    status: Optional[str] = Field(default=None, description="todo|in_progress|done|blocked")
    proof_url: Optional[str] = None
    notes: Optional[str] = None


# -------------------- Rent Comps + Observations + Calibration --------------------
class RentCompCreate(BaseModel):
    rent: float
    source: str = "manual"
    address: Optional[str] = None
    url: Optional[str] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[float] = None
    square_feet: Optional[int] = None
    notes: Optional[str] = None


class RentCompOut(RentCompCreate):
    id: int
    property_id: int
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


class RentCompsBatchIn(BaseModel):
    comps: List[RentCompCreate] = Field(..., min_length=1)


class RentCompsSummaryOut(BaseModel):
    property_id: int
    count: int
    median_rent: float
    mean_rent: float
    min_rent: float
    max_rent: float


class RentObservationCreate(BaseModel):
    property_id: int
    strategy: str = Field(..., description="section8 | market")
    achieved_rent: float

    tenant_portion: Optional[float] = None
    hap_portion: Optional[float] = None

    lease_start: Optional[datetime] = None
    lease_end: Optional[datetime] = None
    notes: Optional[str] = None


class RentObservationOut(RentObservationCreate):
    id: int
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


class CeilingCandidate(BaseModel):
    type: Literal["payment_standard", "rent_reasonableness", "fmr", "manual", "other"]
    value: float


class RentExplainOut(BaseModel):
    property_id: int
    strategy: str

    payment_standard_pct: float
    fmr_adjusted: Optional[float] = None

    market_rent_estimate: Optional[float] = None
    section8_fmr: Optional[float] = None
    rent_reasonableness_comp: Optional[float] = None
    approved_rent_ceiling: Optional[float] = None

    calibrated_market_rent: Optional[float] = None
    rent_used: Optional[float] = None

    ceiling_candidates: List[CeilingCandidate] = Field(default_factory=list)

    cap_reason: Optional[str] = None  # fmr|comps|override|none
    explanation: Optional[str] = None


class RentExplainBatchOut(BaseModel):
    snapshot_id: int
    strategy: str
    attempted: int
    explained: int
    errors: List[dict] = Field(default_factory=list)
    model_config = ConfigDict(from_attributes=True)


class RentCalibrationOut(BaseModel):
    zip: str
    bedrooms: int
    strategy: str
    multiplier: float
    samples: int
    mape: Optional[float] = None
    updated_at: datetime
    model_config = ConfigDict(from_attributes=True)


class RentRecomputeOut(BaseModel):
    property_id: int
    market_rent_estimate: Optional[float]
    section8_fmr: Optional[float]
    rent_reasonableness_comp: Optional[float]
    approved_rent_ceiling: Optional[float]
    calibrated_market_rent: Optional[float]
    strategy: str
    rent_used: Optional[float]


# -------------------- Phase 4: Single Property View --------------------
class PropertyViewOut(BaseModel):
    property: PropertyOut
    deal: DealOut
    rent_explain: RentExplainOut
    jurisdiction_rule: Optional[JurisdictionRuleOut] = None
    jurisdiction_friction: dict
    last_underwriting_result: Optional[UnderwritingResultOut] = None
    checklist: Optional[ChecklistOut] = None


PropertyOut.model_rebuild()


# -----------------------------
# Rehab
# -----------------------------
class RehabTaskCreate(BaseModel):
    property_id: int
    title: str
    category: str = "rehab"
    inspection_relevant: bool = True
    status: str = "todo"
    cost_estimate: float | None = None
    vendor: str | None = None
    deadline: datetime | None = None
    notes: str | None = None


class RehabTaskOut(RehabTaskCreate):
    id: int
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


# -----------------------------
# Tenants + Leases
# -----------------------------
class TenantCreate(BaseModel):
    full_name: str
    phone: str | None = None
    email: str | None = None
    voucher_status: str | None = None
    notes: str | None = None


class TenantOut(TenantCreate):
    id: int
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


class LeaseCreate(BaseModel):
    property_id: int
    tenant_id: int
    start_date: datetime
    end_date: datetime | None = None
    total_rent: float = 0.0
    tenant_portion: float | None = None
    housing_authority_portion: float | None = None
    hap_contract_status: str | None = None
    notes: str | None = None


class LeaseOut(LeaseCreate):
    id: int
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


# -----------------------------
# Cash (Transactions)
# -----------------------------
class TransactionCreate(BaseModel):
    property_id: int
    txn_date: datetime | None = None
    txn_type: str | None = None
    type: str | None = None
    amount: float
    memo: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _coerce_type(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if not data.get("txn_type") and data.get("type"):
                data["txn_type"] = data["type"]
        return data


class TransactionOut(BaseModel):
    id: int
    property_id: int
    txn_date: datetime
    txn_type: str
    amount: float
    memo: str | None
    created_at: datetime
    type: str = Field(default="")

    model_config = ConfigDict(from_attributes=True)

    @model_validator(mode="before")
    @classmethod
    def _fill_type(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if not data.get("type") and data.get("txn_type"):
                data["type"] = data["txn_type"]
        else:
            if not getattr(data, "type", "") and getattr(data, "txn_type", None):
                setattr(data, "type", getattr(data, "txn_type"))
        return data


# -----------------------------
# Equity (Valuations)
# -----------------------------
class ValuationCreate(BaseModel):
    property_id: int
    as_of: datetime | None = None
    estimated_value: float
    loan_balance: float | None = None
    notes: str | None = None


class ValuationOut(BaseModel):
    id: int
    property_id: int
    as_of: datetime
    estimated_value: float
    loan_balance: float | None
    notes: str | None
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


# -----------------------------
# Inspections
# -----------------------------
class InspectorUpsert(BaseModel):
    name: str
    agency: str | None = None


class InspectorOut(BaseModel):
    id: int
    name: str
    agency: str | None
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


class InspectionItemCreate(BaseModel):
    code: str
    failed: bool = True
    severity: int = 1
    location: str | None = None
    details: str | None = None
    resolved_at: datetime | None = None
    resolution_notes: str | None = None


class InspectionItemOut(InspectionItemCreate):
    id: int
    inspection_id: int
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


class InspectionCreate(BaseModel):
    property_id: int
    inspector_id: int | None = None
    inspection_date: datetime | None = None
    passed: bool = False
    reinspect_required: bool = False
    notes: str | None = None
    items: List[InspectionItemCreate] = Field(default_factory=list)


class InspectionOut(BaseModel):
    id: int
    property_id: int
    inspector_id: int | None
    inspection_date: datetime
    passed: bool
    reinspect_required: bool
    notes: str | None
    created_at: datetime
    items: List[InspectionItemOut] = Field(default_factory=list)
    model_config = ConfigDict(from_attributes=True)


# -----------------------------
# Agents
# -----------------------------
class AgentRunCreate(BaseModel):
    property_id: int | None = None
    agent_key: str
    status: str = "queued"
    input_json: str | None = None


class AgentSlotOut(BaseModel):
    key: str
    title: str
    description: str | None = None
    needs_human: bool = False


class AgentSpecOut(BaseModel):
    agent_key: str
    title: str
    description: str | None = None
    needs_human: bool = False
    category: str | None = None
    sidebar_slots: list[AgentSlotOut] = Field(default_factory=list)
    model_config = ConfigDict(from_attributes=True)


class AgentRunOut(BaseModel):
    id: int
    org_id: Optional[int] = None
    property_id: int | None
    agent_key: str
    status: str
    input_json: str | None
    output_json: str | None
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


class AgentMessageCreate(BaseModel):
    thread_key: str
    sender: str
    message: str
    recipient: str | None = None


class AgentMessageOut(BaseModel):
    id: int
    org_id: Optional[int] = None
    thread_key: str
    sender: str
    message: str
    recipient: str | None
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


# -----------------------------
# Inspection Analytics / Prediction
# -----------------------------
class InspectionItemResolve(BaseModel):
    resolution_notes: str | None = None
    resolved_at: datetime | None = None


class FailPointStat(BaseModel):
    code: str
    count: int
    severity: int | None = None


class PredictFailPointsOut(BaseModel):
    city: str
    inspector: str | None = None
    window_inspections: int
    top_fail_points: List[FailPointStat] = Field(default_factory=list)


class ComplianceStatsOut(BaseModel):
    city: str
    inspections: int
    pass_rate: float
    reinspect_rate: float
    top_fail_points: List[FailPointStat] = Field(default_factory=list)


class AgentSlotSpecOut(BaseModel):
    slot_key: str
    title: str
    description: str
    owner_type: str
    default_status: str


class AgentSlotAssignmentUpsert(BaseModel):
    slot_key: str
    property_id: int | None = None
    owner_type: str | None = None
    assignee: str | None = None
    status: str | None = None
    notes: str | None = None


class AgentSlotAssignmentOut(BaseModel):
    id: int
    org_id: Optional[int] = None
    slot_key: str
    property_id: int | None
    owner_type: str
    assignee: str | None
    status: str
    notes: str | None
    updated_at: datetime
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


# -----------------------------
# Principal / RBAC / Audit / Workflow
# -----------------------------
class PrincipalOut(BaseModel):
    org_id: int
    org_slug: str
    user_id: int
    email: str
    role: str


class OrganizationOut(BaseModel):
    id: int
    slug: str
    name: str
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


class AppUserOut(BaseModel):
    id: int
    email: str
    display_name: Optional[str] = None
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


class OrgMembershipOut(BaseModel):
    id: int
    org_id: int
    user_id: int
    role: str
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


class AuditEventOut(BaseModel):
    id: int
    org_id: int
    actor_user_id: Optional[int] = None
    action: str
    entity_type: str
    entity_id: str
    before_json: Optional[str] = None
    after_json: Optional[str] = None
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


class WorkflowEventCreate(BaseModel):
    property_id: Optional[int] = None
    event_type: str
    payload: Optional[dict[str, Any]] = None


class WorkflowEventOut(BaseModel):
    id: int
    org_id: int
    property_id: Optional[int] = None
    actor_user_id: Optional[int] = None
    event_type: str
    payload_json: Optional[str] = None
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


class PropertyStateUpsert(BaseModel):
    property_id: int
    current_stage: Optional[str] = None
    constraints: Optional[dict[str, Any]] = None
    outstanding_tasks: Optional[dict[str, Any]] = None


class PropertyStateOut(BaseModel):
    id: int
    org_id: int
    property_id: int
    current_stage: str
    constraints_json: Optional[str] = None
    outstanding_tasks_json: Optional[str] = None
    updated_at: datetime
    model_config = ConfigDict(from_attributes=True)
