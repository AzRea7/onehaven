# backend/app/schemas.py
# FULL FILE replacement (updated to match your current API + policy models)
from __future__ import annotations

import json
from datetime import datetime
from typing import Optional, List, Any, Literal, Dict

from pydantic import BaseModel, Field, HttpUrl, ConfigDict, model_validator, field_validator


# --------------------
# Imports / Snapshots
# --------------------

AcquisitionDocumentKind = Literal[
    "purchase_agreement",
    "loan_documents",
    "loan_estimate",
    "closing_disclosure",
    "title_documents",
    "insurance_binder",
    "inspection_report",
    "other",
]


class AcquisitionRecordUpdate(BaseModel):
    status: str | None = None
    waiting_on: str | None = None
    next_step: str | None = None
    contract_date: str | None = None
    target_close_date: str | None = None
    closing_date: str | None = None
    purchase_price: float | None = None
    earnest_money: float | None = None
    loan_amount: float | None = None
    loan_type: str | None = None
    interest_rate: float | None = None
    cash_to_close: float | None = None
    closing_costs: float | None = None
    seller_credits: float | None = None
    title_company: str | None = None
    escrow_officer: str | None = None
    notes: str | None = None
    contacts_json: list[dict[str, Any]] | None = None
    milestones_json: list[dict[str, Any]] | None = None

    @field_validator(
        "status",
        "waiting_on",
        "next_step",
        "loan_type",
        "title_company",
        "escrow_officer",
        "notes",
        mode="before",
    )
    @classmethod
    def normalize_optional_text(cls, value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None


class AcquisitionDocumentCreate(BaseModel):
    kind: AcquisitionDocumentKind = "other"
    name: str = Field(min_length=1, max_length=255)
    status: str | None = "received"
    source_url: str | None = None
    extracted_text: str | None = None
    extracted_fields: dict[str, Any] | None = None
    notes: str | None = None

    @field_validator("name", "status", "notes", "source_url", "extracted_text", mode="before")
    @classmethod
    def normalize_text(cls, value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None


class AcquisitionUploadResponse(BaseModel):
    ok: bool = True
    document: dict[str, Any]


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



class IngestionSourceBase(BaseModel):
    provider: str
    slug: str
    display_name: str
    source_type: str = "api"
    is_enabled: bool = True
    base_url: Optional[str] = None
    schedule_cron: Optional[str] = None
    sync_interval_minutes: Optional[int] = 60
    config_json: dict[str, Any] = Field(default_factory=dict)
    credentials_json: dict[str, Any] = Field(default_factory=dict)


class IngestionSourceCreate(IngestionSourceBase):
    pass


class IngestionSourceUpdate(BaseModel):
    display_name: Optional[str] = None
    is_enabled: Optional[bool] = None
    status: Optional[str] = None
    base_url: Optional[str] = None
    schedule_cron: Optional[str] = None
    sync_interval_minutes: Optional[int] = None
    config_json: Optional[dict[str, Any]] = None
    credentials_json: Optional[dict[str, Any]] = None


class IngestionSourceOut(BaseModel):
    id: int
    org_id: int
    provider: str
    slug: str
    display_name: str
    source_type: str
    status: str
    is_enabled: bool
    base_url: Optional[str] = None
    webhook_secret_hint: Optional[str] = None
    schedule_cron: Optional[str] = None
    sync_interval_minutes: Optional[int] = None
    config_json: dict[str, Any] = Field(default_factory=dict)
    cursor_json: dict[str, Any] = Field(default_factory=dict)
    last_synced_at: Optional[datetime] = None
    last_success_at: Optional[datetime] = None
    last_failure_at: Optional[datetime] = None
    next_scheduled_at: Optional[datetime] = None
    last_error_summary: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class IngestionRunOut(BaseModel):
    id: int
    org_id: int
    source_id: int
    trigger_type: str
    status: str
    started_at: datetime
    finished_at: Optional[datetime] = None
    records_seen: int
    records_imported: int
    properties_created: int
    properties_updated: int
    deals_created: int
    deals_updated: int
    rent_rows_upserted: int
    photos_upserted: int
    duplicates_skipped: int
    invalid_rows: int
    retry_count: int
    error_summary: Optional[str] = None
    error_json: Optional[dict[str, Any]] = None
    summary_json: Optional[dict[str, Any]] = None

    model_config = {"from_attributes": True}


class IngestionRunListItem(BaseModel):
    id: int
    source_id: int
    source_label: str
    provider: str
    trigger_type: str
    status: str
    started_at: datetime
    finished_at: Optional[datetime] = None
    records_seen: int
    records_imported: int
    duplicates_skipped: int
    invalid_rows: int
    error_summary: Optional[str] = None


class IngestionSyncRequest(BaseModel):
    trigger_type: str = "manual"


class IngestionWebhookIn(BaseModel):
    external_record_id: Optional[str] = None
    event_type: Optional[str] = None
    payload: dict[str, Any] = Field(default_factory=dict)


class IngestionOverviewOut(BaseModel):
    sources_connected: int
    sources_enabled: int
    last_sync_at: Optional[datetime] = None
    success_runs_24h: int
    failed_runs_24h: int
    records_imported_24h: int
    duplicates_skipped_24h: int
# --------------------
# Evaluation / Survivors
# --------------------
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


# --------------------
# Properties / Deals
# --------------------
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

    # optional on create; typically computed later
    lat: Optional[float] = None
    lng: Optional[float] = None
    county: Optional[str] = None

    # risk metadata
    is_red_zone: bool = False
    crime_density: Optional[float] = None
    crime_score: Optional[float] = None
    offender_count: Optional[int] = None
    crime_band: Optional[str] = None
    crime_source: Optional[str] = None
    crime_method: Optional[str] = None
    crime_radius_miles: Optional[float] = None
    crime_area_sq_miles: Optional[float] = None
    crime_area_type: Optional[str] = None
    crime_incident_count: Optional[int] = None
    crime_weighted_incident_count: Optional[float] = None
    crime_nearest_incident_miles: Optional[float] = None
    crime_dataset_version: Optional[str] = None
    crime_confidence: Optional[float] = None
    investment_area_band: Optional[str] = None
    offender_band: Optional[str] = None
    offender_source: Optional[str] = None
    offender_radius_miles: Optional[float] = None
    nearest_offender_miles: Optional[float] = None
    risk_score: Optional[float] = None
    risk_band: Optional[str] = None
    risk_summary: Optional[str] = None
    risk_confidence: Optional[float] = None
    risk_last_computed_at: Optional[datetime] = None


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

    # pipeline gating fields (decision + acquisition)
    decision: Optional[Literal["buy", "pass", "watch"]] = None
    purchase_price: Optional[float] = None
    closing_date: Optional[datetime] = None
    loan_amount: Optional[float] = None


class DealOut(DealCreate):
    id: int
    org_id: Optional[int] = None
    created_at: datetime
    updated_at: Optional[datetime] = None
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
    property: "PropertyOut"
    deal: DealOut


# --------------------
# Rent Assumptions / Jurisdiction (rules)
# --------------------
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




class FinancialEnrichmentOut(BaseModel):
    ok: bool = True
    property_id: int
    annual_amount: Optional[float] = None
    annual_rate: Optional[float] = None
    source: Optional[str] = None
    confidence: Optional[float] = None
    year: Optional[int] = None
    cached: bool = False


class FinancialEnrichmentBatchIn(BaseModel):
    property_ids: List[int] = Field(default_factory=list)
    force: bool = False

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


# --------------------
# Rent Comps + Observations + Calibration
# --------------------
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

    run_id: Optional[int] = None
    created_at: Optional[datetime] = None


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

class PropertyListingMetadataOut(BaseModel):
    listing_status: Optional[str] = None
    listing_hidden: bool = False
    listing_hidden_reason: Optional[str] = None

    listing_last_seen_at: Optional[datetime] = None
    listing_removed_at: Optional[datetime] = None
    listing_listed_at: Optional[datetime] = None
    listing_created_at: Optional[datetime] = None

    listing_days_on_market: Optional[int] = None
    listing_price: Optional[float] = None

    listing_mls_name: Optional[str] = None
    listing_mls_number: Optional[str] = None
    listing_type: Optional[str] = None

    listing_zillow_url: Optional[str] = None

    listing_agent_name: Optional[str] = None
    listing_agent_phone: Optional[str] = None
    listing_agent_email: Optional[str] = None
    listing_agent_website: Optional[str] = None

    listing_office_name: Optional[str] = None
    listing_office_phone: Optional[str] = None
    listing_office_email: Optional[str] = None

class PropertyOut(PropertyCreate, PropertyListingMetadataOut):
    id: int
    org_id: Optional[int] = None

    asking_price: Optional[float] = None

    market_rent_estimate: Optional[float] = None
    rent_reasonableness_comp: Optional[float] = None
    market_reference_rent: Optional[float] = None
    section8_fmr: Optional[float] = None
    approved_rent_ceiling: Optional[float] = None
    rent_used: Optional[float] = None
    rent_cap_reason: Optional[str] = None

    loan_amount: Optional[float] = None
    monthly_debt_service: Optional[float] = None
    monthly_taxes: Optional[float] = None
    monthly_insurance: Optional[float] = None
    monthly_housing_cost: Optional[float] = None
    property_tax_annual: Optional[float] = None
    property_tax_rate_annual: Optional[float] = None
    property_tax_source: Optional[str] = None
    property_tax_confidence: Optional[float] = None
    property_tax_year: Optional[int] = None
    insurance_annual: Optional[float] = None
    insurance_source: Optional[str] = None
    insurance_confidence: Optional[float] = None
    projected_monthly_cashflow: Optional[float] = None
    rent_gap: Optional[float] = None
    dscr: Optional[float] = None

    normalized_decision: Optional[str] = None
    current_workflow_stage: Optional[str] = None
    current_workflow_stage_label: Optional[str] = None
    gate_status: Optional[str] = None

    crime_label: Optional[str] = None
    crime_score: Optional[float] = None
    crime_band: Optional[str] = None
    crime_source: Optional[str] = None
    crime_method: Optional[str] = None
    crime_radius_miles: Optional[float] = None
    crime_area_sq_miles: Optional[float] = None
    crime_area_type: Optional[str] = None
    crime_incident_count: Optional[int] = None
    crime_weighted_incident_count: Optional[float] = None
    crime_nearest_incident_miles: Optional[float] = None
    crime_dataset_version: Optional[str] = None
    crime_confidence: Optional[float] = None
    investment_area_band: Optional[str] = None
    offender_count: Optional[int] = None
    offender_band: Optional[str] = None
    offender_source: Optional[str] = None
    offender_radius_miles: Optional[float] = None
    nearest_offender_miles: Optional[float] = None
    risk_score: Optional[float] = None
    risk_band: Optional[str] = None
    risk_summary: Optional[str] = None
    risk_confidence: Optional[float] = None
    risk_last_computed_at: Optional[datetime] = None
    is_red_zone: Optional[bool] = None

    next_actions: List[Any] = Field(default_factory=list)
    blockers: List[Any] = Field(default_factory=list)

    rent_assumption: Optional[RentAssumptionOut] = None
    rent_comps: List[RentCompOut] = Field(default_factory=list)

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True, extra="allow")

class PropertyListQuery(BaseModel):
    q: Optional[str] = None
    city: Optional[str] = None
    county: Optional[str] = None
    state: Optional[str] = None

    listing_hidden: Optional[bool] = None
    listing_status: Optional[str] = None
    exclude_hidden: bool = True

# --------------------
# Underwriting Results
# --------------------
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

    bedrooms: Optional[int] = None
    bathrooms: Optional[float] = None

    rent_explain_run_id: Optional[int] = None

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

        payload = {
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
        if hasattr(data, "rent_explain_run_id"):
            payload["rent_explain_run_id"] = getattr(data, "rent_explain_run_id")
        return payload


# --------------------
# Compliance (Checklist)
# --------------------
class ChecklistItemOut(BaseModel):
    item_code: str
    category: str
    description: str
    severity: int = Field(default=2, ge=1, le=5)
    common_fail: bool = False
    applies_if: Optional[Dict[str, Any]] = None

    status: str = "todo"
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
    status: Optional[str] = Field(default=None, description="todo|in_progress|done|failed|blocked")
    proof_url: Optional[str] = None
    notes: Optional[str] = None


# --------------------
# Phase 4: Single Property View
# --------------------
class PropertyViewOut(BaseModel):
    property: PropertyOut
    deal: Optional[DealOut] = None
    rent_explain: Optional[RentExplainOut] = None
    jurisdiction_rule: Optional[JurisdictionRuleOut] = None
    jurisdiction_friction: dict
    last_underwriting_result: Optional[UnderwritingResultOut] = None
    checklist: Optional[ChecklistOut] = None
    inventory_snapshot: Optional[Dict[str, Any]] = None
# --------------------
# Jurisdiction Profiles (policy playbooks / overrides)
# --------------------
class JurisdictionProfileIn(BaseModel):
    state: str = "MI"
    county: Optional[str] = None
    city: Optional[str] = None

    friction_multiplier: float = 1.0
    pha_name: Optional[str] = None
    policy: Dict[str, Any] = Field(default_factory=dict)
    notes: Optional[str] = None


class JurisdictionProfileOut(BaseModel):
    id: int
    scope: str  # "global" | "org"
    org_id: Optional[int] = None

    state: str
    county: Optional[str] = None
    city: Optional[str] = None

    friction_multiplier: float = 1.0
    pha_name: Optional[str] = None
    policy: Dict[str, Any] = Field(default_factory=dict)
    notes: Optional[str] = None

    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class JurisdictionProfileResolveOut(BaseModel):
    matched: bool
    scope: Optional[str] = None
    match_level: Optional[str] = None  # city|county|state|None

    friction_multiplier: float = 1.0
    pha_name: Optional[str] = None
    policy: Dict[str, Any] = Field(default_factory=dict)
    notes: Optional[str] = None
    profile_id: Optional[int] = None


# --------------------
# Workflow State (NEW)
# --------------------
PaneKey = Literal[
    "acquisition",
    "investor",
    "compliance",
    "tenants",
    "management",
    "admin",
]

WorkflowStage = Literal[
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


class WorkflowStateOut(BaseModel):
    property_id: int
    current_stage: WorkflowStage
    suggested_stage: WorkflowStage
    current_stage_label: str

    current_pane: PaneKey
    current_pane_label: str
    suggested_pane: PaneKey
    suggested_pane_label: str
    route_reason: Optional[str] = None

    normalized_decision: Literal["GOOD", "REVIEW", "REJECT"]
    gate_status: Literal["OPEN", "BLOCKED"]
    gate: Dict[str, Any] = Field(default_factory=dict)

    constraints: Dict[str, Any] = Field(default_factory=dict)
    outstanding_tasks: Dict[str, Any] = Field(default_factory=dict)
    next_actions: List[str] = Field(default_factory=list)
    stage_completion_summary: Dict[str, Any] = Field(default_factory=dict)

    allowed_panes: List[PaneKey] = Field(default_factory=list)
    allowed_pane_labels: List[str] = Field(default_factory=list)

    updated_at: Optional[str] = None
    last_transitioned_at: Optional[str] = None
    stage_order: List[str] = Field(default_factory=lambda: [
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
    ])


class WorkflowDecisionIn(BaseModel):
    decision: Literal["buy", "pass", "watch"]


class WorkflowAcquisitionIn(BaseModel):
    purchase_price: float = Field(ge=0)
    closing_date: datetime
    financing_type: Optional[str] = None
    loan_amount: Optional[float] = Field(default=None, ge=0)
    interest_rate: Optional[float] = Field(default=None, ge=0)
    term_years: Optional[int] = Field(default=None, ge=1, le=50)
    down_payment_pct: Optional[float] = Field(default=None, ge=0, le=1)


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


class WorkflowTransitionOut(BaseModel):
    property_id: int
    current_stage: str
    current_stage_label: str
    next_stage: Optional[str] = None
    next_stage_label: Optional[str] = None
    current_pane: str
    current_pane_label: str
    suggested_next_pane: Optional[str] = None
    suggested_next_pane_label: Optional[str] = None
    route_reason: Optional[str] = None
    transition_reason: Optional[str] = None
    transition_at: Optional[datetime] = None
    is_auto_routed: bool = True
    decision_bucket: str
    gate: dict[str, Any] = Field(default_factory=dict)
    gate_status: str
    constraints: dict[str, Any] = Field(default_factory=dict)
    next_actions: list[str] = Field(default_factory=list)
    stage_completion_summary: dict[str, Any] = Field(default_factory=dict)


class PropertyStateOut(BaseModel):
    property_id: int
    current_stage: str
    suggested_stage: str
    current_stage_label: str
    current_pane: str
    current_pane_label: str
    suggested_pane: str
    suggested_pane_label: str
    suggested_next_pane: Optional[str] = None
    suggested_next_pane_label: Optional[str] = None
    route_reason: Optional[str] = None
    transition_reason: Optional[str] = None
    transition_at: Optional[datetime] = None
    is_auto_routed: bool = True
    allowed_panes: list[str] = Field(default_factory=list)
    allowed_pane_labels: list[str] = Field(default_factory=list)
    normalized_decision: str
    decision_bucket: str
    gate: dict[str, Any] = Field(default_factory=dict)
    gate_status: str
    constraints: dict[str, Any] = Field(default_factory=dict)
    outstanding_tasks: dict[str, Any] = Field(default_factory=dict)
    next_actions: list[str] = Field(default_factory=list)
    stage_completion_summary: dict[str, Any] = Field(default_factory=dict)
    updated_at: Optional[str] = None
    last_transitioned_at: Optional[str] = None
    stage_order: list[str] = Field(default_factory=list)

# --------------------
# Rehab
# --------------------
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


# --------------------
# Tenants + Leases
# --------------------
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


# --------------------
# Cash (Transactions)
# --------------------
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


# --------------------
# Equity (Valuations)
# --------------------
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


# --------------------
# Inspections
# --------------------
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


# --------------------
# Agents
# --------------------
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


# --------------------
# Inspection Analytics / Prediction
# --------------------
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


# --------------------
# Agent Slots (assignment layer)
# --------------------
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


# --------------------
# Principal / RBAC / Audit / Workflow
# --------------------
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


class PropertyUpdate(BaseModel):
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip: Optional[str] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[float] = None
    square_feet: Optional[int] = None
    year_built: Optional[int] = None
    has_garage: Optional[bool] = None
    property_type: Optional[str] = None

    lat: Optional[float] = None
    lng: Optional[float] = None
    county: Optional[str] = None

    is_red_zone: Optional[bool] = None
    crime_density: Optional[float] = None
    crime_score: Optional[float] = None
    offender_count: Optional[int] = None

class PropertyPhotoCreate(BaseModel):
    url: str
    source: str = "upload"
    kind: str = "unknown"
    label: str | None = None


class PropertyPhotoOut(BaseModel):
    id: int
    org_id: int | None = None
    property_id: int
    source: str
    kind: str
    label: str | None = None
    url: str
    storage_key: str | None = None
    content_type: str | None = None
    sort_order: int = 0
    created_at: datetime | None = None
    updated_at: datetime | None = None

    model_config = ConfigDict(from_attributes=True)


class RehabPhotoAnalysisIssueOut(BaseModel):
    title: str
    category: str
    severity: str
    estimated_cost: float | None = None
    blocker: bool = False
    notes: str | None = None
    evidence_photo_ids: list[int] = Field(default_factory=list)


class RehabPhotoAnalysisOut(BaseModel):
    ok: bool
    property_id: int
    photo_count: int
    summary: dict[str, int] = Field(default_factory=dict)
    issues: list[RehabPhotoAnalysisIssueOut] = Field(default_factory=list)
    created: int | None = None
    created_task_ids: list[int] = Field(default_factory=list)
    code: str | None = None

class GeoEnrichmentOut(BaseModel):
    ok: bool
    property_id: int
    lat: Optional[float] = None
    lng: Optional[float] = None
    county: Optional[str] = None
    is_red_zone: bool = False
    geocoded: bool = False
    reverse_geocoded: bool = False
    warnings: list[str] = Field(default_factory=list)

class PropertyStateOut(BaseModel):
    id: int
    org_id: int
    property_id: int
    current_stage: str
    constraints_json: Optional[str] = None
    outstanding_tasks_json: Optional[str] = None
    updated_at: datetime
    last_transitioned_at: Optional[datetime] = None
    model_config = ConfigDict(from_attributes=True)


# finalize forward refs
PropertyOut.model_rebuild()
DealIntakeOut.model_rebuild()


# --------------------
# Acquisition workflow engine additions (Step 2.2)
# --------------------

AcquisitionFieldReviewState = Literal["suggested", "accepted", "rejected", "superseded"]


class AcquisitionFieldOverrideIn(BaseModel):
    field_name: str = Field(min_length=1, max_length=128)
    value: Any
    source_document_id: int | None = None
    extraction_version: str | None = "manual_override"

    @field_validator("field_name", mode="before")
    @classmethod
    def normalize_field_name(cls, value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError("field_name is required")
        return text


class AcquisitionParticipantUpsert(BaseModel):
    role: str
    name: str
    email: str | None = None
    phone: str | None = None
    company: str | None = None
    notes: str | None = None
    source_document_id: int | None = None
    confidence: float | None = None
    extraction_version: str | None = None
    manually_overridden: bool = False
    is_primary: bool | None = None
    waiting_on: bool | None = None
    source_type: str | None = None

    @field_validator(
        "role",
        "name",
        "email",
        "phone",
        "company",
        "notes",
        "extraction_version",
        "source_type",
        mode="before",
    )
    @classmethod
    def normalize_text_fields(cls, value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None


class AcquisitionDeadlineUpsert(BaseModel):
    due_at: str
    label: str | None = None
    status: str | None = "open"
    notes: str | None = None
    source_document_id: int | None = None
    confidence: float | None = None
    extraction_version: str | None = None
    manually_overridden: bool = False

    @field_validator("due_at", "label", "status", "notes", mode="before")
    @classmethod
    def normalize_deadline_text(cls, value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None
