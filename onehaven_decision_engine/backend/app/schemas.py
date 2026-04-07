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
    notes: str | None = None


class AcquisitionDocumentOut(BaseModel):
    id: int
    property_id: int
    acquisition_id: int
    kind: str
    name: str
    status: str | None = None
    source_url: str | None = None
    storage_path: str | None = None
    mime_type: str | None = None
    size_bytes: int | None = None
    uploaded_by_user_id: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    notes: str | None = None
    extracted_text: str | None = None
    extracted_json: dict[str, Any] | None = None

    model_config = ConfigDict(from_attributes=True)


class AcquisitionFieldFactOut(BaseModel):
    id: int
    property_id: int
    acquisition_id: int
    field_name: str
    field_value: str | None = None
    normalized_json: dict[str, Any] | None = None
    source_document_id: int | None = None
    confidence: float | None = None
    review_state: str | None = None
    extraction_version: str | None = None
    manually_overridden: bool = False
    active: bool = True
    created_at: datetime | None = None
    updated_at: datetime | None = None

    model_config = ConfigDict(from_attributes=True)


class AcquisitionParticipantOut(BaseModel):
    id: int
    property_id: int
    acquisition_id: int
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
    is_primary: bool = False
    waiting_on: bool = False
    source_type: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    model_config = ConfigDict(from_attributes=True)


class AcquisitionDeadlineOut(BaseModel):
    id: int
    property_id: int
    acquisition_id: int
    due_at: str
    label: str | None = None
    status: str | None = None
    notes: str | None = None
    source_document_id: int | None = None
    confidence: float | None = None
    extraction_version: str | None = None
    manually_overridden: bool = False
    created_at: datetime | None = None
    updated_at: datetime | None = None

    model_config = ConfigDict(from_attributes=True)


class AcquisitionQueueRowOut(BaseModel):
    property_id: int
    acquisition_id: int | None = None
    address: str | None = None
    city: str | None = None
    state: str | None = None
    zip: str | None = None
    status: str | None = None
    stage: str | None = None
    waiting_on: str | None = None
    next_step: str | None = None
    target_close_date: str | None = None
    contract_date: str | None = None
    tags: list[str] = Field(default_factory=list)
    participants: list[AcquisitionParticipantOut] = Field(default_factory=list)
    upcoming_deadlines: list[AcquisitionDeadlineOut] = Field(default_factory=list)
    document_count: int = 0
    missing_document_kinds: list[str] = Field(default_factory=list)
    latest_activity_at: datetime | None = None


class AcquisitionQueueOut(BaseModel):
    ok: bool = True
    rows: list[AcquisitionQueueRowOut] = Field(default_factory=list)


class AcquisitionWorkspaceOut(BaseModel):
    ok: bool = True
    property_id: int
    acquisition_id: int | None = None
    record: dict[str, Any] | None = None
    documents: list[AcquisitionDocumentOut] = Field(default_factory=list)
    field_facts: list[AcquisitionFieldFactOut] = Field(default_factory=list)
    participants: list[AcquisitionParticipantOut] = Field(default_factory=list)
    deadlines: list[AcquisitionDeadlineOut] = Field(default_factory=list)
    notes: list[dict[str, Any]] = Field(default_factory=list)
    blockers: list[str] = Field(default_factory=list)
    summary: dict[str, Any] = Field(default_factory=dict)


class AcquisitionDocumentPreviewOut(BaseModel):
    ok: bool = True
    property_id: int
    document: AcquisitionDocumentOut
    preview_text: str | None = None
    parsed: dict[str, Any] = Field(default_factory=dict)


class AcquisitionDocumentDeleteOut(BaseModel):
    ok: bool = True
    property_id: int
    document_id: int
    deleted: bool = True
    hard_delete_file: bool = False


class AcquisitionWorkspaceResetOut(BaseModel):
    ok: bool = True
    property_id: int
    removed: bool = True
    moved_to_investor: bool = True
    deleted_document_ids: list[int] = Field(default_factory=list)
    deleted_deadline_ids: list[int] = Field(default_factory=list)
    deleted_participant_ids: list[int] = Field(default_factory=list)


class ImportRowBase(BaseModel):
    address: str
    city: str
    state: str
    zip: Optional[str] = None
    list_price: Optional[float] = None
    estimated_rent: Optional[float] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[float] = None
    area_sqft: Optional[int] = None
    source_url: Optional[HttpUrl] = None


class ImportRowCreate(ImportRowBase):
    pass


class ImportRowOut(ImportRowBase):
    id: int
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class ImportSnapshotBase(BaseModel):
    source_name: str = Field(min_length=1, max_length=120)
    source_type: str = Field(default="csv", min_length=1, max_length=40)
    rows_ingested: int = 0
    notes: Optional[str] = None


class ImportSnapshotCreate(ImportSnapshotBase):
    pass


class ImportSnapshotOut(ImportSnapshotBase):
    id: int
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


# --------------------
# Compliance documents
# --------------------

class ComplianceDocumentUploadOut(BaseModel):
    ok: bool = True
    document: "ComplianceDocumentOut"


ComplianceDocumentCategory = Literal[
    "inspection_report",
    "pass_certificate",
    "reinspection_notice",
    "repair_invoice",
    "utility_confirmation",
    "smoke_detector_proof",
    "lead_based_paint_paperwork",
    "local_jurisdiction_document",
    "approval_letter",
    "denial_letter",
    "photo_evidence",
    "other_evidence",
]


class ComplianceDocumentCreate(BaseModel):
    category: ComplianceDocumentCategory = "other_evidence"
    inspection_id: Optional[int] = None
    checklist_item_id: Optional[int] = None
    label: Optional[str] = None
    notes: Optional[str] = None
    parse_document: bool = True


class ComplianceDocumentOut(BaseModel):
    id: int
    org_id: int
    property_id: int
    inspection_id: Optional[int] = None
    checklist_item_id: Optional[int] = None
    created_by_user_id: Optional[int] = None

    category: str
    source: str

    label: Optional[str] = None
    notes: Optional[str] = None

    original_filename: Optional[str] = None
    storage_key: Optional[str] = None
    public_url: Optional[str] = None
    content_type: Optional[str] = None
    file_size_bytes: Optional[int] = None

    parse_status: Optional[str] = None
    extracted_text_preview: Optional[str] = None
    parser_meta_json: Optional[str] = None

    scan_status: Optional[str] = None
    scan_result: Optional[str] = None

    metadata_json: Optional[str] = None

    deleted_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class ComplianceDocumentStackOut(BaseModel):
    ok: bool = True
    property_id: int
    documents: list[ComplianceDocumentOut] = Field(default_factory=list)
    counts_by_category: dict[str, int] = Field(default_factory=dict)
    latest_by_category: dict[str, ComplianceDocumentOut | None] = Field(default_factory=dict)


# --------------------
# Auth / RBAC / SaaS
# --------------------

class LoginIn(BaseModel):
    email: str
    password: str


class RegisterIn(BaseModel):
    email: str
    password: str
    org_slug: Optional[str] = None
    org_name: Optional[str] = None
    role: str = "owner"


class UserOut(BaseModel):
    id: int
    email: str
    is_active: bool = True
    is_verified: bool = False

    model_config = ConfigDict(from_attributes=True)


class OrgOut(BaseModel):
    id: int
    slug: str
    name: str

    model_config = ConfigDict(from_attributes=True)


class MembershipOut(BaseModel):
    org_id: int
    user_id: int
    role: str

    model_config = ConfigDict(from_attributes=True)


class PlanOut(BaseModel):
    code: str
    name: str
    price_cents: int
    billing_interval: str
    is_active: bool = True

    model_config = ConfigDict(from_attributes=True)


class ApiKeyCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    scopes: list[str] = Field(default_factory=list)
    expires_at: Optional[datetime] = None


class ApiKeyOut(BaseModel):
    id: int
    org_id: int
    name: str
    key_prefix: str
    scopes: list[str] = Field(default_factory=list)
    expires_at: Optional[datetime] = None
    revoked_at: Optional[datetime] = None
    last_used_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class ApiKeyCreateOut(BaseModel):
    api_key: str
    meta: ApiKeyOut


# --------------------
# Properties / Core
# --------------------

class PropertyBase(BaseModel):
    address: str
    city: Optional[str] = None
    state: Optional[str] = None
    zip: Optional[str] = None

    purchase_price: Optional[float] = None
    estimated_rent: Optional[float] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[float] = None
    sqft: Optional[int] = None

    strategy: Optional[str] = None
    notes: Optional[str] = None


class PropertyCreate(PropertyBase):
    pass


class PropertyUpdate(BaseModel):
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip: Optional[str] = None

    purchase_price: Optional[float] = None
    estimated_rent: Optional[float] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[float] = None
    sqft: Optional[int] = None

    strategy: Optional[str] = None
    notes: Optional[str] = None


class PropertyOut(PropertyBase):
    id: int
    org_id: Optional[int] = None

    county: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    is_red_zone: Optional[bool] = None
    crime_density: Optional[float] = None
    crime_score: Optional[float] = None
    offender_count: Optional[int] = None

    listing_status: Optional[str] = None
    listing_price: Optional[float] = None
    listing_days_on_market: Optional[int] = None
    listing_listed_at: Optional[datetime] = None
    listing_last_seen_at: Optional[datetime] = None
    listing_removed_at: Optional[datetime] = None
    listing_created_at: Optional[datetime] = None
    listing_type: Optional[str] = None
    listing_mls_name: Optional[str] = None
    listing_mls_number: Optional[str] = None
    listing_zillow_url: Optional[str] = None
    listing_hidden: Optional[bool] = None
    hidden_reason: Optional[str] = None
    listing_agent_name: Optional[str] = None
    listing_agent_phone: Optional[str] = None
    listing_agent_email: Optional[str] = None
    listing_agent_website: Optional[str] = None
    listing_office_name: Optional[str] = None
    listing_office_phone: Optional[str] = None
    listing_office_email: Optional[str] = None

    market_rent_estimate: Optional[float] = None
    rent_reasonableness_comp: Optional[float] = None
    section8_fmr: Optional[float] = None
    approved_rent_ceiling: Optional[float] = None
    rent_used: Optional[float] = None
    rent_reason: Optional[str] = None

    monthly_debt_service: Optional[float] = None
    monthly_taxes: Optional[float] = None
    monthly_insurance: Optional[float] = None
    monthly_housing_cost: Optional[float] = None
    projected_monthly_cashflow: Optional[float] = None
    d_scr: Optional[float] = Field(default=None, alias="dscr")
    rent_gap: Optional[float] = None

    property_tax_annual: Optional[float] = None
    property_tax_rate_annual: Optional[float] = None
    property_tax_source: Optional[str] = None
    property_tax_confidence: Optional[float] = None
    property_tax_year: Optional[int] = None
    parcel_id: Optional[str] = None
    tax_lookup_status: Optional[str] = None
    tax_lookup_provider: Optional[str] = None
    tax_lookup_url: Optional[str] = None
    tax_last_verified_at: Optional[datetime] = None

    geocode_source: Optional[str] = None
    geocode_confidence: Optional[float] = None
    geocode_last_refreshed: Optional[datetime] = None

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class DealIntakeCreate(BaseModel):
    property_id: int
    strategy: Optional[str] = None
    notes: Optional[str] = None
    targets_json: dict[str, Any] = Field(default_factory=dict)


class DealIntakeOut(BaseModel):
    id: int
    property_id: int
    strategy: Optional[str] = None
    notes: Optional[str] = None
    targets_json: dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class UnderwritingRequest(BaseModel):
    arv: Optional[float] = None
    rehab_cost: Optional[float] = None
    holding_months: int = 6
    rate: Optional[float] = None
    down_pct: Optional[float] = None
    insurance_monthly: Optional[float] = None
    taxes_monthly: Optional[float] = None


class UnderwritingResultOut(BaseModel):
    id: int
    property_id: int

    arv: Optional[float] = None
    rehab_cost: Optional[float] = None
    holding_months: Optional[int] = None
    rate: Optional[float] = None
    down_pct: Optional[float] = None
    insurance_monthly: Optional[float] = None
    taxes_monthly: Optional[float] = None

    all_in_cost: Optional[float] = None
    monthly_pi: Optional[float] = None
    monthly_cashflow: Optional[float] = None
    decision: Optional[str] = None
    score: Optional[float] = None

    decision_version: Optional[str] = None
    payment_standard_pct_used: Optional[float] = None
    jurisdiction_multiplier: Optional[float] = None
    jurisdiction_reasons_json: Optional[str] = None
    rent_cap_reason: Optional[str] = None
    fmr_adjusted: Optional[float] = None

    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


# --------------------
# Tenants / Leases / Cash
# --------------------

class TenantCreate(BaseModel):
    property_id: int
    full_name: str
    email: Optional[str] = None
    phone: Optional[str] = None
    notes: Optional[str] = None


class TenantUpdate(BaseModel):
    full_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    notes: Optional[str] = None


class TenantOut(BaseModel):
    id: int
    property_id: int
    full_name: str
    email: Optional[str] = None
    phone: Optional[str] = None
    notes: Optional[str] = None
    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class LeaseCreate(BaseModel):
    property_id: int
    tenant_id: Optional[int] = None
    start_date: str
    end_date: str
    monthly_rent: float
    deposit: Optional[float] = None
    status: str = "active"
    notes: Optional[str] = None


class LeaseUpdate(BaseModel):
    tenant_id: Optional[int] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    monthly_rent: Optional[float] = None
    deposit: Optional[float] = None
    status: Optional[str] = None
    notes: Optional[str] = None


class LeaseOut(BaseModel):
    id: int
    property_id: int
    tenant_id: Optional[int] = None
    start_date: str
    end_date: str
    monthly_rent: float
    deposit: Optional[float] = None
    status: Optional[str] = None
    notes: Optional[str] = None
    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class TransactionCreate(BaseModel):
    property_id: int
    category: str
    amount: float
    occurred_at: datetime
    notes: Optional[str] = None


class TransactionOut(BaseModel):
    id: int
    property_id: int
    category: str
    amount: float
    occurred_at: datetime
    notes: Optional[str] = None
    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


# --------------------
# Rehab / Tasks / Inspections
# --------------------

class RehabTaskCreate(BaseModel):
    property_id: int
    title: str
    category: str = "general"
    status: str = "todo"
    priority: Optional[str] = None
    notes: Optional[str] = None
    cost_estimate: Optional[float] = None
    inspection_relevant: Optional[bool] = None
    vendor: Optional[str] = None
    deadline: Optional[datetime] = None


class RehabTaskUpdate(BaseModel):
    title: Optional[str] = None
    category: Optional[str] = None
    status: Optional[str] = None
    priority: Optional[str] = None
    notes: Optional[str] = None
    cost_estimate: Optional[float] = None
    inspection_relevant: Optional[bool] = None
    vendor: Optional[str] = None
    deadline: Optional[datetime] = None


class RehabTaskOut(BaseModel):
    id: int
    property_id: int
    title: str
    category: str
    status: str
    priority: Optional[str] = None
    notes: Optional[str] = None
    cost_estimate: Optional[float] = None
    inspection_relevant: Optional[bool] = None
    vendor: Optional[str] = None
    deadline: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class InspectionCreate(BaseModel):
    property_id: int
    inspection_date: Optional[datetime] = None
    status: Optional[str] = None
    notes: Optional[str] = None

    template_key: Optional[str] = None
    template_version: Optional[str] = None
    result_status: Optional[str] = None
    reinspect_required: Optional[bool] = None
    score_pct: Optional[float] = None
    blocked_count: Optional[int] = None
    fail_count: Optional[int] = None
    na_count: Optional[int] = None
    raw_payload_json: Optional[dict[str, Any] | list[Any] | str] = None

    scheduled_for: Optional[datetime] = None
    inspector_name: Optional[str] = None
    inspector_company: Optional[str] = None
    inspector_email: Optional[str] = None
    inspector_phone: Optional[str] = None
    calendar_event_id: Optional[str] = None
    reminder_offsets_json: Optional[list[int] | dict[str, Any] | str] = None
    appointment_status: Optional[str] = None
    appointment_notes: Optional[str] = None
    last_reminder_sent_at: Optional[datetime] = None
    next_reminder_due_at: Optional[datetime] = None
    ics_uid: Optional[str] = None
    ics_text: Optional[str] = None

    @field_validator("reminder_offsets_json", mode="before")
    @classmethod
    def normalize_reminder_offsets(cls, value: Any) -> Any:
        if value is None or value == "":
            return None
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            try:
                return json.loads(text)
            except Exception:
                return value
        return value

    @field_validator("raw_payload_json", mode="before")
    @classmethod
    def normalize_raw_payload(cls, value: Any) -> Any:
        if value is None or value == "":
            return None
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            try:
                return json.loads(text)
            except Exception:
                return value
        return value


class InspectionUpdate(BaseModel):
    inspection_date: Optional[datetime] = None
    status: Optional[str] = None
    notes: Optional[str] = None

    template_key: Optional[str] = None
    template_version: Optional[str] = None
    result_status: Optional[str] = None
    reinspect_required: Optional[bool] = None
    score_pct: Optional[float] = None
    blocked_count: Optional[int] = None
    fail_count: Optional[int] = None
    na_count: Optional[int] = None
    raw_payload_json: Optional[dict[str, Any] | list[Any] | str] = None

    scheduled_for: Optional[datetime] = None
    inspector_name: Optional[str] = None
    inspector_company: Optional[str] = None
    inspector_email: Optional[str] = None
    inspector_phone: Optional[str] = None
    calendar_event_id: Optional[str] = None
    reminder_offsets_json: Optional[list[int] | dict[str, Any] | str] = None
    appointment_status: Optional[str] = None
    appointment_notes: Optional[str] = None
    last_reminder_sent_at: Optional[datetime] = None
    next_reminder_due_at: Optional[datetime] = None
    ics_uid: Optional[str] = None
    ics_text: Optional[str] = None

    @field_validator("reminder_offsets_json", mode="before")
    @classmethod
    def normalize_reminder_offsets(cls, value: Any) -> Any:
        if value is None or value == "":
            return None
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            try:
                return json.loads(text)
            except Exception:
                return value
        return value

    @field_validator("raw_payload_json", mode="before")
    @classmethod
    def normalize_raw_payload(cls, value: Any) -> Any:
        if value is None or value == "":
            return None
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            try:
                return json.loads(text)
            except Exception:
                return value
        return value


class InspectionItemCreate(BaseModel):
    code: str
    category: Optional[str] = None
    status: str
    notes: Optional[str] = None
    severity: Optional[str] = None
    evidence_json: Optional[dict[str, Any] | list[Any] | str] = None


class InspectionItemOut(BaseModel):
    id: int
    inspection_id: int
    code: str
    category: Optional[str] = None
    status: str
    notes: Optional[str] = None
    severity: Optional[str] = None
    evidence_json: Optional[str] = None
    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class InspectionOut(BaseModel):
    id: int
    property_id: int
    inspection_date: Optional[datetime] = None
    status: Optional[str] = None
    notes: Optional[str] = None

    template_key: Optional[str] = None
    template_version: Optional[str] = None
    result_status: Optional[str] = None
    reinspect_required: Optional[bool] = None
    score_pct: Optional[float] = None
    blocked_count: Optional[int] = None
    fail_count: Optional[int] = None
    na_count: Optional[int] = None
    raw_payload_json: Optional[str] = None

    scheduled_for: Optional[datetime] = None
    inspector_name: Optional[str] = None
    inspector_company: Optional[str] = None
    inspector_email: Optional[str] = None
    inspector_phone: Optional[str] = None
    calendar_event_id: Optional[str] = None
    reminder_offsets_json: Optional[str] = None
    appointment_status: Optional[str] = None
    appointment_notes: Optional[str] = None
    last_reminder_sent_at: Optional[datetime] = None
    next_reminder_due_at: Optional[datetime] = None
    ics_uid: Optional[str] = None
    ics_text: Optional[str] = None

    created_at: Optional[datetime] = None
    items: list[InspectionItemOut] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


class PropertyChecklistItemOut(BaseModel):
    id: int
    property_id: int
    code: str
    category: Optional[str] = None
    status: Optional[str] = None
    severity: Optional[str] = None
    description: Optional[str] = None
    is_completed: bool = False
    notes: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class InspectionScheduleSummaryOut(BaseModel):
    ok: bool = True
    property_id: int
    appointment: dict[str, Any] | None = None
    reminders: list[dict[str, Any]] = Field(default_factory=list)
    timeline: list[dict[str, Any]] = Field(default_factory=list)


class InspectionReminderPreviewOut(BaseModel):
    inspection_id: int
    property_id: int
    reminder_offset_minutes: int
    email: dict[str, Any] = Field(default_factory=dict)
    sms: dict[str, Any] = Field(default_factory=dict)


# --------------------
# Compliance / Policy
# --------------------

class HqsRuleOut(BaseModel):
    id: int
    code: str
    category: str
    severity: str
    description: str
    template_key: Optional[str] = None
    template_version: Optional[str] = None
    sort_order: Optional[int] = None
    is_active: Optional[bool] = None
    evidence_json: Optional[str] = None
    remediation_hints_json: Optional[str] = None
    source_urls_json: Optional[str] = None
    effective_date: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class HqsAddendumOut(BaseModel):
    id: int
    org_id: int
    jurisdiction_profile_id: int
    code: str
    category: Optional[str] = None
    severity: Optional[str] = None
    description: Optional[str] = None
    template_key: Optional[str] = None
    template_version: Optional[str] = None
    sort_order: Optional[int] = None
    is_active: Optional[bool] = None
    evidence_json: Optional[str] = None
    remediation_hints_json: Optional[str] = None
    effective_date: Optional[datetime] = None
    source_urls_json: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class HudFmrRecordOut(BaseModel):
    id: int
    state: str
    area_name: str
    year: int
    bedrooms: int
    fmr: float
    source: str
    fetched_at: datetime
    raw_json: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class PolicyCatalogEntryCreate(BaseModel):
    state: str = "MI"
    county: Optional[str] = None
    city: Optional[str] = None
    pha_name: Optional[str] = None
    program_type: Optional[str] = None

    url: str
    publisher: Optional[str] = None
    title: Optional[str] = None
    notes: Optional[str] = None
    source_kind: Optional[str] = None

    is_authoritative: bool = True
    priority: int = 100
    is_active: bool = True
    is_override: bool = True
    baseline_url: Optional[str] = None


class PolicyCatalogEntryOut(BaseModel):
    id: int
    org_id: Optional[int] = None
    state: str
    county: Optional[str] = None
    city: Optional[str] = None
    pha_name: Optional[str] = None
    program_type: Optional[str] = None

    url: str
    publisher: Optional[str] = None
    title: Optional[str] = None
    notes: Optional[str] = None
    source_kind: Optional[str] = None

    is_authoritative: bool
    priority: int
    is_active: bool
    is_override: bool
    baseline_url: Optional[str] = None

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class PolicySourceCreate(BaseModel):
    state: Optional[str] = None
    county: Optional[str] = None
    city: Optional[str] = None
    pha_name: Optional[str] = None
    program_type: Optional[str] = None

    publisher: Optional[str] = None
    title: Optional[str] = None
    url: str
    content_type: Optional[str] = None
    http_status: Optional[int] = None
    retrieved_at: Optional[datetime] = None
    content_sha256: Optional[str] = None
    raw_path: Optional[str] = None
    extracted_text: Optional[str] = None
    notes: Optional[str] = None
    is_authoritative: bool = True
    normalized_categories_json: Optional[list[str] | str] = None
    freshness_status: Optional[str] = None
    freshness_reason: Optional[str] = None
    freshness_checked_at: Optional[datetime] = None
    published_at: Optional[datetime] = None
    effective_date: Optional[datetime] = None
    last_verified_at: Optional[datetime] = None


class PolicySourceOut(BaseModel):
    id: int
    org_id: Optional[int] = None

    state: Optional[str] = None
    county: Optional[str] = None
    city: Optional[str] = None
    pha_name: Optional[str] = None
    program_type: Optional[str] = None

    publisher: Optional[str] = None
    title: Optional[str] = None

    url: str
    content_type: Optional[str] = None
    http_status: Optional[int] = None
    retrieved_at: datetime
    content_sha256: Optional[str] = None
    raw_path: Optional[str] = None
    extracted_text: Optional[str] = None
    notes: Optional[str] = None
    is_authoritative: bool = True
    normalized_categories_json: Optional[str] = None
    freshness_status: Optional[str] = None
    freshness_reason: Optional[str] = None
    freshness_checked_at: Optional[datetime] = None
    published_at: Optional[datetime] = None
    effective_date: Optional[datetime] = None
    last_verified_at: Optional[datetime] = None
    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class PolicySourceVersionOut(BaseModel):
    id: int
    source_id: int
    retrieved_at: datetime
    http_status: Optional[int] = None
    content_sha256: Optional[str] = None
    raw_path: Optional[str] = None
    content_type: Optional[str] = None
    fetch_error: Optional[str] = None
    extracted_text: Optional[str] = None
    is_current: bool = False
    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class PolicyAssertionCreate(BaseModel):
    source_id: Optional[int] = None

    state: Optional[str] = None
    county: Optional[str] = None
    city: Optional[str] = None
    pha_name: Optional[str] = None
    program_type: Optional[str] = None

    rule_key: str
    rule_family: Optional[str] = None
    assertion_type: str = "document_reference"
    value_json: Optional[dict[str, Any] | list[Any] | str] = None

    effective_date: Optional[datetime] = None
    expires_at: Optional[datetime] = None

    confidence: float = 0.25
    priority: int = 100
    source_rank: int = 100

    review_status: str = "extracted"
    review_notes: Optional[str] = None
    reviewed_by_user_id: Optional[int] = None
    verification_reason: Optional[str] = None
    stale_after: Optional[datetime] = None
    superseded_by_assertion_id: Optional[int] = None

    normalized_category: Optional[str] = None
    coverage_status: str = "candidate"
    source_freshness_status: Optional[str] = None

    extracted_at: Optional[datetime] = None
    reviewed_at: Optional[datetime] = None


class PolicyAssertionOut(BaseModel):
    id: int
    org_id: Optional[int] = None
    source_id: Optional[int] = None

    state: Optional[str] = None
    county: Optional[str] = None
    city: Optional[str] = None
    pha_name: Optional[str] = None
    program_type: Optional[str] = None

    rule_key: str
    rule_family: Optional[str] = None
    assertion_type: str
    value_json: Optional[str] = None

    effective_date: Optional[datetime] = None
    expires_at: Optional[datetime] = None

    confidence: float
    priority: int
    source_rank: int

    review_status: str
    review_notes: Optional[str] = None
    reviewed_by_user_id: Optional[int] = None
    verification_reason: Optional[str] = None
    stale_after: Optional[datetime] = None
    superseded_by_assertion_id: Optional[int] = None

    normalized_category: Optional[str] = None
    coverage_status: str
    source_freshness_status: Optional[str] = None

    extracted_at: Optional[datetime] = None
    reviewed_at: Optional[datetime] = None
    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class PolicyCoverageStatusOut(BaseModel):
    id: int
    org_id: Optional[int] = None
    state: str
    county: Optional[str] = None
    city: Optional[str] = None
    pha_name: Optional[str] = None

    coverage_status: str
    production_readiness: str

    last_reviewed_at: Optional[datetime] = None
    last_source_refresh_at: Optional[datetime] = None
    verified_rule_count: int
    source_count: int
    fetch_failure_count: int
    stale_warning_count: int

    completeness_score: Optional[float] = None
    completeness_status: Optional[str] = None
    required_categories_json: Optional[str] = None
    covered_categories_json: Optional[str] = None
    missing_categories_json: Optional[str] = None
    category_norm_version: Optional[str] = None
    last_verified_at: Optional[datetime] = None
    is_stale: Optional[bool] = None
    stale_reason: Optional[str] = None
    freshest_source_at: Optional[datetime] = None
    oldest_source_at: Optional[datetime] = None
    source_freshness_json: Optional[str] = None

    notes: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


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
    "management",
    "tenants",
    "ops",
]

StageKey = Literal[
    "new",
    "screening",
    "analyzing",
    "offer",
    "under_contract",
    "owned",
    "stabilized",
    "archived",
    "pursuing",
    "offer_prep",
    "offer_ready",
    "offer_submitted",
    "negotiating",
    "due_diligence",
    "closing",
]


class PropertyStateCreate(BaseModel):
    property_id: int
    current_stage: StageKey = "new"
    constraints_json: Optional[str] = None
    outstanding_tasks_json: Optional[str] = None


class PropertyStateUpdate(BaseModel):
    current_stage: Optional[StageKey] = None
    constraints_json: Optional[str] = None
    outstanding_tasks_json: Optional[str] = None


class WorkflowTransitionIn(BaseModel):
    next_stage: StageKey
    note: Optional[str] = None
    actor: Optional[str] = None


class WorkflowGateOut(BaseModel):
    property_id: int
    current_stage: Optional[str] = None
    next_stage: Optional[str] = None
    allowed: bool
    blocking_reasons: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    required_actions: list[str] = Field(default_factory=list)


class WorkflowSummaryOut(BaseModel):
    property_id: int
    current_stage: Optional[str] = None
    pane: Optional[str] = None
    blockers: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    next_actions: list[str] = Field(default_factory=list)
    constraints: list[str] = Field(default_factory=list)
    readiness_score: Optional[float] = None
    updated_at: Optional[datetime] = None


class WorkflowEventOut(BaseModel):
    id: int
    property_id: int
    event_type: str
    from_stage: Optional[str] = None
    to_stage: Optional[str] = None
    payload_json: Optional[str] = None
    actor_user_id: Optional[int] = None
    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class InspectionTimelineRowOut(BaseModel):
    inspection_id: Optional[int] = None
    event_type: Optional[str] = None
    created_at: Optional[datetime] = None
    scheduled_for: Optional[datetime] = None
    status: Optional[str] = None
    inspector_name: Optional[str] = None
    inspector_company: Optional[str] = None
    note: Optional[str] = None
    payload: dict[str, Any] = Field(default_factory=dict)


class PropertyChecklistSummaryOut(BaseModel):
    property_id: int
    total_items: int
    completed_items: int
    blocked_items: int
    failed_items: int
    unknown_items: int
    percent_complete: float
    items: list[PropertyChecklistItemOut] = Field(default_factory=list)


class PropertyComplianceBriefOut(BaseModel):
    ok: bool = True
    property_id: int
    property: dict[str, Any] = Field(default_factory=dict)
    compliance: dict[str, Any] = Field(default_factory=dict)
    resolved_profile: dict[str, Any] = Field(default_factory=dict)
    coverage: dict[str, Any] = Field(default_factory=dict)
    source_evidence: list[dict[str, Any]] = Field(default_factory=list)
    resolved_layers: list[dict[str, Any]] = Field(default_factory=list)
    blockers: list[str] = Field(default_factory=list)
    recommendations: list[str] = Field(default_factory=list)


class ComplianceQueueRowOut(BaseModel):
    property_id: int
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    county: Optional[str] = None
    stage: Optional[str] = None
    pane: Optional[str] = None
    jurisdiction: dict[str, Any] = Field(default_factory=dict)
    compliance: dict[str, Any] = Field(default_factory=dict)
    blockers: list[str] = Field(default_factory=list)


class ComplianceQueueOut(BaseModel):
    ok: bool = True
    rows: list[ComplianceQueueRowOut] = Field(default_factory=list)


class InspectionReadinessOut(BaseModel):
    property_id: int
    latest_inspection_id: Optional[int] = None
    template_key: Optional[str] = None
    template_version: Optional[str] = None

    completion_pct: float = 0.0
    readiness_score: float = 0.0
    readiness_status: str = "unknown"
    result_status: str = "unknown"

    total_items: int = 0
    scored_items: int = 0
    passed_items: int = 0
    failed_items: int = 0
    blocked_items: int = 0
    na_items: int = 0
    unknown_items: int = 0
    failed_critical_items: int = 0

    latest_inspection_passed: bool = False
    checklist_failed_count: int = 0
    checklist_blocked_count: int = 0
    unresolved_failure_count: int = 0
    unresolved_blocked_count: int = 0
    unresolved_critical_count: int = 0

    hqs_ready: bool = False
    local_ready: bool = False
    voucher_ready: bool = False
    lease_up_ready: bool = False
    is_compliant: bool = False
    reinspect_required: bool = False

    posture: str = "unknown"
    completion_projection_pct: float = 0.0


class CompliancePhotoFindingOut(BaseModel):
    code: str
    label: str
    severity: str
    confidence: float | None = None
    rule_mapping: dict[str, Any] | None = None
    evidence_photo_ids: list[int] = Field(default_factory=list)


class CompliancePhotoAnalysisOut(BaseModel):
    ok: bool
    property_id: int
    photo_count: int
    findings: list[CompliancePhotoFindingOut] = Field(default_factory=list)
    summary: dict[str, Any] = Field(default_factory=dict)


class PhotoUploadOut(BaseModel):
    ok: bool = True
    photo_id: int
    property_id: int
    url: str
    kind: Optional[str] = None
    label: Optional[str] = None


# --------------------
# Ingestion / Markets / Sources
# --------------------

class IngestionSourceOut(BaseModel):
    id: int
    provider: str
    name: str
    source_type: str
    is_enabled: bool = True
    config_json: dict[str, Any] | str | None = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class SupportedMarketOut(BaseModel):
    slug: str
    city: Optional[str] = None
    county: Optional[str] = None
    state: Optional[str] = None
    is_supported: bool = True
    enabled_source_count: int = 0
    source_ids: list[int] = Field(default_factory=list)


class IngestionRunOut(BaseModel):
    id: int
    source_id: int
    trigger_type: Optional[str] = None
    status: str
    dataset_key: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    runtime_config_json: Optional[str] = None
    summary_json: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class MarketSyncLaunchOut(BaseModel):
    ok: bool = True
    queued: bool = True
    city: Optional[str] = None
    state: Optional[str] = None
    market: dict[str, Any] = Field(default_factory=dict)
    task_ids: list[str] = Field(default_factory=list)
    queued_count: int = 0


# --------------------
# Agents / Audit / Misc
# --------------------

class AuditEventOut(BaseModel):
    id: int
    property_id: Optional[int] = None
    event_type: str
    payload_json: Optional[str] = None
    actor_user_id: Optional[int] = None
    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class AgentRunOut(BaseModel):
    id: int
    property_id: Optional[int] = None
    agent_name: str
    status: str
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    input_json: Optional[str] = None
    output_json: Optional[str] = None
    error_text: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class NextActionsOut(BaseModel):
    property_id: int
    actions: list[str] = Field(default_factory=list)
    blockers: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class DashboardPaneCardOut(BaseModel):
    key: str
    label: str
    count: int
    subtitle: Optional[str] = None


class DashboardPanesOut(BaseModel):
    ok: bool = True
    cards: list[DashboardPaneCardOut] = Field(default_factory=list)


class RentExplainOut(BaseModel):
    ok: bool = True
    property_id: int
    market_rent_estimate: Optional[float] = None
    section8_fmr: Optional[float] = None
    approved_rent_ceiling: Optional[float] = None
    rent_used: Optional[float] = None
    rent_reason: Optional[str] = None
    notes: list[str] = Field(default_factory=list)


class ValuationOut(BaseModel):
    ok: bool = True
    property_id: int
    estimate: Optional[float] = None
    method: Optional[str] = None
    confidence: Optional[float] = None
    details: dict[str, Any] = Field(default_factory=dict)


class EquitySummaryOut(BaseModel):
    ok: bool = True
    property_id: int
    estimated_value: Optional[float] = None
    mortgage_balance: Optional[float] = None
    equity: Optional[float] = None
    details: dict[str, Any] = Field(default_factory=dict)


class RiskScoreOut(BaseModel):
    ok: bool = True
    property_id: int
    risk_score: Optional[float] = None
    risk_band: Optional[str] = None
    details: dict[str, Any] = Field(default_factory=dict)


class ConstitutionOut(BaseModel):
    ok: bool = True
    content: str


class RehabPhotoAnalysisIssueOut(BaseModel):
    code: str
    label: str
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


# --------------------
# Compliance rule engine foundation (Phase 1)
# --------------------

ComplianceTrustState = Literal["confirmed", "inferred", "unknown", "stale", "conflicting"]
ComplianceGovernanceState = Literal["draft", "approved", "active", "replaced"]
ComplianceRuleStatus = Literal["candidate", "active", "inactive", "superseded"]


class PolicySourceRegistryOut(BaseModel):
    id: int
    org_id: int | None = None
    state: str | None = None
    county: str | None = None
    city: str | None = None
    pha_name: str | None = None
    program_type: str | None = None
    url: str
    title: str | None = None
    publisher: str | None = None
    is_authoritative: bool = True

    source_name: str | None = None
    source_type: str = "local"
    jurisdiction_slug: str | None = None
    fetch_method: str = "manual"
    trust_level: float = 0.5
    refresh_interval_days: int = 30
    last_fetched_at: datetime | None = None
    last_verified_at: datetime | None = None
    registry_status: str = "active"
    fetch_config_json: str = "{}"
    registry_meta_json: str = "{}"
    fingerprint_algo: str = "sha256"
    current_fingerprint: str | None = None
    last_changed_at: datetime | None = None

    model_config = ConfigDict(from_attributes=True)


class PolicyAssertionRuleOut(BaseModel):
    id: int
    org_id: int | None = None
    source_id: int | None = None
    state: str | None = None
    county: str | None = None
    city: str | None = None
    pha_name: str | None = None
    program_type: str | None = None

    jurisdiction_slug: str | None = None
    source_level: str = "local"
    property_type: str | None = None

    rule_key: str
    rule_family: str | None = None
    rule_category: str | None = None
    assertion_type: str
    value_json: str | None = None
    required: bool = True
    blocking: bool = False

    confidence: float = 0.25
    source_citation: str | None = None
    raw_excerpt: str | None = None
    normalized_version: str = "v1"
    rule_status: ComplianceRuleStatus | str = "candidate"
    governance_state: ComplianceGovernanceState | str = "draft"
    version_group: str | None = None
    version_number: int = 1

    effective_date: datetime | None = None
    expires_at: datetime | None = None
    stale_after: datetime | None = None
    created_at: datetime | None = None
    reviewed_at: datetime | None = None

    model_config = ConfigDict(from_attributes=True)


class PropertyComplianceProjectionItemOut(BaseModel):
    id: int
    org_id: int
    projection_id: int
    property_id: int
    policy_assertion_id: int | None = None

    jurisdiction_slug: str | None = None
    program_type: str | None = None
    property_type: str | None = None
    source_level: str | None = None

    rule_key: str
    rule_category: str | None = None
    required: bool = True
    blocking: bool = False

    evaluation_status: ComplianceTrustState | str = "unknown"
    evidence_status: ComplianceTrustState | str = "unknown"
    confidence: float = 0.0
    estimated_cost: float | None = None
    estimated_days: int | None = None
    evidence_summary: str | None = None
    evidence_gap: str | None = None
    resolution_detail_json: str = "{}"
    created_at: datetime | None = None

    model_config = ConfigDict(from_attributes=True)


class PropertyComplianceProjectionOut(BaseModel):
    id: int
    org_id: int
    property_id: int
    jurisdiction_slug: str | None = None
    program_type: str | None = None
    rules_version: str = "v1"
    projection_status: str = "pending"
    projection_basis_json: str = "{}"

    blocking_count: int = 0
    unknown_count: int = 0
    stale_count: int = 0
    conflicting_count: int = 0
    readiness_score: float = 0.0
    projected_compliance_cost: float | None = None
    projected_days_to_rent: int | None = None
    confidence_score: float = 0.0
    impacted_rules_json: str = "[]"
    unresolved_evidence_gaps_json: str = "[]"
    last_projected_at: datetime | None = None
    superseded_at: datetime | None = None
    is_current: bool = True
    created_at: datetime | None = None
    updated_at: datetime | None = None

    items: list[PropertyComplianceProjectionItemOut] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


class PropertyComplianceEvidenceOut(BaseModel):
    id: int
    org_id: int
    property_id: int
    projection_item_id: int | None = None
    policy_assertion_id: int | None = None
    compliance_document_id: int | None = None
    inspection_id: int | None = None
    checklist_item_id: int | None = None

    evidence_source_type: str = "document"
    evidence_key: str | None = None
    evidence_name: str | None = None
    evidence_status: ComplianceTrustState | str = "unknown"
    proof_state: ComplianceTrustState | str = "inferred"
    satisfies_rule: bool | None = None
    observed_at: datetime | None = None
    expires_at: datetime | None = None
    resolved_at: datetime | None = None
    source_details_json: str = "{}"
    notes: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    model_config = ConfigDict(from_attributes=True)