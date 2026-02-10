# backend/app/schemas.py
from __future__ import annotations

import json
from datetime import datetime
from typing import Optional, List, Any

from pydantic import BaseModel, Field, ConfigDict, model_validator


# -------------------- Imports / Snapshots --------------------

class ImportSnapshotOut(BaseModel):
    id: int
    source: str
    notes: Optional[str] = None
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

    # report per-deal errors so endpoint always returns JSON
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

    # include nested objects in property response
    rent_assumption: Optional["RentAssumptionOut"] = None
    rent_comps: List["RentCompOut"] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


class DealCreate(BaseModel):
    property_id: int
    asking_price: float
    estimated_purchase_price: Optional[float] = None
    rehab_estimate: float = 0.0

    financing_type: str = "dscr"
    interest_rate: float = 0.07
    term_years: int = 30
    down_payment_pct: float = 0.20

    # optional ingestion metadata
    snapshot_id: Optional[int] = None
    source_fingerprint: Optional[str] = None
    source_raw_json: Optional[str] = None
    source: Optional[str] = None


class DealOut(DealCreate):
    id: int
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


# -------------------- Rent Assumptions / Jurisdiction --------------------

class RentAssumptionUpsert(BaseModel):
    market_rent_estimate: Optional[float] = None
    section8_fmr: Optional[float] = None
    approved_rent_ceiling: Optional[float] = None
    rent_reasonableness_comp: Optional[float] = None

    # NOTE: models.py has rent_used now, but you typically don't let clients set it.
    # If you DO want to expose it, add: rent_used: Optional[float] = None

    inventory_count: Optional[int] = None
    starbucks_minutes: Optional[int] = None


class RentAssumptionOut(RentAssumptionUpsert):
    id: int
    property_id: int
    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class JurisdictionRuleUpsert(BaseModel):
    city: str
    state: str = "MI"
    rental_license_required: bool = False
    inspection_authority: Optional[str] = None
    typical_fail_points: Optional[List[str]] = None
    registration_fee: Optional[float] = None
    processing_days: Optional[int] = None
    tenant_waitlist_depth: Optional[str] = None


class JurisdictionRuleOut(JurisdictionRuleUpsert):
    id: int
    model_config = ConfigDict(from_attributes=True)


# -------------------- Underwriting Results --------------------

class UnderwritingResultOut(BaseModel):
    """
    DB stores reasons_json (TEXT) while API wants reasons: List[str].
    This schema safely accepts dicts OR ORM rows and will never throw on bad JSON.
    """

    id: int
    deal_id: int
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

    model_config = ConfigDict(from_attributes=True)

    @model_validator(mode="before")
    @classmethod
    def _coerce_reasons(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "reasons" in data and isinstance(data["reasons"], list):
                return data

            rj = data.get("reasons_json")
            if rj is None:
                if "reasons" in data and isinstance(data["reasons"], str):
                    data["reasons"] = [data["reasons"]]
                return data

            try:
                parsed = json.loads(rj) if isinstance(rj, str) else []
                data["reasons"] = parsed if isinstance(parsed, list) else [str(parsed)]
            except Exception:
                data["reasons"] = [f"Failed to parse reasons_json: {rj!r}"]
            return data

        try:
            rj = getattr(data, "reasons_json", "[]")
        except Exception:
            rj = "[]"

        try:
            parsed = json.loads(rj) if isinstance(rj, str) else []
            reasons = parsed if isinstance(parsed, list) else [str(parsed)]
        except Exception:
            reasons = [f"Failed to parse reasons_json: {rj!r}"]

        return {
            "id": getattr(data, "id"),
            "deal_id": getattr(data, "deal_id"),
            "decision": getattr(data, "decision"),
            "score": getattr(data, "score"),
            "reasons": reasons,
            "gross_rent_used": getattr(data, "gross_rent_used"),
            "mortgage_payment": getattr(data, "mortgage_payment"),
            "operating_expenses": getattr(data, "operating_expenses"),
            "noi": getattr(data, "noi"),
            "cash_flow": getattr(data, "cash_flow"),
            "dscr": getattr(data, "dscr"),
            "cash_on_cash": getattr(data, "cash_on_cash"),
            "break_even_rent": getattr(data, "break_even_rent"),
            "min_rent_for_target_roi": getattr(data, "min_rent_for_target_roi"),
        }


# -------------------- Compliance --------------------

class InspectorUpsert(BaseModel):
    name: str
    agency: Optional[str] = None


class InspectorOut(InspectorUpsert):
    id: int
    model_config = ConfigDict(from_attributes=True)


class InspectionCreate(BaseModel):
    property_id: int
    inspector_id: Optional[int] = None
    inspection_date: Optional[datetime] = None
    passed: bool = False
    reinspect_required: bool = False
    notes: Optional[str] = None

# -------------------- NEW: Compliance Checklists --------------------

class ChecklistItemOut(BaseModel):
    """
    A single checklist item (rule / requirement / inspection item).
    This is an output schema only, used by /compliance routes.
    """
    code: str
    title: str
    description: Optional[str] = None

    # status flags (UI-friendly)
    required: bool = True
    passed: bool = False
    failed: bool = False
    needs_review: bool = False

    # optional evidence / notes
    notes: Optional[str] = None
    evidence: Optional[dict] = None  # room for debug payloads, source citations, etc.

    model_config = ConfigDict(from_attributes=True)


class ChecklistOut(BaseModel):
    """
    A checklist for a property (or for a city/jurisdiction),
    returned by /compliance endpoints.
    """
    property_id: int
    city: Optional[str] = None
    state: Optional[str] = None

    checklist_name: str = "section8_compliance"
    generated_at: datetime = Field(default_factory=datetime.utcnow)

    items: List[ChecklistItemOut] = Field(default_factory=list)

    # summary stats for quick UI rendering
    total: int = 0
    passed: int = 0
    failed: int = 0
    needs_review: int = 0

    model_config = ConfigDict(from_attributes=True)

    @model_validator(mode="after")
    def _compute_counts(self):
        # If router didn't precompute totals, compute them here.
        if self.items:
            self.total = len(self.items)
            self.passed = sum(1 for i in self.items if i.passed)
            self.failed = sum(1 for i in self.items if i.failed)
            self.needs_review = sum(1 for i in self.items if i.needs_review)
        return self


class InspectionOut(InspectionCreate):
    id: int
    model_config = ConfigDict(from_attributes=True)


class InspectionItemCreate(BaseModel):
    code: str = Field(..., description="Normalized fail point key e.g. GFCI, HANDRAIL, OUTLET, PAINT, TRIP_HAZARD")
    failed: bool = True
    severity: int = 1
    location: Optional[str] = None
    details: Optional[str] = None


class InspectionItemUpdate(BaseModel):
    """
    Generic patch/update for an inspection item.
    Useful if you support editing severity/location/details later.
    """
    failed: Optional[bool] = None
    severity: Optional[int] = Field(default=None, ge=1, le=5)
    location: Optional[str] = None
    details: Optional[str] = None
    resolution_notes: Optional[str] = None


class InspectionItemResolve(BaseModel):
    """
    This is the missing schema your router is trying to import.
    Use this for a 'resolve' endpoint: mark item resolved with optional notes.
    """
    resolved: bool = Field(True, description="Set true to resolve, false to un-resolve")
    resolution_notes: Optional[str] = None
    resolved_at: Optional[datetime] = None


class InspectionItemOut(InspectionItemCreate):
    id: int
    inspection_id: int

    # these exist in models.py now
    resolved_at: Optional[datetime] = None
    resolution_notes: Optional[str] = None

    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class PredictFailPointsOut(BaseModel):
    city: str
    inspector: Optional[str] = None
    window_inspections: int
    top_fail_points: List[dict]


class ComplianceStatsOut(BaseModel):
    city: str
    inspections: int
    pass_rate: float
    reinspect_rate: float
    top_fail_points: List[dict]


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

class RentExplainOut(BaseModel):
    """
    Human-readable explanation of how rent_used was chosen.

    Intended for /rent/explain style endpoints that tell you:
      - market estimate
      - section8 FMR
      - rent reasonableness comp
      - approved ceiling
      - strategy
      - final rent_used
    """
    property_id: int
    strategy: str

    market_rent_estimate: Optional[float] = None
    section8_fmr: Optional[float] = None
    rent_reasonableness_comp: Optional[float] = None
    approved_rent_ceiling: Optional[float] = None
    rent_used: Optional[float] = None

    # explanation text so UI can show it
    explanation: str = ""

    # optional: show how ceiling was computed
    ceiling_candidates: List[dict] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


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

# Important: rebuild forward refs so PropertyOut can include RentAssumptionOut/RentCompOut
PropertyOut.model_rebuild()
