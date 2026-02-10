from __future__ import annotations

from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime


class ImportSnapshotOut(BaseModel):
    id: int
    source: str
    notes: Optional[str] = None
    class Config:
        from_attributes = True


class ImportErrorRow(BaseModel):
    row: int
    error: str


class ImportResultOut(BaseModel):
    snapshot_id: int
    source: str
    imported: int
    skipped_duplicates: int
    errors: list[ImportErrorRow]


class BatchEvalOut(BaseModel):
    snapshot_id: int
    total_deals: int
    pass_count: int
    review_count: int
    reject_count: int


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

    class Config:
        from_attributes = True


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
    class Config:
        from_attributes = True


class DealCreate(BaseModel):
    property_id: int
    source: Optional[str] = None
    asking_price: float
    estimated_purchase_price: Optional[float] = None
    rehab_estimate: float = 0.0
    financing_type: str = "dscr"
    interest_rate: float = 0.07
    term_years: int = 30
    down_payment_pct: float = 0.20


class DealOut(DealCreate):
    id: int
    class Config:
        from_attributes = True


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
    class Config:
        from_attributes = True


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
    class Config:
        from_attributes = True


class UnderwritingResultOut(BaseModel):
    id: int
    deal_id: int
    decision: str
    score: int
    reasons: List[str]

    gross_rent_used: float
    mortgage_payment: float
    operating_expenses: float
    noi: float
    cash_flow: float
    dscr: float
    cash_on_cash: float

    break_even_rent: float
    min_rent_for_target_roi: float

    class Config:
        from_attributes = True


# -------------------- NEW: Compliance --------------------

class InspectorUpsert(BaseModel):
    name: str
    agency: Optional[str] = None


class InspectorOut(InspectorUpsert):
    id: int
    class Config:
        from_attributes = True


class InspectionCreate(BaseModel):
    property_id: int
    inspector_id: Optional[int] = None
    inspection_date: Optional[datetime] = None
    passed: bool = False
    reinspect_required: bool = False
    notes: Optional[str] = None


class InspectionOut(InspectionCreate):
    id: int
    class Config:
        from_attributes = True


class InspectionItemCreate(BaseModel):
    code: str = Field(..., description="Normalized fail point key e.g. GFCI, HANDRAIL, OUTLET, PAINT, TRIP_HAZARD")
    failed: bool = True
    severity: int = 1
    location: Optional[str] = None
    details: Optional[str] = None


class InspectionItemOut(InspectionItemCreate):
    id: int
    inspection_id: int
    class Config:
        from_attributes = True


class PredictFailPointsOut(BaseModel):
    city: str
    inspector: Optional[str] = None
    window_inspections: int
    top_fail_points: List[dict]  # [{"code":"GFCI","count":7,"rate":0.35}, ...]


class ComplianceStatsOut(BaseModel):
    city: str
    inspections: int
    pass_rate: float
    reinspect_rate: float
    top_fail_points: List[dict]

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

    class Config:
        from_attributes = True


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

    class Config:
        from_attributes = True


class RentCalibrationOut(BaseModel):
    zip: str
    bedrooms: int
    strategy: str
    multiplier: float
    samples: int
    mape: Optional[float] = None
    updated_at: datetime

    class Config:
        from_attributes = True


class RentRecomputeOut(BaseModel):
    property_id: int
    market_rent_estimate: Optional[float]
    section8_fmr: Optional[float]
    rent_reasonableness_comp: Optional[float]
    approved_rent_ceiling: Optional[float]
    calibrated_market_rent: Optional[float]
    strategy: str
    rent_used: Optional[float]