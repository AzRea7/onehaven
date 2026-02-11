# backend/app/schemas.py
from __future__ import annotations

import json
from datetime import datetime
from typing import Optional, List, Any, Literal, Dict

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
    rent_assumption: Optional["RentAssumptionOut"] = None
    rent_comps: List["RentCompOut"] = Field(default_factory=list)
    model_config = ConfigDict(from_attributes=True)


class DealCreate(BaseModel):
    property_id: int
    asking_price: float
    estimated_purchase_price: Optional[float] = None
    rehab_estimate: float = 0.0

    # section8 | market
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
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)


class DealIntakeIn(BaseModel):
    # Property fields
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

    # Deal fields
    # NOTE: your DB model uses Deal.asking_price. Many people map intake.purchase_price -> asking_price internally.
    purchase_price: float
    est_rehab: float = 0.0
    strategy: str = Field(default="section8", description="section8|market")

    financing_type: str = "dscr"
    interest_rate: float = 0.07
    term_years: int = 30
    down_payment_pct: float = 0.20

    # allow snapshot selection (optional)
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
    created_at: Optional[datetime] = None

    rent_used: Optional[float] = None
    model_config = ConfigDict(from_attributes=True)


class JurisdictionRuleUpsert(BaseModel):
    city: str
    state: str = "MI"
    rental_license_required: bool = False

    inspection_frequency: Optional[str] = Field(default=None, description="annual|biennial|complaint")
    inspection_authority: Optional[str] = None

    # Stored in DB as JSON text (typical_fail_points_json) — schema stays list[str] for ergonomics.
    typical_fail_points: Optional[List[str]] = None

    registration_fee: Optional[float] = None
    processing_days: Optional[int] = None
    tenant_waitlist_depth: Optional[str] = None

    jurisdiction_type: Optional[str] = None
    notes: Optional[str] = None


class JurisdictionRuleOut(JurisdictionRuleUpsert):
    id: int
    model_config = ConfigDict(from_attributes=True)


# -------------------- Underwriting Results --------------------

class UnderwritingResultOut(BaseModel):
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

    # Phase 0/2/3 persisted truth
    decision_version: Optional[str] = None
    payment_standard_pct_used: Optional[float] = None
    jurisdiction_multiplier: Optional[float] = None
    jurisdiction_reasons: Optional[List[str]] = None
    rent_cap_reason: Optional[str] = None
    fmr_adjusted: Optional[float] = None

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

        # ORM object
        rj = getattr(data, "reasons_json", "[]")
        jurj = getattr(data, "jurisdiction_reasons_json", None)

        return {
            "id": getattr(data, "id"),
            "deal_id": getattr(data, "deal_id"),
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


# -------------------- Compliance (existing checklist out + new template + persisted checklist) --------------------

class ChecklistItemOut(BaseModel):
    item_code: str
    category: str
    description: str
    severity: int = Field(default=2, ge=1, le=5)
    common_fail: bool = False
    applies_if: Optional[Dict[str, Any]] = None


class ChecklistOut(BaseModel):
    property_id: int
    city: Optional[str] = None
    state: Optional[str] = None

    checklist_name: str = "section8_hqs_precheck"
    generated_at: datetime = Field(default_factory=datetime.utcnow)

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
    property_id: int
    strategy: str
    version: str
    generated_at: datetime
    items: List[ChecklistItemOut]
    model_config = ConfigDict(from_attributes=True)


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
