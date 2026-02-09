from __future__ import annotations

from typing import Optional, List
from pydantic import BaseModel


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
