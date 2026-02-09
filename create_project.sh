#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="onehaven_decision_engine"

mkdir -p "$PROJECT_ROOT"/backend/app/{domain,routers,alembic/versions}
mkdir -p "$PROJECT_ROOT"/backend/app/alembic

# ---------------- root files ----------------
cat > "$PROJECT_ROOT/docker-compose.yml" <<'YAML'
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: app
      POSTGRES_PASSWORD: app
      POSTGRES_DB: decision_engine
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U app -d decision_engine"]
      interval: 5s
      timeout: 3s
      retries: 20

  backend:
    build: ./backend
    environment:
      DATABASE_URL: postgresql+psycopg://app:app@postgres:5432/decision_engine
      APP_ENV: local
    ports:
      - "8000:8000"
    depends_on:
      postgres:
        condition: service_healthy

volumes:
  pgdata:
YAML

# ---------------- backend files ----------------
cat > "$PROJECT_ROOT/backend/Dockerfile" <<'DOCKER'
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 8000

CMD ["bash", "-lc", "alembic -c app/alembic.ini upgrade head && uvicorn app.main:app --host 0.0.0.0 --port 8000"]
DOCKER

cat > "$PROJECT_ROOT/backend/requirements.txt" <<'REQ'
fastapi==0.115.6
uvicorn[standard]==0.32.1
SQLAlchemy==2.0.36
psycopg[binary]==3.2.3
pydantic==2.10.3
pydantic-settings==2.6.1
alembic==1.14.0
python-dateutil==2.9.0.post0
REQ

cat > "$PROJECT_ROOT/backend/app/__init__.py" <<'PY'
__all__ = []
PY

cat > "$PROJECT_ROOT/backend/app/config.py" <<'PY'
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    app_env: str = "local"
    database_url: str

    # Deal rules defaults
    max_price: int = 150_000
    min_bedrooms: int = 2
    min_inventory: int = 80
    rent_rule_min_pct: float = 0.013  # 1.3%
    rent_rule_target_pct: float = 0.015  # 1.5%

    # Underwriting defaults
    vacancy_rate: float = 0.05
    maintenance_rate: float = 0.10
    management_rate: float = 0.08
    capex_rate: float = 0.05
    insurance_monthly: float = 150.0
    taxes_monthly: float = 300.0
    utilities_monthly: float = 0.0

    target_monthly_cashflow: float = 400.0
    target_roi: float = 0.15

    dscr_min: float = 1.20
    dscr_penalty_enabled: bool = True


settings = Settings()
PY

cat > "$PROJECT_ROOT/backend/app/db.py" <<'PY'
from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase

from .config import settings


class Base(DeclarativeBase):
    pass


engine = create_engine(settings.database_url, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
PY

cat > "$PROJECT_ROOT/backend/app/models.py" <<'PY'
from __future__ import annotations

from datetime import datetime
from typing import Optional, List

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .db import Base


class Property(Base):
    __tablename__ = "properties"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    address: Mapped[str] = mapped_column(String(255), nullable=False)
    city: Mapped[str] = mapped_column(String(120), nullable=False)
    state: Mapped[str] = mapped_column(String(2), nullable=False, default="MI")
    zip: Mapped[str] = mapped_column(String(10), nullable=False)

    bedrooms: Mapped[int] = mapped_column(Integer, nullable=False)
    bathrooms: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    square_feet: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    year_built: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    has_garage: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    property_type: Mapped[str] = mapped_column(String(60), nullable=False, default="single_family")

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    deals: Mapped[List["Deal"]] = relationship(back_populates="property", cascade="all, delete-orphan")
    rent_assumption: Mapped[Optional["RentAssumption"]] = relationship(
        back_populates="property", uselist=False, cascade="all, delete-orphan"
    )


class Deal(Base):
    __tablename__ = "deals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id", ondelete="CASCADE"), nullable=False)

    source: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    asking_price: Mapped[float] = mapped_column(Float, nullable=False)
    estimated_purchase_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    rehab_estimate: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    financing_type: Mapped[str] = mapped_column(String(40), nullable=False, default="dscr")
    interest_rate: Mapped[float] = mapped_column(Float, nullable=False, default=0.07)
    term_years: Mapped[int] = mapped_column(Integer, nullable=False, default=30)
    down_payment_pct: Mapped[float] = mapped_column(Float, nullable=False, default=0.20)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    property: Mapped["Property"] = relationship(back_populates="deals")
    results: Mapped[List["UnderwritingResult"]] = relationship(back_populates="deal", cascade="all, delete-orphan")


class RentAssumption(Base):
    __tablename__ = "rent_assumptions"
    __table_args__ = (UniqueConstraint("property_id", name="uq_rent_assumptions_property"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id", ondelete="CASCADE"), nullable=False)

    market_rent_estimate: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    section8_fmr: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    approved_rent_ceiling: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    rent_reasonableness_comp: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    inventory_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    starbucks_minutes: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    property: Mapped["Property"] = relationship(back_populates="rent_assumption")


class JurisdictionRule(Base):
    __tablename__ = "jurisdiction_rules"
    __table_args__ = (UniqueConstraint("city", "state", name="uq_jurisdiction_city_state"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    city: Mapped[str] = mapped_column(String(120), nullable=False)
    state: Mapped[str] = mapped_column(String(2), nullable=False, default="MI")

    rental_license_required: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    inspection_authority: Mapped[Optional[str]] = mapped_column(String(180), nullable=True)
    typical_fail_points_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    registration_fee: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    processing_days: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    tenant_waitlist_depth: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


class UnderwritingResult(Base):
    __tablename__ = "underwriting_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    deal_id: Mapped[int] = mapped_column(ForeignKey("deals.id", ondelete="CASCADE"), nullable=False)

    decision: Mapped[str] = mapped_column(String(12), nullable=False)
    score: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reasons_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")

    gross_rent_used: Mapped[float] = mapped_column(Float, nullable=False)
    mortgage_payment: Mapped[float] = mapped_column(Float, nullable=False)
    operating_expenses: Mapped[float] = mapped_column(Float, nullable=False)
    noi: Mapped[float] = mapped_column(Float, nullable=False)
    cash_flow: Mapped[float] = mapped_column(Float, nullable=False)
    dscr: Mapped[float] = mapped_column(Float, nullable=False)
    cash_on_cash: Mapped[float] = mapped_column(Float, nullable=False)

    break_even_rent: Mapped[float] = mapped_column(Float, nullable=False)
    min_rent_for_target_roi: Mapped[float] = mapped_column(Float, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    deal: Mapped["Deal"] = relationship(back_populates="results")
PY

cat > "$PROJECT_ROOT/backend/app/schemas.py" <<'PY'
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
PY

# ---------------- domain ----------------
cat > "$PROJECT_ROOT/backend/app/domain/underwriting.py" <<'PY'
from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class UnderwritingInputs:
    purchase_price: float
    rehab: float
    down_payment_pct: float
    interest_rate: float
    term_years: int

    gross_rent: float

    vacancy_rate: float
    maintenance_rate: float
    management_rate: float
    capex_rate: float

    insurance_monthly: float
    taxes_monthly: float
    utilities_monthly: float


@dataclass(frozen=True)
class UnderwritingOutputs:
    mortgage_payment: float
    operating_expenses: float
    noi: float
    cash_flow: float
    dscr: float
    cash_on_cash: float
    break_even_rent: float
    min_rent_for_target_roi: float


def _monthly_mortgage_payment(principal: float, annual_rate: float, term_years: int) -> float:
    if principal <= 0:
        return 0.0
    r = annual_rate / 12.0
    n = term_years * 12
    if r <= 0:
        return principal / n
    return principal * (r * (1 + r) ** n) / ((1 + r) ** n - 1)


def run_underwriting(inp: UnderwritingInputs, target_roi: float) -> UnderwritingOutputs:
    all_in_cost = inp.purchase_price + inp.rehab
    down_payment = all_in_cost * inp.down_payment_pct
    loan_amount = max(all_in_cost - down_payment, 0.0)

    mortgage_payment = _monthly_mortgage_payment(loan_amount, inp.interest_rate, inp.term_years)

    effective_gross = inp.gross_rent * (1.0 - inp.vacancy_rate)

    var_opex = (
        inp.gross_rent * inp.maintenance_rate
        + inp.gross_rent * inp.management_rate
        + inp.gross_rent * inp.capex_rate
    )
    fixed_opex = inp.insurance_monthly + inp.taxes_monthly + inp.utilities_monthly
    operating_expenses = var_opex + fixed_opex

    noi = effective_gross - operating_expenses
    cash_flow = noi - mortgage_payment

    dscr = (noi / mortgage_payment) if mortgage_payment > 1e-9 else float("inf")

    cash_invested = down_payment
    annual_cash_flow = cash_flow * 12.0
    cash_on_cash = (annual_cash_flow / cash_invested) if cash_invested > 1e-9 else float("inf")

    a = (1.0 - inp.vacancy_rate) - (inp.maintenance_rate + inp.management_rate + inp.capex_rate)
    b = fixed_opex + mortgage_payment
    break_even_rent = (b / a) if a > 1e-9 else float("inf")

    required_annual_cash_flow = target_roi * cash_invested
    required_monthly_cash_flow = required_annual_cash_flow / 12.0
    min_rent_for_target_roi = ((fixed_opex + mortgage_payment + required_monthly_cash_flow) / a) if a > 1e-9 else float("inf")

    return UnderwritingOutputs(
        mortgage_payment=round(mortgage_payment, 2),
        operating_expenses=round(operating_expenses, 2),
        noi=round(noi, 2),
        cash_flow=round(cash_flow, 2),
        dscr=round(dscr, 3) if math.isfinite(dscr) else dscr,
        cash_on_cash=round(cash_on_cash, 3) if math.isfinite(cash_on_cash) else cash_on_cash,
        break_even_rent=round(break_even_rent, 2) if math.isfinite(break_even_rent) else break_even_rent,
        min_rent_for_target_roi=round(min_rent_for_target_roi, 2) if math.isfinite(min_rent_for_target_roi) else min_rent_for_target_roi,
    )
PY

cat > "$PROJECT_ROOT/backend/app/domain/decision_engine.py" <<'PY'
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Optional, List

from ..config import settings


@dataclass(frozen=True)
class DealContext:
    asking_price: float
    bedrooms: int
    has_garage: bool

    rent_market: Optional[float]
    rent_ceiling: Optional[float]
    inventory_count: Optional[int]
    starbucks_minutes: Optional[int]


@dataclass(frozen=True)
class Decision:
    decision: str
    score: int
    reasons: List[str]


def _rent_used(rent_market: Optional[float], rent_ceiling: Optional[float]) -> Optional[float]:
    if rent_market is None and rent_ceiling is None:
        return None
    if rent_market is None:
        return rent_ceiling
    if rent_ceiling is None:
        return rent_market
    return min(rent_market, rent_ceiling)


def evaluate_deal_rules(ctx: DealContext) -> Decision:
    reasons: list[str] = []
    score = 50

    if ctx.asking_price > settings.max_price:
        return Decision("REJECT", 0, [f"Price {ctx.asking_price:.0f} exceeds max ${settings.max_price}"])

    if ctx.bedrooms < settings.min_bedrooms:
        return Decision("REJECT", 0, [f"Bedrooms {ctx.bedrooms} below minimum {settings.min_bedrooms}"])

    if ctx.has_garage:
        reasons.append("Garage present (rehab/maintenance risk flag)")
        score -= 5

    rent = _rent_used(ctx.rent_market, ctx.rent_ceiling)
    if rent is None:
        reasons.append("Missing rent inputs (need market rent and/or FMR/ceiling)")
        score -= 20
    else:
        min_rent = ctx.asking_price * settings.rent_rule_min_pct
        target_rent = ctx.asking_price * settings.rent_rule_target_pct

        if rent < min_rent:
            return Decision("REJECT", 0, [f"Fails 1.3% rule: rent {rent:.0f} < {min_rent:.0f}"])

        if rent >= target_rent:
            score += 15
            reasons.append("Meets 1.5% target rent rule")
        else:
            score += 5
            reasons.append("Meets 1.3% minimum rent rule")

    if ctx.rent_ceiling is not None and ctx.rent_market is not None and ctx.rent_market > ctx.rent_ceiling:
        reasons.append("Market rent exceeds Section 8 ceiling (rent will be capped)")
        score -= 5

    if ctx.inventory_count is None:
        reasons.append("Missing inventory count proxy")
        score -= 5
    else:
        if ctx.inventory_count < settings.min_inventory:
            reasons.append(f"Inventory proxy low ({ctx.inventory_count} < {settings.min_inventory})")
            score -= 15
        else:
            reasons.append("Inventory proxy healthy")
            score += 10

    if ctx.starbucks_minutes is not None:
        if ctx.starbucks_minutes <= 10:
            reasons.append("Starbucks proxy good (<= 10 minutes)")
            score += 10
        else:
            reasons.append("Starbucks proxy weak (> 10 minutes)")
            score -= 5

    score = max(0, min(100, score))

    if score >= 75:
        decision = "PASS"
    elif score >= 55:
        decision = "REVIEW"
    else:
        decision = "REJECT"

    return Decision(decision, score, reasons)


def reasons_to_json(reasons: list[str]) -> str:
    return json.dumps(reasons, ensure_ascii=False)


def reasons_from_json(s: str) -> list[str]:
    try:
        v = json.loads(s)
        if isinstance(v, list):
            return [str(x) for x in v]
    except Exception:
        pass
    return []
PY

# ---------------- routers ----------------
cat > "$PROJECT_ROOT/backend/app/routers/health.py" <<'PY'
from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/health")
def health():
    return {"ok": True}
PY

cat > "$PROJECT_ROOT/backend/app/routers/properties.py" <<'PY'
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import Property
from ..schemas import PropertyCreate, PropertyOut

router = APIRouter(prefix="/properties", tags=["properties"])


@router.post("", response_model=PropertyOut)
def create_property(payload: PropertyCreate, db: Session = Depends(get_db)):
    p = Property(**payload.model_dump())
    db.add(p)
    db.commit()
    db.refresh(p)
    return p


@router.get("/{property_id}", response_model=PropertyOut)
def get_property(property_id: int, db: Session = Depends(get_db)):
    p = db.get(Property, property_id)
    if not p:
        raise HTTPException(status_code=404, detail="Property not found")
    return p
PY

cat > "$PROJECT_ROOT/backend/app/routers/deals.py" <<'PY'
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select

from ..db import get_db
from ..models import Deal, Property, RentAssumption
from ..schemas import DealCreate, DealOut, RentAssumptionUpsert, RentAssumptionOut

router = APIRouter(prefix="/deals", tags=["deals"])


@router.post("", response_model=DealOut)
def create_deal(payload: DealCreate, db: Session = Depends(get_db)):
    prop = db.get(Property, payload.property_id)
    if not prop:
        raise HTTPException(status_code=400, detail="Invalid property_id")

    d = Deal(**payload.model_dump())
    db.add(d)
    db.commit()
    db.refresh(d)
    return d


@router.get("/{deal_id}", response_model=DealOut)
def get_deal(deal_id: int, db: Session = Depends(get_db)):
    d = db.get(Deal, deal_id)
    if not d:
        raise HTTPException(status_code=404, detail="Deal not found")
    return d


@router.put("/property/{property_id}/rent", response_model=RentAssumptionOut)
def upsert_rent(property_id: int, payload: RentAssumptionUpsert, db: Session = Depends(get_db)):
    prop = db.get(Property, property_id)
    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")

    ra = db.scalar(select(RentAssumption).where(RentAssumption.property_id == property_id))
    data = payload.model_dump(exclude_unset=True)

    if ra is None:
        ra = RentAssumption(property_id=property_id, **data)
        db.add(ra)
    else:
        for k, v in data.items():
            setattr(ra, k, v)

    db.commit()
    db.refresh(ra)
    return ra


@router.get("/property/{property_id}/rent", response_model=RentAssumptionOut)
def get_rent(property_id: int, db: Session = Depends(get_db)):
    ra = db.scalar(select(RentAssumption).where(RentAssumption.property_id == property_id))
    if not ra:
        raise HTTPException(status_code=404, detail="Rent assumptions not found")
    return ra
PY

cat > "$PROJECT_ROOT/backend/app/routers/jurisdictions.py" <<'PY'
from __future__ import annotations

import json

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select

from ..db import get_db
from ..models import JurisdictionRule
from ..schemas import JurisdictionRuleUpsert, JurisdictionRuleOut

router = APIRouter(prefix="/jurisdictions", tags=["jurisdictions"])


@router.put("", response_model=JurisdictionRuleOut)
def upsert_jurisdiction(payload: JurisdictionRuleUpsert, db: Session = Depends(get_db)):
    jr = db.scalar(
        select(JurisdictionRule).where(
            JurisdictionRule.city == payload.city,
            JurisdictionRule.state == payload.state,
        )
    )

    typical = payload.typical_fail_points
    data = payload.model_dump(exclude={"typical_fail_points"})
    if typical is not None:
        data["typical_fail_points_json"] = json.dumps(typical, ensure_ascii=False)

    if jr is None:
        jr = JurisdictionRule(**data)
        db.add(jr)
    else:
        for k, v in data.items():
            setattr(jr, k, v)

    db.commit()
    db.refresh(jr)

    out = JurisdictionRuleOut(
        id=jr.id,
        city=jr.city,
        state=jr.state,
        rental_license_required=jr.rental_license_required,
        inspection_authority=jr.inspection_authority,
        typical_fail_points=json.loads(jr.typical_fail_points_json) if jr.typical_fail_points_json else None,
        registration_fee=jr.registration_fee,
        processing_days=jr.processing_days,
        tenant_waitlist_depth=jr.tenant_waitlist_depth,
    )
    return out


@router.get("/{state}/{city}", response_model=JurisdictionRuleOut)
def get_jurisdiction(state: str, city: str, db: Session = Depends(get_db)):
    jr = db.scalar(select(JurisdictionRule).where(JurisdictionRule.city == city, JurisdictionRule.state == state))
    if not jr:
        raise HTTPException(status_code=404, detail="Jurisdiction rule not found")

    return JurisdictionRuleOut(
        id=jr.id,
        city=jr.city,
        state=jr.state,
        rental_license_required=jr.rental_license_required,
        inspection_authority=jr.inspection_authority,
        typical_fail_points=json.loads(jr.typical_fail_points_json) if jr.typical_fail_points_json else None,
        registration_fee=jr.registration_fee,
        processing_days=jr.processing_days,
        tenant_waitlist_depth=jr.tenant_waitlist_depth,
    )
PY

cat > "$PROJECT_ROOT/backend/app/routers/evaluate.py" <<'PY'
from __future__ import annotations

import json

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select

from ..db import get_db
from ..models import Deal, Property, RentAssumption, UnderwritingResult, JurisdictionRule
from ..schemas import UnderwritingResultOut
from ..config import settings
from ..domain.decision_engine import DealContext, evaluate_deal_rules, reasons_from_json
from ..domain.underwriting import UnderwritingInputs, run_underwriting

router = APIRouter(prefix="/evaluate", tags=["evaluate"])


@router.post("/deal/{deal_id}", response_model=UnderwritingResultOut)
def evaluate_deal(deal_id: int, db: Session = Depends(get_db)):
    deal = db.get(Deal, deal_id)
    if not deal:
        raise HTTPException(status_code=404, detail="Deal not found")

    prop = db.get(Property, deal.property_id)
    if not prop:
        raise HTTPException(status_code=500, detail="Deal has missing property")

    ra = db.scalar(select(RentAssumption).where(RentAssumption.property_id == prop.id))

    rent_market = ra.market_rent_estimate if ra else None
    rent_ceiling = None
    if ra:
        rent_ceiling = ra.approved_rent_ceiling if ra.approved_rent_ceiling is not None else ra.section8_fmr

    ctx = DealContext(
        asking_price=deal.asking_price,
        bedrooms=prop.bedrooms,
        has_garage=prop.has_garage,
        rent_market=rent_market,
        rent_ceiling=rent_ceiling,
        inventory_count=ra.inventory_count if ra else None,
        starbucks_minutes=ra.starbucks_minutes if ra else None,
    )
    d = evaluate_deal_rules(ctx)

    reasons = list(d.reasons)

    if rent_market is None and rent_ceiling is None:
        reasons.append("Missing rent data -> cannot underwrite")
        final_decision = "REJECT"
        final_score = 0

        result = UnderwritingResult(
            deal_id=deal.id,
            decision=final_decision,
            score=int(final_score),
            reasons_json=json.dumps(reasons, ensure_ascii=False),
            gross_rent_used=0.0,
            mortgage_payment=0.0,
            operating_expenses=0.0,
            noi=0.0,
            cash_flow=0.0,
            dscr=0.0,
            cash_on_cash=0.0,
            break_even_rent=0.0,
            min_rent_for_target_roi=0.0,
        )
        db.add(result)
        db.commit()
        db.refresh(result)

        return UnderwritingResultOut(
            id=result.id,
            deal_id=result.deal_id,
            decision=result.decision,
            score=result.score,
            reasons=reasons_from_json(result.reasons_json),
            gross_rent_used=result.gross_rent_used,
            mortgage_payment=result.mortgage_payment,
            operating_expenses=result.operating_expenses,
            noi=result.noi,
            cash_flow=result.cash_flow,
            dscr=result.dscr,
            cash_on_cash=result.cash_on_cash,
            break_even_rent=result.break_even_rent,
            min_rent_for_target_roi=result.min_rent_for_target_roi,
        )

    if rent_market is None:
        gross_rent_used = float(rent_ceiling)
    elif rent_ceiling is None:
        gross_rent_used = float(rent_market)
    else:
        gross_rent_used = float(min(rent_market, rent_ceiling))

    purchase = deal.estimated_purchase_price if deal.estimated_purchase_price is not None else deal.asking_price

    uw_in = UnderwritingInputs(
        purchase_price=float(purchase),
        rehab=float(deal.rehab_estimate),
        down_payment_pct=float(deal.down_payment_pct),
        interest_rate=float(deal.interest_rate),
        term_years=int(deal.term_years),
        gross_rent=float(gross_rent_used),
        vacancy_rate=float(settings.vacancy_rate),
        maintenance_rate=float(settings.maintenance_rate),
        management_rate=float(settings.management_rate),
        capex_rate=float(settings.capex_rate),
        insurance_monthly=float(settings.insurance_monthly),
        taxes_monthly=float(settings.taxes_monthly),
        utilities_monthly=float(settings.utilities_monthly),
    )
    uw_out = run_underwriting(uw_in, target_roi=settings.target_roi)

    final_decision = d.decision
    final_score = d.score

    if uw_out.dscr < settings.dscr_min:
        reasons.append(f"DSCR {uw_out.dscr:.3f} below minimum {settings.dscr_min:.2f}")
        final_decision = "REJECT"
        final_score = min(final_score, 45)
    else:
        if uw_out.cash_flow < settings.target_monthly_cashflow:
            reasons.append(f"Cash flow ${uw_out.cash_flow:.2f} below target ${settings.target_monthly_cashflow:.0f}")
            final_decision = "REVIEW" if final_decision != "REJECT" else "REJECT"
            final_score = min(final_score, 65)

    jr = db.scalar(select(JurisdictionRule).where(JurisdictionRule.city == prop.city, JurisdictionRule.state == prop.state))
    if jr and jr.processing_days is not None and jr.processing_days >= 45:
        reasons.append(f"Jurisdiction processing delay risk ({jr.processing_days} days)")
        if final_decision == "PASS":
            final_decision = "REVIEW"
            final_score = min(final_score, 70)

    result = UnderwritingResult(
        deal_id=deal.id,
        decision=final_decision,
        score=int(final_score),
        reasons_json=json.dumps(reasons, ensure_ascii=False),
        gross_rent_used=float(gross_rent_used),
        mortgage_payment=float(uw_out.mortgage_payment),
        operating_expenses=float(uw_out.operating_expenses),
        noi=float(uw_out.noi),
        cash_flow=float(uw_out.cash_flow),
        dscr=float(uw_out.dscr),
        cash_on_cash=float(uw_out.cash_on_cash),
        break_even_rent=float(uw_out.break_even_rent),
        min_rent_for_target_roi=float(uw_out.min_rent_for_target_roi),
    )
    db.add(result)
    db.commit()
    db.refresh(result)

    return UnderwritingResultOut(
        id=result.id,
        deal_id=result.deal_id,
        decision=result.decision,
        score=result.score,
        reasons=reasons_from_json(result.reasons_json),
        gross_rent_used=result.gross_rent_used,
        mortgage_payment=result.mortgage_payment,
        operating_expenses=result.operating_expenses,
        noi=result.noi,
        cash_flow=result.cash_flow,
        dscr=result.dscr,
        cash_on_cash=result.cash_on_cash,
        break_even_rent=result.break_even_rent,
        min_rent_for_target_roi=result.min_rent_for_target_roi,
    )
PY

cat > "$PROJECT_ROOT/backend/app/main.py" <<'PY'
from __future__ import annotations

from fastapi import FastAPI

from .routers.health import router as health_router
from .routers.properties import router as properties_router
from .routers.deals import router as deals_router
from .routers.jurisdictions import router as jurisdictions_router
from .routers.evaluate import router as evaluate_router

app = FastAPI(
    title="Decision Engine for Regulated Residential Cash-Flow Assets",
    version="0.1.0",
)

app.include_router(health_router)
app.include_router(properties_router)
app.include_router(deals_router)
app.include_router(jurisdictions_router)
app.include_router(evaluate_router)
PY

# ---------------- alembic ----------------
cat > "$PROJECT_ROOT/backend/app/alembic.ini" <<'INI'
[alembic]
script_location = app/alembic
sqlalchemy.url = %(DATABASE_URL)s

[loggers]
keys = root,sqlalchemy,alembic

[handlers]
keys = console

[formatters]
keys = generic

[logger_root]
level = INFO
handlers = console

[logger_sqlalchemy]
level = WARN
handlers =
qualname = sqlalchemy.engine

[logger_alembic]
level = INFO
handlers =
qualname = alembic

[handler_console]
class = StreamHandler
args = (sys.stdout,)
level = NOTSET
formatter = generic

[formatter_generic]
format = %(levelname)-5.5s [%(name)s] %(message)s
INI

cat > "$PROJECT_ROOT/backend/app/alembic/env.py" <<'PY'
from __future__ import annotations

import os
from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool
from alembic import context

from app.db import Base
from app import models  # noqa: F401

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def get_url() -> str:
    return os.environ["DATABASE_URL"]


def run_migrations_offline() -> None:
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    configuration = config.get_section(config.config_ini_section) or {}
    configuration["sqlalchemy.url"] = get_url()

    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
PY

cat > "$PROJECT_ROOT/backend/app/alembic/script.py.mako" <<'MAKO'
"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}

"""
from alembic import op
import sqlalchemy as sa


revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}
branch_labels = ${repr(branch_labels)}
depends_on = ${repr(depends_on)}


def upgrade():
    ${upgrades if upgrades else "pass"}


def downgrade():
    ${downgrades if downgrades else "pass"}
MAKO

cat > "$PROJECT_ROOT/backend/app/alembic/versions/0001_init.py" <<'PY'
"""init schema

Revision ID: 0001_init
Revises:
Create Date: 2026-02-09
"""
from alembic import op
import sqlalchemy as sa


revision = "0001_init"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "properties",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("address", sa.String(length=255), nullable=False),
        sa.Column("city", sa.String(length=120), nullable=False),
        sa.Column("state", sa.String(length=2), nullable=False),
        sa.Column("zip", sa.String(length=10), nullable=False),
        sa.Column("bedrooms", sa.Integer(), nullable=False),
        sa.Column("bathrooms", sa.Float(), nullable=False),
        sa.Column("square_feet", sa.Integer(), nullable=True),
        sa.Column("year_built", sa.Integer(), nullable=True),
        sa.Column("has_garage", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("property_type", sa.String(length=60), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )

    op.create_table(
        "deals",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("property_id", sa.Integer(), sa.ForeignKey("properties.id", ondelete="CASCADE"), nullable=False),
        sa.Column("source", sa.String(length=80), nullable=True),
        sa.Column("asking_price", sa.Float(), nullable=False),
        sa.Column("estimated_purchase_price", sa.Float(), nullable=True),
        sa.Column("rehab_estimate", sa.Float(), nullable=False, server_default="0"),
        sa.Column("financing_type", sa.String(length=40), nullable=False),
        sa.Column("interest_rate", sa.Float(), nullable=False),
        sa.Column("term_years", sa.Integer(), nullable=False),
        sa.Column("down_payment_pct", sa.Float(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )

    op.create_table(
        "rent_assumptions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("property_id", sa.Integer(), sa.ForeignKey("properties.id", ondelete="CASCADE"), nullable=False),
        sa.Column("market_rent_estimate", sa.Float(), nullable=True),
        sa.Column("section8_fmr", sa.Float(), nullable=True),
        sa.Column("approved_rent_ceiling", sa.Float(), nullable=True),
        sa.Column("rent_reasonableness_comp", sa.Float(), nullable=True),
        sa.Column("inventory_count", sa.Integer(), nullable=True),
        sa.Column("starbucks_minutes", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("property_id", name="uq_rent_assumptions_property"),
    )

    op.create_table(
        "jurisdiction_rules",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("city", sa.String(length=120), nullable=False),
        sa.Column("state", sa.String(length=2), nullable=False),
        sa.Column("rental_license_required", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("inspection_authority", sa.String(length=180), nullable=True),
        sa.Column("typical_fail_points_json", sa.Text(), nullable=True),
        sa.Column("registration_fee", sa.Float(), nullable=True),
        sa.Column("processing_days", sa.Integer(), nullable=True),
        sa.Column("tenant_waitlist_depth", sa.String(length=80), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("city", "state", name="uq_jurisdiction_city_state"),
    )

    op.create_table(
        "underwriting_results",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("deal_id", sa.Integer(), sa.ForeignKey("deals.id", ondelete="CASCADE"), nullable=False),
        sa.Column("decision", sa.String(length=12), nullable=False),
        sa.Column("score", sa.Integer(), nullable=False),
        sa.Column("reasons_json", sa.Text(), nullable=False),
        sa.Column("gross_rent_used", sa.Float(), nullable=False),
        sa.Column("mortgage_payment", sa.Float(), nullable=False),
        sa.Column("operating_expenses", sa.Float(), nullable=False),
        sa.Column("noi", sa.Float(), nullable=False),
        sa.Column("cash_flow", sa.Float(), nullable=False),
        sa.Column("dscr", sa.Float(), nullable=False),
        sa.Column("cash_on_cash", sa.Float(), nullable=False),
        sa.Column("break_even_rent", sa.Float(), nullable=False),
        sa.Column("min_rent_for_target_roi", sa.Float(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )


def downgrade():
    op.drop_table("underwriting_results")
    op.drop_table("jurisdiction_rules")
    op.drop_table("rent_assumptions")
    op.drop_table("deals")
    op.drop_table("properties")
PY

echo "✅ Created project at: $PROJECT_ROOT"
echo "Next:"
echo "  cd $PROJECT_ROOT"
echo "  docker compose up --build"
echo "Then open: http://localhost:8000/docs"
