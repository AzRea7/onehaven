from __future__ import annotations

from datetime import datetime, date
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
    Date,
    func,
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

    inspections: Mapped[List["Inspection"]] = relationship(back_populates="property", cascade="all, delete-orphan")

    rent_comps: Mapped[List["RentComp"]] = relationship(back_populates="property", cascade="all, delete-orphan")
    rent_observations: Mapped[List["RentObservation"]] = relationship(
        back_populates="property", cascade="all, delete-orphan"
    )

    checklists: Mapped[List["PropertyChecklist"]] = relationship(
        back_populates="property", cascade="all, delete-orphan"
    )


class Deal(Base):
    __tablename__ = "deals"
    __table_args__ = (UniqueConstraint("source_fingerprint", name="uq_deals_source_fingerprint"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id", ondelete="CASCADE"), nullable=False)

    snapshot_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("import_snapshots.id", ondelete="SET NULL"), nullable=True
    )
    source_fingerprint: Mapped[Optional[str]] = mapped_column(String(128), nullable=True, index=True)
    source_raw_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    source: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    asking_price: Mapped[float] = mapped_column(Float, nullable=False)
    estimated_purchase_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    rehab_estimate: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    # section8 | market
    strategy: Mapped[str] = mapped_column(String(20), nullable=False, default="section8")

    financing_type: Mapped[str] = mapped_column(String(40), nullable=False, default="dscr")
    interest_rate: Mapped[float] = mapped_column(Float, nullable=False, default=0.07)
    term_years: Mapped[int] = mapped_column(Integer, nullable=False, default=30)
    down_payment_pct: Mapped[float] = mapped_column(Float, nullable=False, default=0.20)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    property: Mapped["Property"] = relationship(back_populates="deals")
    results: Mapped[List["UnderwritingResult"]] = relationship(back_populates="deal", cascade="all, delete-orphan")
    snapshot: Mapped[Optional["ImportSnapshot"]] = relationship(back_populates="deals")


class RentAssumption(Base):
    __tablename__ = "rent_assumptions"
    __table_args__ = (UniqueConstraint("property_id", name="uq_rent_assumptions_property"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id", ondelete="CASCADE"), nullable=False)

    market_rent_estimate: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    section8_fmr: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    approved_rent_ceiling: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    rent_reasonableness_comp: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    rent_used: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    inventory_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    starbucks_minutes: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    property: Mapped["Property"] = relationship(back_populates="rent_assumption")


class RentComp(Base):
    __tablename__ = "rent_comps"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id", ondelete="CASCADE"), nullable=False)

    source: Mapped[str] = mapped_column(String(40), nullable=False, default="manual")
    address: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    rent: Mapped[float] = mapped_column(Float, nullable=False)
    bedrooms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    bathrooms: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    square_feet: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    property: Mapped["Property"] = relationship(back_populates="rent_comps")


class RentObservation(Base):
    __tablename__ = "rent_observations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id", ondelete="CASCADE"), nullable=False)

    strategy: Mapped[str] = mapped_column(String(20), nullable=False)

    achieved_rent: Mapped[float] = mapped_column(Float, nullable=False)
    tenant_portion: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    hap_portion: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    lease_start: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    lease_end: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    property: Mapped["Property"] = relationship(back_populates="rent_observations")


class RentCalibration(Base):
    __tablename__ = "rent_calibrations"
    __table_args__ = (UniqueConstraint("zip", "bedrooms", "strategy", name="uq_rent_calibration_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    zip: Mapped[str] = mapped_column(String(10), nullable=False)
    bedrooms: Mapped[int] = mapped_column(Integer, nullable=False)
    strategy: Mapped[str] = mapped_column(String(20), nullable=False)

    multiplier: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    samples: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    mape: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


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

    # added in revision 0007_add_deal_strategy
    inspection_frequency: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)

    jurisdiction_type: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)

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

    decision_version: Mapped[str] = mapped_column(String(64), nullable=False, default="unknown")
    payment_standard_pct_used: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    jurisdiction_multiplier: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    jurisdiction_reasons_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    rent_cap_reason: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    fmr_adjusted: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    deal: Mapped["Deal"] = relationship(back_populates="results")


# -------------------- Compliance logging (existing) --------------------

class Inspector(Base):
    __tablename__ = "inspectors"
    __table_args__ = (UniqueConstraint("name", "agency", name="uq_inspector_name_agency"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(180), nullable=False)
    agency: Mapped[Optional[str]] = mapped_column(String(180), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    inspections: Mapped[List["Inspection"]] = relationship(back_populates="inspector")


class Inspection(Base):
    __tablename__ = "inspections"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id", ondelete="CASCADE"), nullable=False)
    inspector_id: Mapped[Optional[int]] = mapped_column(ForeignKey("inspectors.id", ondelete="SET NULL"), nullable=True)

    inspection_date: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    passed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    reinspect_required: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    property: Mapped["Property"] = relationship(back_populates="inspections")
    inspector: Mapped[Optional["Inspector"]] = relationship(back_populates="inspections")
    items: Mapped[List["InspectionItem"]] = relationship(back_populates="inspection", cascade="all, delete-orphan")


class InspectionItem(Base):
    __tablename__ = "inspection_items"
    __table_args__ = (UniqueConstraint("inspection_id", "code", name="uq_inspection_item_per_code"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    inspection_id: Mapped[int] = mapped_column(ForeignKey("inspections.id", ondelete="CASCADE"), nullable=False)

    code: Mapped[str] = mapped_column(String(80), nullable=False)

    failed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    severity: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    location: Mapped[Optional[str]] = mapped_column(String(180), nullable=True)
    details: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    resolution_notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    inspection: Mapped["Inspection"] = relationship(back_populates="items")


class ImportSnapshot(Base):
    __tablename__ = "import_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    source: Mapped[str] = mapped_column(String(40), nullable=False)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    deals: Mapped[List["Deal"]] = relationship(back_populates="snapshot")


class ApiUsage(Base):
    __tablename__ = "api_usage"
    __table_args__ = (UniqueConstraint("provider", "day", name="uq_api_usage_provider_day"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    provider: Mapped[str] = mapped_column(String(50), nullable=False)
    day: Mapped[date] = mapped_column(Date, nullable=False)
    calls: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


# -------------------- Phase 3: checklist templates + persisted checklists --------------------

class ChecklistTemplateItem(Base):
    __tablename__ = "checklist_template_items"
    __table_args__ = (UniqueConstraint("strategy", "code", "version", name="uq_checklist_template_key"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    strategy: Mapped[str] = mapped_column(String(20), nullable=False, default="section8")
    version: Mapped[str] = mapped_column(String(32), nullable=False, default="v1")

    code: Mapped[str] = mapped_column(String(80), nullable=False)
    category: Mapped[str] = mapped_column(String(80), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)

    applies_if_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    severity: Mapped[int] = mapped_column(Integer, nullable=False, default=2)
    common_fail: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


class PropertyChecklist(Base):
    __tablename__ = "property_checklists"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id", ondelete="CASCADE"), nullable=False)

    strategy: Mapped[str] = mapped_column(String(20), nullable=False, default="section8")
    version: Mapped[str] = mapped_column(String(32), nullable=False, default="v1")

    generated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    items_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")

    property: Mapped["Property"] = relationship(back_populates="checklists")


class InspectionEvent(Base):
    __tablename__ = "inspection_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    property_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("properties.id", ondelete="CASCADE"), nullable=False
    )

    inspector_name: Mapped[str | None] = mapped_column(String(120), nullable=True)
    inspection_date: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)

    # scheduled | passed | failed | reinspect
    status: Mapped[str] = mapped_column(String(20), nullable=False, server_default="scheduled")

    fail_items_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    resolution_notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    days_to_resolve: Mapped[int | None] = mapped_column(Integer, nullable=True)
    reinspection_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")

    created_at: Mapped[DateTime] = mapped_column(DateTime, nullable=False, server_default=func.now())


class RehabTask(Base):
    __tablename__ = "rehab_tasks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    property_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("properties.id", ondelete="CASCADE"), nullable=False
    )

    title: Mapped[str] = mapped_column(String(200), nullable=False)
    category: Mapped[str] = mapped_column(String(50), nullable=False, server_default="rehab")
    inspection_relevant: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="true")

    # todo | doing | blocked | done
    status: Mapped[str] = mapped_column(String(30), nullable=False, server_default="todo")

    cost_estimate: Mapped[float | None] = mapped_column(Float, nullable=True)
    vendor: Mapped[str | None] = mapped_column(String(120), nullable=True)
    deadline: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[DateTime] = mapped_column(DateTime, nullable=False, server_default=func.now())


class Tenant(Base):
    __tablename__ = "tenants"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    full_name: Mapped[str] = mapped_column(String(200), nullable=False)
    phone: Mapped[str | None] = mapped_column(String(50), nullable=True)
    email: Mapped[str | None] = mapped_column(String(200), nullable=True)
    voucher_status: Mapped[str | None] = mapped_column(String(80), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[DateTime] = mapped_column(DateTime, nullable=False, server_default=func.now())

    leases: Mapped[list["Lease"]] = relationship("Lease", back_populates="tenant")


class Lease(Base):
    __tablename__ = "leases"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    property_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("properties.id", ondelete="CASCADE"), nullable=False
    )
    tenant_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("tenants.id", ondelete="RESTRICT"), nullable=False
    )

    start_date: Mapped[DateTime] = mapped_column(DateTime, nullable=False)
    end_date: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)

    total_rent: Mapped[float] = mapped_column(Float, nullable=False, server_default="0")
    tenant_portion: Mapped[float | None] = mapped_column(Float, nullable=True)
    housing_authority_portion: Mapped[float | None] = mapped_column(Float, nullable=True)

    hap_contract_status: Mapped[str | None] = mapped_column(String(80), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[DateTime] = mapped_column(DateTime, nullable=False, server_default=func.now())

    tenant: Mapped["Tenant"] = relationship("Tenant", back_populates="leases")


class Transaction(Base):
    __tablename__ = "transactions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    property_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("properties.id", ondelete="CASCADE"), nullable=False
    )

    txn_date: Mapped[DateTime] = mapped_column(DateTime, nullable=False)
    txn_type: Mapped[str] = mapped_column(String(50), nullable=False)
    amount: Mapped[float] = mapped_column(Float, nullable=False)
    memo: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[DateTime] = mapped_column(DateTime, nullable=False, server_default=func.now())


class Valuation(Base):
    __tablename__ = "valuations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    property_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("properties.id", ondelete="CASCADE"), nullable=False
    )

    as_of: Mapped[DateTime] = mapped_column(DateTime, nullable=False)

    estimated_value: Mapped[float] = mapped_column(Float, nullable=False)
    loan_balance: Mapped[float | None] = mapped_column(Float, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[DateTime] = mapped_column(DateTime, nullable=False, server_default=func.now())


class AgentRun(Base):
    __tablename__ = "agent_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    property_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # example keys: "s8_intake", "tenant_screen", "inspection_prep"
    agent_key: Mapped[str] = mapped_column(String(80), nullable=False)

    # queued | running | needs_human | done | failed
    status: Mapped[str] = mapped_column(String(30), nullable=False, server_default="queued")

    input_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    output_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[DateTime] = mapped_column(DateTime, nullable=False, server_default=func.now())


class AgentMessage(Base):
    __tablename__ = "agent_messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # thread key ties messages to a property workflow:
    # e.g. "property:123" or "deal:45"
    thread_key: Mapped[str] = mapped_column(String(120), nullable=False)

    # "human" | "agent:<name>" | "system"
    sender: Mapped[str] = mapped_column(String(80), nullable=False)
    recipient: Mapped[str | None] = mapped_column(String(80), nullable=True)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[DateTime] = mapped_column(DateTime, nullable=False, server_default=func.now())
