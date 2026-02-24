# backend/app/models.py
from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    Index,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .db import Base


# -----------------------------
# Multitenant RBAC tables
# -----------------------------
class Organization(Base):
    __tablename__ = "organizations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    slug: Mapped[str] = mapped_column(String(80), nullable=False, unique=True, index=True)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


class AppUser(Base):
    __tablename__ = "app_users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    email: Mapped[str] = mapped_column(String(200), nullable=False, unique=True, index=True)
    display_name: Mapped[Optional[str]] = mapped_column(String(160), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


class OrgMembership(Base):
    __tablename__ = "org_memberships"
    __table_args__ = (UniqueConstraint("org_id", "user_id", name="uq_org_memberships_org_user"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), index=True, nullable=False)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("app_users.id"), index=True, nullable=False)
    role: Mapped[str] = mapped_column(String(20), nullable=False, default="owner")  # owner|operator|analyst
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


class AuditEvent(Base):
    __tablename__ = "audit_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), index=True, nullable=False)
    actor_user_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("app_users.id"), nullable=True)

    action: Mapped[str] = mapped_column(String(80), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(80), nullable=False)
    entity_id: Mapped[str] = mapped_column(String(80), nullable=False)

    before_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    after_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


class PropertyState(Base):
    __tablename__ = "property_states"
    __table_args__ = (UniqueConstraint("org_id", "property_id", name="uq_property_states_org_property"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), index=True, nullable=False)
    property_id: Mapped[int] = mapped_column(Integer, ForeignKey("properties.id"), index=True, nullable=False)

    current_stage: Mapped[str] = mapped_column(String(30), nullable=False, default="deal")
    constraints_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    outstanding_tasks_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


class WorkflowEvent(Base):
    __tablename__ = "workflow_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), index=True, nullable=False)

    property_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("properties.id"), nullable=True, index=True)
    actor_user_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("app_users.id"), nullable=True)

    event_type: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    payload_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


# -----------------------------
# Core domain: Properties / Deals
# -----------------------------
class Property(Base):
    __tablename__ = "properties"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)

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
    checklist_items: Mapped[List["PropertyChecklistItem"]] = relationship(
        back_populates="property", cascade="all, delete-orphan"
    )

    rehab_tasks: Mapped[List["RehabTask"]] = relationship(back_populates="property", cascade="all, delete-orphan")
    leases: Mapped[List["Lease"]] = relationship(back_populates="property", cascade="all, delete-orphan")
    transactions: Mapped[List["Transaction"]] = relationship(back_populates="property", cascade="all, delete-orphan")
    valuations: Mapped[List["Valuation"]] = relationship(back_populates="property", cascade="all, delete-orphan")


class ImportSnapshot(Base):
    __tablename__ = "import_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # org scoping (Phase 5)
    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), index=True, nullable=False)

    source: Mapped[str] = mapped_column(String(40), nullable=False)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    deals: Mapped[List["Deal"]] = relationship(back_populates="snapshot")


class Deal(Base):
    __tablename__ = "deals"

    __table_args__ = (UniqueConstraint("org_id", "source_fingerprint", name="uq_deals_org_source_fingerprint"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)

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

    strategy: Mapped[str] = mapped_column(String(20), nullable=False, default="section8")

    financing_type: Mapped[str] = mapped_column(String(40), nullable=False, default="dscr")
    interest_rate: Mapped[float] = mapped_column(Float, nullable=False, default=0.07)
    term_years: Mapped[int] = mapped_column(Integer, nullable=False, default=30)
    down_payment_pct: Mapped[float] = mapped_column(Float, nullable=False, default=0.20)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    property: Mapped["Property"] = relationship(back_populates="deals")
    results: Mapped[List["UnderwritingResult"]] = relationship(back_populates="deal", cascade="all, delete-orphan")
    snapshot: Mapped[Optional["ImportSnapshot"]] = relationship(back_populates="deals")


# -----------------------------
# Rent
# -----------------------------
class RentAssumption(Base):
    __tablename__ = "rent_assumptions"
    __table_args__ = (UniqueConstraint("org_id", "property_id", name="uq_rent_assumptions_org_property"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)
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


# -----------------------------
# Jurisdiction + Underwriting
# -----------------------------
class JurisdictionRule(Base):
    __tablename__ = "jurisdiction_rules"
    __table_args__ = (UniqueConstraint("org_id", "city", "state", name="uq_jurisdiction_org_city_state"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("organizations.id"), index=True, nullable=True)

    city: Mapped[str] = mapped_column(String(120), nullable=False)
    state: Mapped[str] = mapped_column(String(2), nullable=False, default="MI")

    rental_license_required: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    inspection_authority: Mapped[Optional[str]] = mapped_column(String(180), nullable=True)
    typical_fail_points_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    registration_fee: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    processing_days: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    inspection_frequency: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)
    tenant_waitlist_depth: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)

    # ✅ column exists after patched migration 0018 runs
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


class UnderwritingResult(Base):
    __tablename__ = "underwriting_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)

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


# -----------------------------
# Inspections
# -----------------------------
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

    # ✅ CRITICAL: org scope (state machine + orchestrator assumes this exists)
    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)

    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id", ondelete="CASCADE"), nullable=False)
    inspector_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("inspectors.id", ondelete="SET NULL"), nullable=True
    )

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


# -----------------------------
# API usage limiter
# -----------------------------
class ApiUsage(Base):
    __tablename__ = "api_usage"
    __table_args__ = (UniqueConstraint("provider", "day", name="uq_api_usage_provider_day"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    provider: Mapped[str] = mapped_column(String(50), nullable=False)
    day: Mapped[date] = mapped_column(Date, nullable=False)
    calls: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


# -----------------------------
# Phase 3: checklist templates + persisted checklists
# -----------------------------
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
    __table_args__ = (
        UniqueConstraint(
            "org_id",
            "property_id",
            "strategy",
            "version",
            name="uq_property_checklists_org_property_strategy_version",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)
    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id", ondelete="CASCADE"), nullable=False)

    strategy: Mapped[str] = mapped_column(String(20), nullable=False, default="section8")
    version: Mapped[str] = mapped_column(String(32), nullable=False, default="v1")

    generated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    items_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")

    property: Mapped["Property"] = relationship(back_populates="checklists")
    items: Mapped[List["PropertyChecklistItem"]] = relationship(back_populates="checklist", cascade="all, delete-orphan")


class PropertyChecklistItem(Base):
    __tablename__ = "property_checklist_items"
    __table_args__ = (
        UniqueConstraint("org_id", "property_id", "item_code", name="uq_checklist_item_org_property_code"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)
    property_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("properties.id", ondelete="CASCADE"), nullable=False, index=True
    )
    checklist_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("property_checklists.id", ondelete="CASCADE"), nullable=True, index=True
    )

    item_code: Mapped[str] = mapped_column(String(80), nullable=False)
    category: Mapped[str] = mapped_column(String(80), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)

    severity: Mapped[int] = mapped_column(Integer, nullable=False, default=2)
    common_fail: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    applies_if_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    status: Mapped[str] = mapped_column(String(20), nullable=False, default="todo")
    marked_by_user_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("app_users.id"), nullable=True)
    marked_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    proof_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    property: Mapped["Property"] = relationship(back_populates="checklist_items")
    checklist: Mapped[Optional["PropertyChecklist"]] = relationship(back_populates="items")


# -----------------------------
# Phase 4/5: rehab, tenants, cash, equity
# -----------------------------
class RehabTask(Base):
    __tablename__ = "rehab_tasks"
    __table_args__ = (UniqueConstraint("org_id", "property_id", "title", name="uq_rehab_tasks_org_property_title"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)
    property_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("properties.id", ondelete="CASCADE"), nullable=False, index=True
    )

    title: Mapped[str] = mapped_column(String(255), nullable=False)
    category: Mapped[str] = mapped_column(String(60), nullable=False, default="rehab")
    inspection_relevant: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    status: Mapped[str] = mapped_column(String(20), nullable=False, default="todo")  # todo|in_progress|done|blocked
    cost_estimate: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    vendor: Mapped[Optional[str]] = mapped_column(String(160), nullable=True)
    deadline: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    property: Mapped["Property"] = relationship(back_populates="rehab_tasks")


class Tenant(Base):
    __tablename__ = "tenants"
    __table_args__ = (UniqueConstraint("org_id", "full_name", name="uq_tenants_org_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)

    full_name: Mapped[str] = mapped_column(String(200), nullable=False)
    phone: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)
    email: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    voucher_status: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    leases: Mapped[List["Lease"]] = relationship(back_populates="tenant", cascade="all, delete-orphan")


class Lease(Base):
    __tablename__ = "leases"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)

    property_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("properties.id", ondelete="CASCADE"), nullable=False, index=True
    )
    tenant_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )

    start_date: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    end_date: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    total_rent: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    tenant_portion: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    housing_authority_portion: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    hap_contract_status: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    property: Mapped["Property"] = relationship(back_populates="leases")
    tenant: Mapped["Tenant"] = relationship(back_populates="leases")


class Transaction(Base):
    __tablename__ = "transactions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)

    property_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("properties.id", ondelete="CASCADE"), nullable=False, index=True
    )

    txn_date: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    txn_type: Mapped[str] = mapped_column(String(80), nullable=False, default="other")  # income|expense|capex|other
    amount: Mapped[float] = mapped_column(Float, nullable=False)
    memo: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    property: Mapped["Property"] = relationship(back_populates="transactions")


class Valuation(Base):
    __tablename__ = "valuations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)

    property_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("properties.id", ondelete="CASCADE"), nullable=False, index=True
    )

    as_of: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    estimated_value: Mapped[float] = mapped_column(Float, nullable=False)
    loan_balance: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    property: Mapped["Property"] = relationship(back_populates="valuations")


# -----------------------------
# Inspection analytics / future ops
# -----------------------------
class InspectionEvent(Base):
    __tablename__ = "inspection_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    property_id: Mapped[int] = mapped_column(Integer, ForeignKey("properties.id", ondelete="CASCADE"), nullable=False)

    inspector_name: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    inspection_date: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    status: Mapped[str] = mapped_column(String(20), nullable=False, server_default="scheduled")

    fail_items_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    resolution_notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    days_to_resolve: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    reinspection_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())


# -----------------------------
# Agents
# -----------------------------
class AgentRun(Base):
    __tablename__ = "agent_runs"
    __table_args__ = (
        UniqueConstraint("org_id", "idempotency_key", name="uq_agent_runs_org_idempotency_key"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), index=True, nullable=False)

    property_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("properties.id"), index=True, nullable=True)
    agent_key: Mapped[str] = mapped_column(String(80), nullable=False, index=True)

    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="queued",
    )  # queued|running|done|failed|blocked

    input_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    output_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Reliability semantics
    idempotency_key: Mapped[Optional[str]] = mapped_column(String(128), nullable=True, index=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    heartbeat_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    created_by_user_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("app_users.id"), nullable=True)

    # Approval semantics (mutation agents)
    approval_status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="not_required",
    )  # not_required|pending|approved|rejected
    approved_by_user_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("app_users.id"), nullable=True)
    approved_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    proposed_actions_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class AgentMessage(Base):
    __tablename__ = "agent_messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)

    # ✅ tie messages to a run/property for querying + SSE
    run_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("agent_runs.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    property_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("properties.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )

    thread_key: Mapped[str] = mapped_column(String(120), nullable=False, index=True)
    sender: Mapped[str] = mapped_column(String(80), nullable=False)
    recipient: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    message: Mapped[str] = mapped_column(Text, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())


class AgentSlotAssignment(Base):
    __tablename__ = "agent_slot_assignments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)

    slot_key: Mapped[str] = mapped_column(String(80), index=True)
    property_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("properties.id"), nullable=True, index=True)

    owner_type: Mapped[str] = mapped_column(String(20), default="human")
    assignee: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    status: Mapped[str] = mapped_column(String(20), default="idle")
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class RentExplainRun(Base):
    __tablename__ = "rent_explain_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    org_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    property_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("properties.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    strategy: Mapped[str] = mapped_column(String(20), nullable=False, server_default="section8")

    cap_reason: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    explain_json: Mapped[str] = mapped_column(Text, nullable=False, server_default="{}")

    decision_version: Mapped[str] = mapped_column(String(64), nullable=False, server_default="unknown")
    payment_standard_pct_used: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now(), index=True)


class AgentTraceEvent(Base):
    __tablename__ = "agent_trace_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    org_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    property_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("properties.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )

    run_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("agent_runs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    agent_key: Mapped[str] = mapped_column(String(80), nullable=False, index=True)

    # examples: started | context | tool_call | tool_result | decision | warning | final | validation | blocked | approved | applied | error
    event_type: Mapped[str] = mapped_column(String(40), nullable=False, index=True)

    # structured JSON serialized to text for portability
    payload_json: Mapped[str] = mapped_column(Text, nullable=False, server_default="{}")

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now(), index=True)

    __table_args__ = (
        Index("ix_agent_trace_events_org_run_id_id", "org_id", "run_id", "id"),
        Index("ix_agent_trace_events_org_property_id_id", "org_id", "property_id", "id"),
    )


from .policy_models import JurisdictionProfile, HqsRule, HqsAddendumRule, HudFmrRecord  # noqa: E402,F401