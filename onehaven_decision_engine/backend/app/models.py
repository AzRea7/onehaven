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
    JSON,
    Index,
    BigInteger,
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
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


class AppUser(Base):
    __tablename__ = "app_users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("organizations.id"), index=True, nullable=True)
    email: Mapped[str] = mapped_column(String(200), nullable=False, unique=True, index=True)
    role: Mapped[str] = mapped_column(String(20), nullable=False, default="user")  # user|admin
    display_name: Mapped[Optional[str]] = mapped_column(String(160), nullable=True)
    password_hash: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    email_verified: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    last_login_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    org = relationship("Organization", backref="users")


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
    __table_args__ = (
        UniqueConstraint("org_id", "property_id", name="uq_property_states_org_property"),
        Index("ix_property_states_org_stage", "org_id", "current_stage"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), index=True, nullable=False)
    property_id: Mapped[int] = mapped_column(Integer, ForeignKey("properties.id"), index=True, nullable=False)

    current_stage: Mapped[str] = mapped_column(String(64), nullable=False, default="import", index=True)
    constraints_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    outstanding_tasks_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    last_transitioned_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)


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
    __table_args__ = (
        UniqueConstraint("org_id", "address", "city", "state", "zip", name="uq_properties_org_addr"),
        Index("ix_properties_org_state", "org_id", "state"),
        Index("ix_properties_org_county", "org_id", "county"),
        Index("ix_properties_org_is_red_zone", "org_id", "is_red_zone"),
        Index("ix_properties_org_normalized_address", "org_id", "normalized_address"),
        Index("ix_properties_org_geocode_last_refreshed", "org_id", "geocode_last_refreshed"),
        Index("ix_properties_org_listing_hidden", "org_id", "listing_hidden"),
        Index("ix_properties_org_listing_status", "org_id", "listing_status"),
        Index("ix_properties_org_listing_last_seen_at", "org_id", "listing_last_seen_at"),
        Index("ix_properties_org_listing_removed_at", "org_id", "listing_removed_at"),
        Index("ix_properties_org_listing_hidden_status", "org_id", "listing_hidden", "listing_status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)

    address: Mapped[str] = mapped_column(String(255), nullable=False)
    city: Mapped[str] = mapped_column(String(120), nullable=False)
    state: Mapped[str] = mapped_column(String(2), nullable=False, default="MI")
    zip: Mapped[str] = mapped_column(String(10), nullable=False)

    normalized_address: Mapped[Optional[str]] = mapped_column(String(400), nullable=True)

    bedrooms: Mapped[int] = mapped_column(Integer, nullable=False)
    bathrooms: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    square_feet: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    year_built: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    has_garage: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    property_type: Mapped[str] = mapped_column(String(60), nullable=False, default="single_family")

    lat: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    lng: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    geocode_source: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)
    geocode_confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    geocode_last_refreshed: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)


        # ---- durable listing visibility / lifecycle fields ----
    listing_status: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    listing_hidden: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    listing_hidden_reason: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)

    listing_last_seen_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    listing_removed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    listing_listed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    listing_created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    listing_days_on_market: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    listing_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    listing_mls_name: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    listing_mls_number: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    listing_type: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)

    listing_zillow_url: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)

    listing_agent_name: Mapped[Optional[str]] = mapped_column(String(160), nullable=True)
    listing_agent_phone: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    listing_agent_email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    listing_agent_website: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)

    listing_office_name: Mapped[Optional[str]] = mapped_column(String(160), nullable=True)
    listing_office_phone: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    listing_office_email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    county: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)

    is_red_zone: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    crime_density: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    crime_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    offender_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

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

    photos: Mapped[List["PropertyPhoto"]] = relationship(
        back_populates="property",
        cascade="all, delete-orphan",
    )

    rehab_tasks: Mapped[List["RehabTask"]] = relationship(back_populates="property", cascade="all, delete-orphan")
    leases: Mapped[List["Lease"]] = relationship(back_populates="property", cascade="all, delete-orphan")
    transactions: Mapped[List["Transaction"]] = relationship(back_populates="property", cascade="all, delete-orphan")
    valuations: Mapped[List["Valuation"]] = relationship(back_populates="property", cascade="all, delete-orphan")

    agent_runs: Mapped[List["AgentRun"]] = relationship(
        primaryjoin="Property.id==foreign(AgentRun.property_id)",
        viewonly=True,
    )

class PropertyInventorySnapshot(Base):
    __tablename__ = "property_inventory_snapshots"
    __table_args__ = (
        UniqueConstraint("org_id", "property_id", name="uq_property_inventory_snapshots_org_property"),
        Index("ix_property_inventory_snapshots_org_stage", "org_id", "current_stage"),
        Index("ix_property_inventory_snapshots_org_pane", "org_id", "current_pane"),
        Index("ix_property_inventory_snapshots_org_decision", "org_id", "normalized_decision"),
        Index("ix_property_inventory_snapshots_org_county", "org_id", "county"),
        Index("ix_property_inventory_snapshots_org_city", "org_id", "city"),
        Index("ix_property_inventory_snapshots_org_state", "org_id", "state"),
        Index("ix_property_inventory_snapshots_org_enriched", "org_id", "is_fully_enriched"),
        Index("ix_property_inventory_snapshots_org_updated", "org_id", "snapshot_updated_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)
    property_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("properties.id", ondelete="CASCADE"), nullable=False, index=True
    )

    address: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    normalized_address: Mapped[Optional[str]] = mapped_column(String(400), nullable=True)
    city: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    county: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    state: Mapped[Optional[str]] = mapped_column(String(2), nullable=True)
    zip: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)

    lat: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    lng: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    geocode_confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    crime_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    offender_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    is_red_zone: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    property_type: Mapped[Optional[str]] = mapped_column(String(60), nullable=True)
    bedrooms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    bathrooms: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    square_feet: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    asking_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    market_rent_estimate: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    approved_rent_ceiling: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    section8_fmr: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    projected_monthly_cashflow: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    dscr: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    current_stage: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    current_stage_label: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    current_pane: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    current_pane_label: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    normalized_decision: Mapped[Optional[str]] = mapped_column(String(20), nullable=True, index=True)
    gate_status: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    route_reason: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    completeness: Mapped[Optional[str]] = mapped_column(String(20), nullable=True, index=True)
    is_fully_enriched: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    blockers_json: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    next_actions_json: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    source_updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    snapshot_updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    property: Mapped["Property"] = relationship("Property", viewonly=True)


class PaneSummarySnapshot(Base):
    __tablename__ = "pane_summary_snapshots"
    __table_args__ = (
        UniqueConstraint("org_id", "scope_hash", "pane_key", name="uq_pane_summary_snapshots_org_scope_pane"),
        Index("ix_pane_summary_snapshots_org_pane", "org_id", "pane_key"),
        Index("ix_pane_summary_snapshots_org_updated", "org_id", "snapshot_updated_at"),
        Index("ix_pane_summary_snapshots_org_scope", "org_id", "scope_hash"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)

    scope_hash: Mapped[str] = mapped_column(String(96), nullable=False, index=True)
    pane_key: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    state_filter: Mapped[Optional[str]] = mapped_column(String(2), nullable=True)
    county_filter: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    city_filter: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    q_filter: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    assigned_user_id_filter: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    property_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    kpis_json: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    top_blockers_json: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    top_actions_json: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    snapshot_updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

class GeocodeCache(Base):
    __tablename__ = "geocode_cache"
    __table_args__ = (
        UniqueConstraint("normalized_address", name="uq_geocode_cache_normalized_address"),
        Index("ix_geocode_cache_source", "source"),
        Index("ix_geocode_cache_expires_at", "expires_at"),
        Index("ix_geocode_cache_last_refreshed_at", "last_refreshed_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    normalized_address: Mapped[str] = mapped_column(String(400), nullable=False)
    raw_address: Mapped[Optional[str]] = mapped_column(String(400), nullable=True)

    city: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    state: Mapped[Optional[str]] = mapped_column(String(2), nullable=True)
    zip: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    county: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)

    lat: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    lng: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    source: Mapped[str] = mapped_column(String(40), nullable=False, default="unknown")
    confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    formatted_address: Mapped[Optional[str]] = mapped_column(String(400), nullable=True)
    provider_response_json: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    hit_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    last_refreshed_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)


class ImportSnapshot(Base):
    __tablename__ = "import_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), index=True, nullable=False)

    source: Mapped[str] = mapped_column(String(40), nullable=False)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    deals: Mapped[List["Deal"]] = relationship(back_populates="snapshot")


class Deal(Base):
    __tablename__ = "deals"
    __table_args__ = (
        UniqueConstraint("org_id", "source_fingerprint", name="uq_deals_org_source_fingerprint"),
        Index("ix_deals_org_property", "org_id", "property_id"),
        Index("ix_deals_org_created_at", "org_id", "created_at"),
    )

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

    decision: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    purchase_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    closing_date: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    loan_amount: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

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


class IngestionSource(Base):
    __tablename__ = "ingestion_sources"
    __table_args__ = (
        UniqueConstraint("org_id", "provider", "slug", name="uq_ingestion_sources_org_provider_slug"),
        Index("ix_ingestion_sources_org_provider", "org_id", "provider"),
        Index("ix_ingestion_sources_org_enabled", "org_id", "is_enabled"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)

    provider: Mapped[str] = mapped_column(String(50), nullable=False)
    slug: Mapped[str] = mapped_column(String(100), nullable=False)
    display_name: Mapped[str] = mapped_column(String(160), nullable=False)

    source_type: Mapped[str] = mapped_column(String(30), nullable=False, default="api")
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="disconnected")
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    base_url: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    webhook_secret_hint: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)

    credentials_json: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    config_json: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    cursor_json: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    schedule_cron: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    sync_interval_minutes: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, default=60)
    last_synced_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_success_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_failure_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    next_scheduled_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    last_error_summary: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    last_error_json: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    runs: Mapped[List["IngestionRun"]] = relationship(
        "IngestionRun",
        back_populates="source",
        cascade="all, delete-orphan",
    )

    market_sync_states: Mapped[List["MarketSyncState"]] = relationship(
        "MarketSyncState",
        back_populates="source",
        cascade="all, delete-orphan",
    )

class MarketSyncState(Base):
    __tablename__ = "market_sync_states"
    __table_args__ = (
        UniqueConstraint("org_id", "source_id", "market_slug", name="uq_market_sync_states_org_source_market"),
        Index("ix_market_sync_states_org_market", "org_id", "market_slug"),
        Index("ix_market_sync_states_org_provider_market", "org_id", "provider", "market_slug"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    org_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("organizations.id"),
        nullable=False,
        index=True,
    )
    source_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("ingestion_sources.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    provider: Mapped[str] = mapped_column(String(50), nullable=False)
    market_slug: Mapped[str] = mapped_column(String(120), nullable=False)

    state: Mapped[str] = mapped_column(String(2), nullable=False, default="MI")
    county: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    city: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)

    status: Mapped[str] = mapped_column(String(30), nullable=False, default="idle")

    cursor_json: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    last_page: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    last_shard: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    last_sort_mode: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)
    last_requested_limit: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    last_sync_started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_sync_completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_seen_provider_record_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    last_page_fingerprint: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)

    market_exhausted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    backfill_completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )

    source: Mapped["IngestionSource"] = relationship("IngestionSource", back_populates="market_sync_states")

class IngestionRun(Base):
    __tablename__ = "ingestion_runs"
    __table_args__ = (
        Index("ix_ingestion_runs_org_source_started", "org_id", "source_id", "started_at"),
        Index("ix_ingestion_runs_org_status", "org_id", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)
    source_id: Mapped[int] = mapped_column(Integer, ForeignKey("ingestion_sources.id"), nullable=False, index=True)

    trigger_type: Mapped[str] = mapped_column(String(30), nullable=False, default="manual")
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="queued")
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    records_seen: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    records_imported: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    properties_created: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    properties_updated: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    deals_created: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    deals_updated: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    rent_rows_upserted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    photos_upserted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    duplicates_skipped: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    invalid_rows: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_summary: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error_json: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    summary_json: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    source = relationship("IngestionSource", back_populates="runs")


class IngestionRecordLink(Base):
    __tablename__ = "ingestion_record_links"
    __table_args__ = (
        UniqueConstraint("org_id", "provider", "external_record_id", name="uq_ingestion_record_links_org_provider_ext"),
        Index("ix_ingestion_record_links_org_property", "org_id", "property_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)

    provider: Mapped[str] = mapped_column(String(50), nullable=False)
    source_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("ingestion_sources.id"), nullable=True, index=True)

    external_record_id: Mapped[str] = mapped_column(String(200), nullable=False)
    external_url: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)
    fingerprint: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)

    property_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("properties.id"), nullable=True, index=True)
    deal_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("deals.id"), nullable=True, index=True)

    first_seen_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    raw_json: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)


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

    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


class UnderwritingResult(Base):
    __tablename__ = "underwriting_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)

    deal_id: Mapped[int] = mapped_column(ForeignKey("deals.id", ondelete="CASCADE"), nullable=False)

    rent_explain_run_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("rent_explain_runs.id", ondelete="SET NULL"),
        nullable=True,
    )

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
    __table_args__ = (
        Index("ix_inspections_template_key", "template_key"),
        Index("ix_inspections_template_version", "template_version"),
        Index("ix_inspections_result_status", "result_status"),
        Index("ix_inspections_readiness_status", "readiness_status"),
        Index("ix_inspections_property_template_version", "property_id", "template_version"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)

    property_id: Mapped[int] = mapped_column(ForeignKey("properties.id", ondelete="CASCADE"), nullable=False)
    inspector_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("inspectors.id", ondelete="SET NULL"), nullable=True
    )

    inspection_date: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    passed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    reinspect_required: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # ---- Step 18 real inspection compliance foundation ----
    template_key: Mapped[str] = mapped_column(String(80), nullable=False, default="hqs")
    template_version: Mapped[str] = mapped_column(String(40), nullable=False, default="hqs_v1")
    inspection_status: Mapped[str] = mapped_column(String(32), nullable=False, default="completed")
    result_status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    inspection_method: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    standards_version: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)

    readiness_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    readiness_status: Mapped[str] = mapped_column(String(32), nullable=False, default="unknown")

    total_items: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    passed_items: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_items: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    blocked_items: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    na_items: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_critical_items: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    evidence_summary_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    last_scored_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    # ------------------------------------------------------

    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    property: Mapped["Property"] = relationship(back_populates="inspections")
    inspector: Mapped[Optional["Inspector"]] = relationship(back_populates="inspections")
    items: Mapped[List["InspectionItem"]] = relationship(back_populates="inspection", cascade="all, delete-orphan")


class InspectionItem(Base):
    __tablename__ = "inspection_items"
    __table_args__ = (
        UniqueConstraint("inspection_id", "code", name="uq_inspection_item_per_code"),
        Index("ix_inspection_items_result_status", "result_status"),
        Index("ix_inspection_items_inspection_result_status", "inspection_id", "result_status"),
        Index("ix_inspection_items_category", "category"),
        Index("ix_inspection_items_requires_reinspection", "requires_reinspection"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    inspection_id: Mapped[int] = mapped_column(ForeignKey("inspections.id", ondelete="CASCADE"), nullable=False)

    code: Mapped[str] = mapped_column(String(80), nullable=False)

    failed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    severity: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    location: Mapped[Optional[str]] = mapped_column(String(180), nullable=True)
    details: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # ---- Step 18 real inspection compliance foundation ----
    category: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    result_status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    fail_reason: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    remediation_guidance: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    evidence_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    photo_references_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    standard_label: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    standard_citation: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    readiness_impact: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    requires_reinspection: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    # ------------------------------------------------------

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
    __table_args__ = (UniqueConstraint("org_id", "property_id", "item_code", name="uq_checklist_item_org_property_code"),)

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
# Trust models
# -----------------------------
class TrustSignal(Base):
    __tablename__ = "trust_signals"
    __table_args__ = (
        Index("ix_trust_signals_org_entity", "org_id", "entity_type", "entity_id", "created_at"),
        Index("ix_trust_signals_org_entity_key", "org_id", "entity_type", "entity_id", "signal_key"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)

    entity_type: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    entity_id: Mapped[str] = mapped_column(String(80), nullable=False, index=True)

    signal_key: Mapped[str] = mapped_column(String(120), nullable=False, index=True)

    value: Mapped[float] = mapped_column(Float, nullable=False)
    weight: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)

    meta_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


class TrustScore(Base):
    __tablename__ = "trust_scores"
    __table_args__ = (UniqueConstraint("org_id", "entity_type", "entity_id", name="uq_trust_scores_org_entity"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)

    entity_type: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    entity_id: Mapped[str] = mapped_column(String(80), nullable=False, index=True)

    score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    components_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


# -----------------------------
# Phase 4/5: rehab, tenants, cash, equity
# -----------------------------
class PropertyPhoto(Base):
    __tablename__ = "property_photos"
    __table_args__ = (
        UniqueConstraint("org_id", "property_id", "url", name="uq_property_photos_org_property_url"),
        Index("ix_property_photos_org_property", "org_id", "property_id"),
        Index("ix_property_photos_org_source", "org_id", "source"),
        Index("ix_property_photos_org_kind", "org_id", "kind"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("organizations.id"),
        nullable=False,
        index=True,
    )
    property_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("properties.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    source: Mapped[str] = mapped_column(String(40), nullable=False, default="upload")
    kind: Mapped[str] = mapped_column(String(40), nullable=False, default="unknown")
    label: Mapped[Optional[str]] = mapped_column(String(160), nullable=True)

    url: Mapped[str] = mapped_column(Text, nullable=False)
    storage_key: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    content_type: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )

    property: Mapped["Property"] = relationship(back_populates="photos")


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

    status: Mapped[str] = mapped_column(String(20), nullable=False, default="todo")
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
    txn_type: Mapped[str] = mapped_column(String(80), nullable=False, default="other")
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
        Index("ix_agent_runs_org_property_id_id", "org_id", "property_id", "id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), index=True, nullable=False)

    property_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("properties.id"), index=True, nullable=True)
    agent_key: Mapped[str] = mapped_column(String(80), nullable=False, index=True)

    status: Mapped[str] = mapped_column(String(20), nullable=False, default="queued")

    input_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    output_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    idempotency_key: Mapped[Optional[str]] = mapped_column(String(128), nullable=True, index=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    heartbeat_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    created_by_user_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("app_users.id"), nullable=True)

    approval_status: Mapped[str] = mapped_column(String(20), nullable=False, default="not_required")
    approved_by_user_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("app_users.id"), nullable=True)
    approved_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    proposed_actions_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class AgentMessage(Base):
    __tablename__ = "agent_messages"
    __table_args__ = (Index("ix_agent_messages_org_run_id_id", "org_id", "run_id", "id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), nullable=False, index=True)

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
    run_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("agent_runs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    property_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("properties.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )

    agent_key: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(40), nullable=False, index=True)

    payload_json: Mapped[str] = mapped_column(Text, nullable=False, server_default="{}")

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False),
        nullable=False,
        server_default=func.now(),
        index=True,
    )

    __table_args__ = (
        Index("ix_agent_trace_events_org_run_id_id", "org_id", "run_id", "id"),
        Index("ix_agent_trace_events_org_property_id_id", "org_id", "property_id", "id"),
    )


class AuthIdentity(Base):
    __tablename__ = "auth_identities"
    __table_args__ = (UniqueConstraint("email", name="uq_auth_identities_email"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    email: Mapped[str] = mapped_column(String(255), nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    email_verified_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


class EmailToken(Base):
    __tablename__ = "email_tokens"
    __table_args__ = (UniqueConstraint("token_hash", name="uq_email_tokens_token_hash"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, nullable=False)
    email: Mapped[str] = mapped_column(String(255), nullable=False)
    purpose: Mapped[str] = mapped_column(String(50), nullable=False)
    token_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    used_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


class ApiKey(Base):
    __tablename__ = "api_keys"
    __table_args__ = (UniqueConstraint("org_id", "name", name="uq_api_keys_org_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, nullable=False)
    name: Mapped[str] = mapped_column(String(80), nullable=False)
    key_prefix: Mapped[str] = mapped_column(String(16), nullable=False)
    key_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    created_by_user_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


class Plan(Base):
    __tablename__ = "plans"
    __table_args__ = (UniqueConstraint("code", name="uq_plans_code"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(50), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    limits_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


class OrgSubscription(Base):
    __tablename__ = "org_subscriptions"
    __table_args__ = (UniqueConstraint("org_id", name="uq_org_subscriptions_org"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, nullable=False)
    plan_code: Mapped[str] = mapped_column(String(50), nullable=False, default="free")
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="active")
    stripe_customer_id: Mapped[str | None] = mapped_column(String(80), nullable=True)
    stripe_subscription_id: Mapped[str | None] = mapped_column(String(80), nullable=True)
    current_period_end: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


class UsageLedger(Base):
    __tablename__ = "usage_ledger"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, nullable=False)
    metric: Mapped[str] = mapped_column(String(80), nullable=False)
    units: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    meta_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


class ExternalBudgetLedger(Base):
    __tablename__ = "external_budget_ledger"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, nullable=False)
    provider: Mapped[str] = mapped_column(String(50), nullable=False)
    cost_units: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    meta_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

class OrgLock(Base):
    __tablename__ = "org_locks"
    __table_args__ = (
        UniqueConstraint("org_id", "lock_key", name="uq_org_locks_org_lock_key"),
        Index("ix_org_locks_org_lock_key", "org_id", "lock_key"),
        Index("ix_org_locks_locked_until", "locked_until"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    org_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    lock_key: Mapped[str] = mapped_column(String(120), nullable=False)

    owner_token: Mapped[str] = mapped_column(
        String(120),
        nullable=False,
        default="system",
    )

    acquired_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
    )

    locked_until: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        index=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )

class AgentRunDeadletter(Base):
    __tablename__ = "agent_run_deadletters"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, nullable=False)
    run_id: Mapped[int] = mapped_column(Integer, nullable=False)
    agent_key: Mapped[str] = mapped_column(String(80), nullable=False)
    reason: Mapped[str] = mapped_column(String(120), nullable=False)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


from .policy_models import JurisdictionProfile, HqsRule, HqsAddendumRule, HudFmrRecord  # noqa: E402,F401
