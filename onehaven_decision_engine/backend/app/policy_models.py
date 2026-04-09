# backend/app/policy_models.py
from __future__ import annotations

from datetime import date, datetime
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from .db import Base


class PolicyCatalogEntry(Base):
    __tablename__ = "policy_catalog_entries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    state: Mapped[str] = mapped_column(String(8), nullable=False, default="MI")
    county: Mapped[str | None] = mapped_column(String(120), nullable=True)
    city: Mapped[str | None] = mapped_column(String(120), nullable=True)
    pha_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    program_type: Mapped[str | None] = mapped_column(String(64), nullable=True)

    url: Mapped[str] = mapped_column(Text, nullable=False)
    publisher: Mapped[str | None] = mapped_column(String(255), nullable=True)
    title: Mapped[str | None] = mapped_column(String(500), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_kind: Mapped[str | None] = mapped_column(String(120), nullable=True)

    is_authoritative: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100)

    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    is_override: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    baseline_url: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class JurisdictionProfile(Base):
    __tablename__ = "jurisdiction_profiles"
    __table_args__ = (
        UniqueConstraint("org_id", "state", "county", "city", name="uq_jp_scope_state_county_city"),
        Index("ix_jp_scope_lookup", "state", "county", "city"),
        Index("ix_jp_completeness_status", "completeness_status"),
        Index("ix_jp_is_stale", "is_stale"),
        Index("ix_jp_last_verified_at", "last_verified_at"),
        Index("ix_jp_last_refresh_success_at", "last_refresh_success_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)

    state: Mapped[str] = mapped_column(String(2), nullable=False, default="MI")
    county: Mapped[str | None] = mapped_column(String(80), nullable=True)
    city: Mapped[str | None] = mapped_column(String(120), nullable=True)

    rental_registration_required: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    rental_registration_frequency: Mapped[str | None] = mapped_column(String(64), nullable=True)

    city_inspection_required: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    inspection_frequency: Mapped[str | None] = mapped_column(String(64), nullable=True)
    inspection_type: Mapped[str | None] = mapped_column(String(64), nullable=True)

    certificate_required: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    certificate_type: Mapped[str | None] = mapped_column(String(80), nullable=True)

    lead_paint_affidavit_required: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    local_contact_required: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    criminal_background_policy: Mapped[str | None] = mapped_column(String(120), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)

    # legacy / earlier fields retained
    completeness_status: Mapped[str] = mapped_column(String(40), nullable=False, default="unknown")
    completeness_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    confidence_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    is_stale: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    stale_reason: Mapped[str | None] = mapped_column(String(255), nullable=True)
    missing_categories_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    unresolved_items_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    latest_rule_version: Mapped[str | None] = mapped_column(String(64), nullable=True)

    last_verified_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_refresh_started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_refresh_success_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_refresh_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    metadata_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class HqsRule(Base):
    __tablename__ = "hqs_rules"
    __table_args__ = (
        Index("ix_hqs_rules_code", "code"),
        Index("ix_hqs_rules_category", "category"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(120), nullable=False, unique=True)
    category: Mapped[str] = mapped_column(String(80), nullable=False)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    severity: Mapped[str] = mapped_column(String(40), nullable=False, default="fail")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class HqsAddendumRule(Base):
    __tablename__ = "hqs_addendum_rules"
    __table_args__ = (
        Index("ix_hqs_addendum_rules_code", "code"),
        Index("ix_hqs_addendum_rules_category", "category"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(120), nullable=False, unique=True)
    category: Mapped[str] = mapped_column(String(80), nullable=False)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    severity: Mapped[str] = mapped_column(String(40), nullable=False, default="fail")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class HudFmrRecord(Base):
    __tablename__ = "hud_fmr_records"
    __table_args__ = (
        UniqueConstraint("year", "state", "county", "bedrooms", name="uq_hud_fmr_scope"),
        Index("ix_hud_fmr_scope", "year", "state", "county"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    state: Mapped[str] = mapped_column(String(2), nullable=False)
    county: Mapped[str] = mapped_column(String(80), nullable=False)
    bedrooms: Mapped[int] = mapped_column(Integer, nullable=False)
    amount: Mapped[float] = mapped_column(Float, nullable=False)
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class PolicySource(Base):
    __tablename__ = "policy_sources"
    __table_args__ = (
        Index("ix_policy_sources_state_county_city", "state", "county", "city"),
        Index("ix_policy_sources_org_state", "org_id", "state"),
        Index("ix_policy_sources_freshness_status", "freshness_status"),
        Index("ix_policy_sources_last_verified_at", "last_verified_at"),
        Index("ix_policy_sources_jurisdiction_slug", "jurisdiction_slug"),
        Index("ix_policy_sources_registry_status", "registry_status"),
        Index("ix_policy_sources_status_type", "registry_status", "source_type"),
        Index("ix_policy_sources_next_refresh_due_at", "next_refresh_due_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    org_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("organizations.id"),
        index=True,
        nullable=True,
    )

    state: Mapped[Optional[str]] = mapped_column(String(2), nullable=True)
    county: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    city: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)

    pha_name: Mapped[Optional[str]] = mapped_column(String(180), nullable=True)
    program_type: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)

    publisher: Mapped[Optional[str]] = mapped_column(String(180), nullable=True)
    title: Mapped[Optional[str]] = mapped_column(String(260), nullable=True)

    url: Mapped[str] = mapped_column(Text, nullable=False)
    content_type: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    http_status: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    retrieved_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    content_sha256: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

    raw_path: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    extracted_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    is_authoritative: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    normalized_categories_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    freshness_status: Mapped[str] = mapped_column(String(40), nullable=False, default="unknown")
    freshness_reason: Mapped[Optional[str]] = mapped_column(String(160), nullable=True)
    freshness_checked_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    published_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    effective_date: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_verified_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    source_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    source_type: Mapped[str] = mapped_column(String(40), nullable=False, default="local")
    jurisdiction_slug: Mapped[Optional[str]] = mapped_column(String(160), nullable=True)
    fetch_method: Mapped[str] = mapped_column(String(40), nullable=False, default="manual")
    trust_level: Mapped[float] = mapped_column(Float, nullable=False, default=0.5)
    refresh_interval_days: Mapped[int] = mapped_column(Integer, nullable=False, default=30)
    last_fetched_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    registry_status: Mapped[str] = mapped_column(String(40), nullable=False, default="active")
    fetch_config_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    registry_meta_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    fingerprint_algo: Mapped[str] = mapped_column(String(40), nullable=False, default="sha256")
    current_fingerprint: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    last_changed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    next_refresh_due_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_fetch_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    last_http_status: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    last_seen_same_fingerprint_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    source_metadata_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    last_verified_by_user_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=sa.text("now()"),
    )


class PolicySourceVersion(Base):
    __tablename__ = "policy_source_versions"
    __table_args__ = (
        Index("ix_policy_source_versions_source_retrieved", "source_id", "retrieved_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("policy_sources.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    retrieved_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    http_status: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    content_sha256: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    raw_path: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    content_type: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    fetch_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    extracted_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=sa.text("now()"),
    )


class PolicyAssertion(Base):
    __tablename__ = "policy_assertions"
    __table_args__ = (
        Index("ix_policy_assertions_scope", "state", "county", "city"),
        Index("ix_policy_assertions_status", "review_status"),
        Index("ix_policy_assertions_rule_key", "rule_key"),
        Index("ix_policy_assertions_rule_family", "rule_family"),
        Index("ix_policy_assertions_assertion_type", "assertion_type"),
        Index("ix_policy_assertions_stale_after", "stale_after"),
        Index("ix_policy_assertions_normalized_category", "normalized_category"),
        Index("ix_policy_assertions_coverage_status", "coverage_status"),
        Index("ix_policy_assertions_jurisdiction_slug", "jurisdiction_slug"),
        Index("ix_policy_assertions_governance_state", "governance_state"),
        Index("ix_policy_assertions_rule_status", "rule_status"),
        Index("ix_policy_assertions_rule_category", "rule_category"),
        Index("ix_policy_assertions_version_group_number", "version_group", "version_number"),
        Index("ix_policy_assertions_source_version_id", "source_version_id"),
        Index("ix_policy_assertions_is_current", "is_current"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    org_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("organizations.id"),
        index=True,
        nullable=True,
    )

    source_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("policy_sources.id", ondelete="SET NULL"),
        index=True,
        nullable=True,
    )
    source_version_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("policy_source_versions.id", ondelete="SET NULL"),
        index=True,
        nullable=True,
    )

    state: Mapped[Optional[str]] = mapped_column(String(2), nullable=True)
    county: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    city: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)

    pha_name: Mapped[Optional[str]] = mapped_column(String(180), nullable=True)
    program_type: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)

    rule_key: Mapped[str] = mapped_column(String(120), nullable=False)
    rule_family: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    assertion_type: Mapped[str] = mapped_column(String(40), nullable=False, default="document_reference")
    value_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    effective_date: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.25)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    source_rank: Mapped[int] = mapped_column(Integer, nullable=False, default=100)

    review_status: Mapped[str] = mapped_column(String(40), nullable=False, default="extracted")
    review_notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    reviewed_by_user_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    verification_reason: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    stale_after: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    superseded_by_assertion_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("policy_assertions.id", ondelete="SET NULL"),
        nullable=True,
    )
    replaced_by_assertion_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("policy_assertions.id", ondelete="SET NULL"),
        nullable=True,
    )

    jurisdiction_slug: Mapped[Optional[str]] = mapped_column(String(160), nullable=True)
    source_level: Mapped[str] = mapped_column(String(40), nullable=False, default="local")
    property_type: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    rule_category: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    required: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    blocking: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    source_citation: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    raw_excerpt: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    normalized_version: Mapped[str] = mapped_column(String(40), nullable=False, default="v1")
    rule_status: Mapped[str] = mapped_column(String(40), nullable=False, default="candidate")
    governance_state: Mapped[str] = mapped_column(String(40), nullable=False, default="draft")
    version_group: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    version_number: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    citation_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    rule_provenance_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    value_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    confidence_basis: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    change_summary: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    approved_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    approved_by_user_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    activated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    activated_by_user_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    replaced_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    normalized_category: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    coverage_status: Mapped[str] = mapped_column(String(40), nullable=False, default="candidate")
    source_freshness_status: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)

    extracted_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=sa.text("now()"),
    )


class JurisdictionCoverageStatus(Base):
    __tablename__ = "jurisdiction_coverage_status"
    __table_args__ = (
        UniqueConstraint("org_id", "state", "county", "city", name="uq_jurisdiction_coverage_status_scope"),
        Index("ix_jurisdiction_coverage_status_scope", "state", "county", "city"),
        Index("ix_jurisdiction_coverage_status_completeness_status", "completeness_status"),
        Index("ix_jurisdiction_coverage_status_is_stale", "is_stale"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)

    state: Mapped[str] = mapped_column(String(2), nullable=False, default="MI")
    county: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    city: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)

    completeness_status: Mapped[str] = mapped_column(String(40), nullable=False, default="unknown")
    completeness_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    confidence_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    is_stale: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    stale_reason: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    missing_categories_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    unresolved_items_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    latest_rule_version: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

    last_verified_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_refresh_started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_refresh_success_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_refresh_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    metadata_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class PropertyComplianceProjection(Base):
    __tablename__ = "property_compliance_projections"
    __table_args__ = (
        Index("ix_property_compliance_projections_org_property", "org_id", "property_id"),
        Index("ix_property_compliance_projections_rules_version", "rules_version"),
        Index("ix_property_compliance_projections_is_current", "is_current"),
        Index("ix_property_compliance_projections_jurisdiction_slug", "jurisdiction_slug"),
        Index("ix_property_compliance_projections_last_rule_change_at", "last_rule_change_at"),
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

    jurisdiction_slug: Mapped[Optional[str]] = mapped_column(String(160), nullable=True)
    program_type: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)
    rules_version: Mapped[str] = mapped_column(String(64), nullable=False, default="v1")
    projection_status: Mapped[str] = mapped_column(String(40), nullable=False, default="pending")
    projection_basis_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")

    blocking_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    unknown_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    stale_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    conflicting_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    evidence_gap_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    confirmed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    inferred_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failing_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    readiness_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    projected_compliance_cost: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    projected_days_to_rent: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    confidence_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    impacted_rules_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    unresolved_evidence_gaps_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    source_confidence_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    projection_reason_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")

    rules_effective_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_rule_change_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_projected_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    superseded_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=sa.text("now()"),
    )
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime,
        nullable=True,
        onupdate=datetime.utcnow,
    )


class PropertyComplianceProjectionItem(Base):
    __tablename__ = "property_compliance_projection_items"
    __table_args__ = (
        Index("ix_property_compliance_projection_items_projection", "projection_id"),
        Index("ix_property_compliance_projection_items_org_property", "org_id", "property_id"),
        Index("ix_property_compliance_projection_items_rule_key", "rule_key"),
        Index("ix_property_compliance_projection_items_evaluation_status", "evaluation_status"),
        Index("ix_property_compliance_projection_items_proof_state", "proof_state"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("organizations.id"),
        nullable=False,
        index=True,
    )
    projection_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("property_compliance_projections.id", ondelete="CASCADE"),
        nullable=False,
    )
    property_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("properties.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    policy_assertion_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("policy_assertions.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    jurisdiction_slug: Mapped[Optional[str]] = mapped_column(String(160), nullable=True)
    program_type: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)
    property_type: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    source_level: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)

    rule_key: Mapped[str] = mapped_column(String(120), nullable=False)
    rule_category: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    required: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    blocking: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    evaluation_status: Mapped[str] = mapped_column(String(40), nullable=False, default="unknown")
    evidence_status: Mapped[str] = mapped_column(String(40), nullable=False, default="unknown")
    proof_state: Mapped[str] = mapped_column(String(40), nullable=False, default="unknown")
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    estimated_cost: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    estimated_days: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    evidence_summary: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    evidence_gap: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_citation: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    raw_excerpt: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    rule_value_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    resolution_detail_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    conflicting_evidence_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    required_document_kind: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    last_evaluated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    evidence_updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=sa.text("now()"),
    )


class PropertyComplianceEvidence(Base):
    __tablename__ = "property_compliance_evidence"
    __table_args__ = (
        Index("ix_property_compliance_evidence_org_property", "org_id", "property_id"),
        Index("ix_property_compliance_evidence_projection_item", "projection_item_id"),
        Index("ix_property_compliance_evidence_policy_assertion", "policy_assertion_id"),
        Index("ix_property_compliance_evidence_status", "evidence_status", "proof_state"),
        Index("ix_property_compliance_evidence_expires_at", "expires_at"),
        Index("ix_property_compliance_evidence_verified_at", "verified_at"),
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
    projection_item_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("property_compliance_projection_items.id", ondelete="SET NULL"),
        nullable=True,
    )
    policy_assertion_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("policy_assertions.id", ondelete="SET NULL"),
        nullable=True,
    )
    compliance_document_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("compliance_documents.id", ondelete="SET NULL"),
        nullable=True,
    )
    inspection_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("inspections.id", ondelete="SET NULL"),
        nullable=True,
    )
    checklist_item_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("property_checklist_items.id", ondelete="SET NULL"),
        nullable=True,
    )

    evidence_source_type: Mapped[str] = mapped_column(String(40), nullable=False, default="document")
    evidence_category: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    evidence_key: Mapped[Optional[str]] = mapped_column(String(160), nullable=True)
    evidence_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    evidence_status: Mapped[str] = mapped_column(String(40), nullable=False, default="unknown")
    proof_state: Mapped[str] = mapped_column(String(40), nullable=False, default="inferred")
    satisfies_rule: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    observed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    verified_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    verified_by_user_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    invalidated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    invalidated_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    source_details_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    metadata_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=sa.text("now()"),
    )
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime,
        nullable=True,
        onupdate=datetime.utcnow,
    )


HqsAddendum = HqsAddendumRule