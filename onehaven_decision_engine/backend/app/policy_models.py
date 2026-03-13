from __future__ import annotations

from datetime import date, datetime
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import (
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Index,
    Boolean,
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

    created_at: Mapped[DateTime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    updated_at: Mapped[DateTime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

class JurisdictionProfile(Base):
    __tablename__ = "jurisdiction_profiles"
    __table_args__ = (
        UniqueConstraint("org_id", "state", "county", "city", name="uq_jp_scope_state_county_city"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("organizations.id"), index=True, nullable=True
    )

    state: Mapped[str] = mapped_column(String(2), nullable=False, default="MI")
    county: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    city: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)

    friction_multiplier: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    pha_name: Mapped[Optional[str]] = mapped_column(String(180), nullable=True)
    policy_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=sa.text("now()")
    )
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, nullable=True, onupdate=datetime.utcnow
    )


class HqsRule(Base):
    __tablename__ = "hqs_rules"
    __table_args__ = (UniqueConstraint("code", name="uq_hqs_rules_code"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    category: Mapped[str] = mapped_column(String(40), nullable=False)
    severity: Mapped[str] = mapped_column(String(20), nullable=False, default="fail")
    description: Mapped[str] = mapped_column(String(260), nullable=False)

    evidence_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    remediation_hints_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_urls_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    effective_date: Mapped[date] = mapped_column(Date, nullable=False, default=date(2026, 1, 1))


class HqsAddendumRule(Base):
    __tablename__ = "hqs_addendum_rules"
    __table_args__ = (
        UniqueConstraint("jurisdiction_profile_id", "code", name="uq_hqs_addendum_jp_code"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("organizations.id"), index=True, nullable=False
    )

    jurisdiction_profile_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("jurisdiction_profiles.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )

    code: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    category: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)
    severity: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    description: Mapped[Optional[str]] = mapped_column(String(260), nullable=True)
    evidence_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    remediation_hints_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    effective_date: Mapped[date] = mapped_column(Date, nullable=False, default=date(2026, 1, 1))
    source_urls_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class HudFmrRecord(Base):
    __tablename__ = "hud_fmr_records"
    __table_args__ = (
        UniqueConstraint("state", "area_name", "year", "bedrooms", name="uq_hud_fmr_key"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    state: Mapped[str] = mapped_column(String(2), nullable=False, index=True)
    area_name: Mapped[str] = mapped_column(String(180), nullable=False, index=True)
    year: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    bedrooms: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    fmr: Mapped[float] = mapped_column(Float, nullable=False)
    source: Mapped[str] = mapped_column(String(80), nullable=False, default="hud_user_api")
    fetched_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    raw_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class PolicySource(Base):
    __tablename__ = "policy_sources"
    __table_args__ = (
        Index("ix_policy_sources_state_county_city", "state", "county", "city"),
        Index("ix_policy_sources_org_state", "org_id", "state"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    org_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("organizations.id"), index=True, nullable=True
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

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=sa.text("now()")
    )


class PolicySourceVersion(Base):
    __tablename__ = "policy_source_versions"
    __table_args__ = (
        Index("ix_policy_source_versions_source_retrieved", "source_id", "retrieved_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("policy_sources.id", ondelete="CASCADE"), index=True, nullable=False
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
        DateTime, nullable=False, server_default=sa.text("now()")
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
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    org_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("organizations.id"), index=True, nullable=True
    )

    source_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("policy_sources.id", ondelete="SET NULL"), index=True, nullable=True
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
        Integer, ForeignKey("policy_assertions.id", ondelete="SET NULL"), nullable=True
    )

    extracted_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=sa.text("now()")
    )


class JurisdictionCoverageStatus(Base):
    __tablename__ = "jurisdiction_coverage_status"
    __table_args__ = (
        Index("ix_jurisdiction_coverage_scope", "state", "county", "city"),
        Index("ix_jurisdiction_coverage_status", "coverage_status", "production_readiness"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("organizations.id"), nullable=True, index=True
    )

    state: Mapped[str] = mapped_column(String(2), nullable=False, default="MI")
    county: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    city: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    pha_name: Mapped[Optional[str]] = mapped_column(String(180), nullable=True)

    coverage_status: Mapped[str] = mapped_column(String(40), nullable=False, default="not_started")
    production_readiness: Mapped[str] = mapped_column(String(40), nullable=False, default="partial")

    last_reviewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_source_refresh_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    verified_rule_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    source_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    fetch_failure_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    stale_warning_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=sa.text("now()")
    )
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, nullable=True, onupdate=datetime.utcnow
    )


HqsAddendum = HqsAddendumRule
