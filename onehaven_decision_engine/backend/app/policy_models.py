# backend/app/policy_models.py
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
)
from sqlalchemy.orm import Mapped, mapped_column

from .db import Base


class JurisdictionProfile(Base):
    """
    Jurisdiction Profiles = operational reality model:
      - global defaults (org_id is NULL)
      - org overrides (org_id = organizations.id)

    Matching specificity:
      city+state > county+state > state-only

    This model is intentionally simple:
      - friction_multiplier: the "time/complexity drag" factor
      - policy_json: structured notes your agents/UI can use
    """

    __tablename__ = "jurisdiction_profiles"
    __table_args__ = (
        # Note: Postgres treats NULLs as distinct in UNIQUE constraints, so duplicates
        # for global rows are possible. We also enforce uniqueness in the service logic.
        UniqueConstraint("org_id", "state", "county", "city", name="uq_jp_scope_state_county_city"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # NULL => global default row
    org_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("organizations.id"), index=True, nullable=True
    )

    state: Mapped[str] = mapped_column(String(2), nullable=False, default="MI")
    county: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    city: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)

    friction_multiplier: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)

    pha_name: Mapped[Optional[str]] = mapped_column(String(180), nullable=True)

    # store arbitrary structured policy blob as JSON string
    policy_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # DB-safe timestamps:
    # - created_at uses server_default now() so inserts outside ORM still get populated
    # - updated_at is set by service logic (and optionally by ORM onupdate)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=sa.text("now()")
    )
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, nullable=True, onupdate=datetime.utcnow
    )


class HqsRule(Base):
    """
    Canonical HQS-like item library: your "federal baseline" checklist.
    Local addenda override with HqsAddendumRule.
    """

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
    """
    Local addendum overlay for a jurisdiction profile.
    """

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
    """
    Cached HUD Fair Market Rent (FMR) / SAFMR-like records.
    """

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
    """
    Evidence store.
    A PolicySource is a fetched artifact (HTML/PDF/etc) with hash + metadata.
    """

    __tablename__ = "policy_sources"
    __table_args__ = (
        Index("ix_policy_sources_state_county_city", "state", "county", "city"),
        Index("ix_policy_sources_org_state", "org_id", "state"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # NULL => global evidence usable by all orgs
    org_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("organizations.id"), index=True, nullable=True
    )

    state: Mapped[Optional[str]] = mapped_column(String(2), nullable=True)
    county: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    city: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)

    pha_name: Mapped[Optional[str]] = mapped_column(String(180), nullable=True)
    program_type: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)  # "hcv", "pbv", etc.

    publisher: Mapped[Optional[str]] = mapped_column(String(180), nullable=True)
    title: Mapped[Optional[str]] = mapped_column(String(260), nullable=True)

    url: Mapped[str] = mapped_column(Text, nullable=False)
    content_type: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    http_status: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    retrieved_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    content_sha256: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

    # Where raw bytes/text are stored in container filesystem
    raw_path: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Light extracted text (optional). Keep small; PDFs usually empty here.
    extracted_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=sa.text("now()"))


class PolicyAssertion(Base):
    """
    Actionable statements derived from PolicySource, then human-reviewed.

    IMPORTANT:
    - Your underwriting logic should only read review_status="verified".
    - Everything else is draft.
    """

    __tablename__ = "policy_assertions"
    __table_args__ = (
        Index("ix_policy_assertions_scope", "state", "county", "city"),
        Index("ix_policy_assertions_status", "review_status"),
        Index("ix_policy_assertions_rule_key", "rule_key"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # NULL => global rule
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
    value_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.25)

    # "extracted" | "reviewed" | "verified" | "rejected"
    review_status: Mapped[str] = mapped_column(String(40), nullable=False, default="extracted")
    review_notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    extracted_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=sa.text("now()"))
# Compatibility alias (older imports)
HqsAddendum = HqsAddendumRule
