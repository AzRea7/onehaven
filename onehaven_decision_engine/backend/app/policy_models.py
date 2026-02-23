# backend/app/policy_models.py
from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from sqlalchemy import (
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from .db import Base


class JurisdictionProfile(Base):
    """
    Your versioned, source-backed truth for "how Section 8 is done here".
    This is NOT the same as JurisdictionRule friction scoring.
    This is: PHA + packet + inspection cadence + local overlays + utility handling.
    """

    __tablename__ = "jurisdiction_profiles"
    __table_args__ = (
        UniqueConstraint("org_id", "key", "effective_date", name="uq_jp_org_key_effective"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), index=True, nullable=False)

    # e.g. "mi_detroit_wayne_hud_hcv", "mi_royal_oak_oakland_hcv"
    key: Mapped[str] = mapped_column(String(120), nullable=False, index=True)

    # Human-friendly
    name: Mapped[str] = mapped_column(String(180), nullable=False)

    # Matching hints (lightweight, deterministic)
    state: Mapped[str] = mapped_column(String(2), nullable=False, default="MI")
    county: Mapped[Optional[str]] = mapped_column(String(80), nullable=True)
    city: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    zip_prefix: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)  # "482" or "48067"

    # PHA / Program identity
    pha_name: Mapped[Optional[str]] = mapped_column(String(180), nullable=True)
    pha_code: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)
    program_type: Mapped[str] = mapped_column(String(40), nullable=False, default="hcv")  # hcv|pbv|other

    # Payment standard knobs (varies by PHA; store what you know)
    payment_standard_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    uses_safmr: Mapped[bool] = mapped_column(Integer, nullable=False, default=0)  # 0/1 for sqlite friendliness

    # Inspection cadence and packet expectations (JSON stored as text for simplicity)
    # Store lists/dicts as JSON strings. Agents will treat as structured truth.
    inspection_cadence_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    packet_requirements_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    local_overlays_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    utility_allowance_notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Governance fields
    effective_date: Mapped[date] = mapped_column(Date, nullable=False)
    source_urls_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # list[str]
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    last_verified_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


class HqsRule(Base):
    """
    Canonical HQS-like item library: your "federal baseline" checklist.
    Local addenda override with HqsAddendumRule.
    """

    __tablename__ = "hqs_rules"
    __table_args__ = (UniqueConstraint("code", name="uq_hqs_rules_code"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # stable code you control, e.g. "HQS_SMOKE_DETECTOR"
    code: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    category: Mapped[str] = mapped_column(String(40), nullable=False)  # safety|electrical|plumbing|egress|interior|exterior|structure|sanitary|thermal

    # fail/advisory; agents treat "fail" as needs remediation before inspection readiness
    severity: Mapped[str] = mapped_column(String(20), nullable=False, default="fail")

    description: Mapped[str] = mapped_column(String(260), nullable=False)
    evidence_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # list[str]
    remediation_hints_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # list[str]

    # Governance
    source_urls_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    effective_date: Mapped[date] = mapped_column(Date, nullable=False, default=date(2026, 1, 1))


class HqsAddendumRule(Base):
    """
    Local addendum overlay for a jurisdiction profile.
    Use this to:
      - add extra items
      - override severity/description for an existing code
    """

    __tablename__ = "hqs_addendum_rules"
    __table_args__ = (
        UniqueConstraint("jurisdiction_profile_id", "code", name="uq_hqs_addendum_jp_code"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    org_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), index=True, nullable=False)

    jurisdiction_profile_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("jurisdiction_profiles.id", ondelete="CASCADE"), index=True, nullable=False
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
    Agents should NOT call HUD live during underwriting; they should use cache
    and create "needs_refresh" next-actions when stale.
    """

    __tablename__ = "hud_fmr_records"
    __table_args__ = (
        UniqueConstraint("state", "area_name", "year", "bedrooms", name="uq_hud_fmr_key"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # Matching keys (keep it flexible)
    state: Mapped[str] = mapped_column(String(2), nullable=False, index=True)
    area_name: Mapped[str] = mapped_column(String(180), nullable=False, index=True)  # e.g. "Detroit-Warren-Dearborn, MI HUD Metro FMR Area"
    year: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    bedrooms: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    fmr: Mapped[float] = mapped_column(Float, nullable=False)

    # provenance
    source: Mapped[str] = mapped_column(String(80), nullable=False, default="hud_user_api")
    fetched_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    raw_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


# -------------------------------------------------------------------
# Compatibility aliases (older imports)
# -------------------------------------------------------------------
# Some modules still import `HqsAddendum`. The canonical model is `HqsAddendumRule`.
HqsAddendum = HqsAddendumRule