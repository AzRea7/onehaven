# backend/app/alembic/versions/0040_policy_models_bootstrap.py
"""
0040 bootstrap policy models tables (jurisdiction_profiles, hqs_rules, hqs_addendum_rules, hud_fmr_records)

Revision ID: 0040_policy_models_bootstrap
Revises: 0039_safe_add_org_updated_at
Create Date: 2026-03-05
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "0040_policy_models_bootstrap"
down_revision = "0039_safe_add_org_updated_at"
branch_labels = None
depends_on = None


def _insp():
    return inspect(op.get_bind())


def _has_table(name: str) -> bool:
    return name in _insp().get_table_names()


def _has_index(table: str, index_name: str) -> bool:
    if not _has_table(table):
        return False
    idx = {i["name"] for i in _insp().get_indexes(table)}
    return index_name in idx


def _has_column(table: str, col: str) -> bool:
    if not _has_table(table):
        return False
    cols = {c["name"] for c in _insp().get_columns(table)}
    return col in cols


def upgrade() -> None:
    # ============================================================
    # 1) jurisdiction_profiles (simple override model)
    # ============================================================
    if not _has_table("jurisdiction_profiles"):
        op.create_table(
            "jurisdiction_profiles",
            sa.Column("id", sa.Integer(), primary_key=True),

            # NULL => global default row
            sa.Column("org_id", sa.Integer(), sa.ForeignKey("organizations.id"), nullable=True),

            sa.Column("state", sa.String(length=2), nullable=False, server_default="MI"),
            sa.Column("county", sa.String(length=80), nullable=True),
            sa.Column("city", sa.String(length=120), nullable=True),

            sa.Column("friction_multiplier", sa.Float(), nullable=False, server_default="1.0"),
            sa.Column("pha_name", sa.String(length=180), nullable=True),
            sa.Column("policy_json", sa.Text(), nullable=True),
            sa.Column("notes", sa.Text(), nullable=True),

            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.Column("updated_at", sa.DateTime(), nullable=True),

            sa.UniqueConstraint("org_id", "state", "county", "city", name="uq_jp_scope_state_county_city"),
        )

        if not _has_index("jurisdiction_profiles", "ix_jurisdiction_profiles_org_id"):
            op.create_index("ix_jurisdiction_profiles_org_id", "jurisdiction_profiles", ["org_id"])
        if not _has_index("jurisdiction_profiles", "ix_jurisdiction_profiles_state"):
            op.create_index("ix_jurisdiction_profiles_state", "jurisdiction_profiles", ["state"])
        if not _has_index("jurisdiction_profiles", "ix_jurisdiction_profiles_county"):
            op.create_index("ix_jurisdiction_profiles_county", "jurisdiction_profiles", ["county"])
        if not _has_index("jurisdiction_profiles", "ix_jurisdiction_profiles_city"):
            op.create_index("ix_jurisdiction_profiles_city", "jurisdiction_profiles", ["city"])
    else:
        # Add-only repair
        with op.batch_alter_table("jurisdiction_profiles") as batch:
            if not _has_column("jurisdiction_profiles", "org_id"):
                batch.add_column(sa.Column("org_id", sa.Integer(), nullable=True))
            if not _has_column("jurisdiction_profiles", "state"):
                batch.add_column(sa.Column("state", sa.String(length=2), nullable=True))
            if not _has_column("jurisdiction_profiles", "county"):
                batch.add_column(sa.Column("county", sa.String(length=80), nullable=True))
            if not _has_column("jurisdiction_profiles", "city"):
                batch.add_column(sa.Column("city", sa.String(length=120), nullable=True))
            if not _has_column("jurisdiction_profiles", "friction_multiplier"):
                batch.add_column(sa.Column("friction_multiplier", sa.Float(), nullable=True))
            if not _has_column("jurisdiction_profiles", "pha_name"):
                batch.add_column(sa.Column("pha_name", sa.String(length=180), nullable=True))
            if not _has_column("jurisdiction_profiles", "policy_json"):
                batch.add_column(sa.Column("policy_json", sa.Text(), nullable=True))
            if not _has_column("jurisdiction_profiles", "notes"):
                batch.add_column(sa.Column("notes", sa.Text(), nullable=True))
            if not _has_column("jurisdiction_profiles", "created_at"):
                batch.add_column(sa.Column("created_at", sa.DateTime(), nullable=True))
            if not _has_column("jurisdiction_profiles", "updated_at"):
                batch.add_column(sa.Column("updated_at", sa.DateTime(), nullable=True))

    # =====================================
    # 2) hqs_rules
    # =====================================
    if not _has_table("hqs_rules"):
        op.create_table(
            "hqs_rules",
            sa.Column("id", sa.Integer(), primary_key=True),

            sa.Column("code", sa.String(length=80), nullable=False),
            sa.Column("category", sa.String(length=40), nullable=False),
            sa.Column("severity", sa.String(length=20), nullable=False, server_default="fail"),
            sa.Column("description", sa.String(length=260), nullable=False),

            sa.Column("evidence_json", sa.Text(), nullable=True),
            sa.Column("remediation_hints_json", sa.Text(), nullable=True),

            sa.Column("source_urls_json", sa.Text(), nullable=True),
            sa.Column("effective_date", sa.Date(), nullable=False, server_default=sa.text("'2026-01-01'::date")),

            sa.UniqueConstraint("code", name="uq_hqs_rules_code"),
        )
        if not _has_index("hqs_rules", "ix_hqs_rules_code"):
            op.create_index("ix_hqs_rules_code", "hqs_rules", ["code"])

    # =====================================
    # 3) hqs_addendum_rules
    # =====================================
    if not _has_table("hqs_addendum_rules"):
        op.create_table(
            "hqs_addendum_rules",
            sa.Column("id", sa.Integer(), primary_key=True),

            sa.Column("org_id", sa.Integer(), sa.ForeignKey("organizations.id"), nullable=False),
            sa.Column(
                "jurisdiction_profile_id",
                sa.Integer(),
                sa.ForeignKey("jurisdiction_profiles.id", ondelete="CASCADE"),
                nullable=False,
            ),

            sa.Column("code", sa.String(length=80), nullable=False),
            sa.Column("category", sa.String(length=40), nullable=True),
            sa.Column("severity", sa.String(length=20), nullable=True),
            sa.Column("description", sa.String(length=260), nullable=True),

            sa.Column("evidence_json", sa.Text(), nullable=True),
            sa.Column("remediation_hints_json", sa.Text(), nullable=True),

            sa.Column("effective_date", sa.Date(), nullable=False, server_default=sa.text("'2026-01-01'::date")),
            sa.Column("source_urls_json", sa.Text(), nullable=True),

            sa.UniqueConstraint("jurisdiction_profile_id", "code", name="uq_hqs_addendum_jp_code"),
        )
        if not _has_index("hqs_addendum_rules", "ix_hqs_addendum_rules_org_id"):
            op.create_index("ix_hqs_addendum_rules_org_id", "hqs_addendum_rules", ["org_id"])
        if not _has_index("hqs_addendum_rules", "ix_hqs_addendum_rules_jurisdiction_profile_id"):
            op.create_index(
                "ix_hqs_addendum_rules_jurisdiction_profile_id",
                "hqs_addendum_rules",
                ["jurisdiction_profile_id"],
            )
        if not _has_index("hqs_addendum_rules", "ix_hqs_addendum_rules_code"):
            op.create_index("ix_hqs_addendum_rules_code", "hqs_addendum_rules", ["code"])

    # =====================================
    # 4) hud_fmr_records
    # =====================================
    if not _has_table("hud_fmr_records"):
        op.create_table(
            "hud_fmr_records",
            sa.Column("id", sa.Integer(), primary_key=True),

            sa.Column("state", sa.String(length=2), nullable=False),
            sa.Column("area_name", sa.String(length=180), nullable=False),
            sa.Column("year", sa.Integer(), nullable=False),
            sa.Column("bedrooms", sa.Integer(), nullable=False),

            sa.Column("fmr", sa.Float(), nullable=False),

            sa.Column("source", sa.String(length=80), nullable=False, server_default="hud_user_api"),
            sa.Column("fetched_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.Column("raw_json", sa.Text(), nullable=True),

            sa.UniqueConstraint("state", "area_name", "year", "bedrooms", name="uq_hud_fmr_key"),
        )
        if not _has_index("hud_fmr_records", "ix_hud_fmr_records_state"):
            op.create_index("ix_hud_fmr_records_state", "hud_fmr_records", ["state"])
        if not _has_index("hud_fmr_records", "ix_hud_fmr_records_area_name"):
            op.create_index("ix_hud_fmr_records_area_name", "hud_fmr_records", ["area_name"])
        if not _has_index("hud_fmr_records", "ix_hud_fmr_records_year"):
            op.create_index("ix_hud_fmr_records_year", "hud_fmr_records", ["year"])
        if not _has_index("hud_fmr_records", "ix_hud_fmr_records_bedrooms"):
            op.create_index("ix_hud_fmr_records_bedrooms", "hud_fmr_records", ["bedrooms"])


def downgrade() -> None:
    # Conservative: do not drop policy tables automatically.
    pass
