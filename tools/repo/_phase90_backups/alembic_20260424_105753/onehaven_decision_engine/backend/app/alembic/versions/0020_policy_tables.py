# backend/app/alembic/versions/0020_policy_tables.py
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0020_policy_tables"
down_revision = "0019_agent_messages_link_run_id"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "jurisdiction_profiles",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), sa.ForeignKey("organizations.id"), nullable=False),
        sa.Column("key", sa.String(length=120), nullable=False),
        sa.Column("name", sa.String(length=180), nullable=False),
        sa.Column("state", sa.String(length=2), nullable=False, server_default="MI"),
        sa.Column("county", sa.String(length=80), nullable=True),
        sa.Column("city", sa.String(length=120), nullable=True),
        sa.Column("zip_prefix", sa.String(length=10), nullable=True),
        sa.Column("pha_name", sa.String(length=180), nullable=True),
        sa.Column("pha_code", sa.String(length=40), nullable=True),
        sa.Column("program_type", sa.String(length=40), nullable=False, server_default="hcv"),
        sa.Column("payment_standard_pct", sa.Float(), nullable=True),
        sa.Column("uses_safmr", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("inspection_cadence_json", sa.Text(), nullable=True),
        sa.Column("packet_requirements_json", sa.Text(), nullable=True),
        sa.Column("local_overlays_json", sa.Text(), nullable=True),
        sa.Column("utility_allowance_notes", sa.Text(), nullable=True),
        sa.Column("effective_date", sa.Date(), nullable=False),
        sa.Column("source_urls_json", sa.Text(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("last_verified_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("org_id", "key", "effective_date", name="uq_jp_org_key_effective"),
    )
    op.create_index("ix_jp_org_id", "jurisdiction_profiles", ["org_id"])
    op.create_index("ix_jp_key", "jurisdiction_profiles", ["key"])

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
        sa.Column("effective_date", sa.Date(), nullable=False),
        sa.UniqueConstraint("code", name="uq_hqs_rules_code"),
    )
    op.create_index("ix_hqs_code", "hqs_rules", ["code"])
    op.create_index("ix_hqs_category", "hqs_rules", ["category"])

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
        sa.Column("effective_date", sa.Date(), nullable=False),
        sa.Column("source_urls_json", sa.Text(), nullable=True),
        sa.UniqueConstraint("jurisdiction_profile_id", "code", name="uq_hqs_addendum_jp_code"),
    )
    op.create_index("ix_hqs_add_org", "hqs_addendum_rules", ["org_id"])
    op.create_index("ix_hqs_add_jp", "hqs_addendum_rules", ["jurisdiction_profile_id"])
    op.create_index("ix_hqs_add_code", "hqs_addendum_rules", ["code"])

    op.create_table(
        "hud_fmr_records",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("state", sa.String(length=2), nullable=False),
        sa.Column("area_name", sa.String(length=180), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("bedrooms", sa.Integer(), nullable=False),
        sa.Column("fmr", sa.Float(), nullable=False),
        sa.Column("source", sa.String(length=80), nullable=False, server_default="hud_user_api"),
        sa.Column("fetched_at", sa.DateTime(), nullable=False),
        sa.Column("raw_json", sa.Text(), nullable=True),
        sa.UniqueConstraint("state", "area_name", "year", "bedrooms", name="uq_hud_fmr_key"),
    )
    op.create_index("ix_fmr_state", "hud_fmr_records", ["state"])
    op.create_index("ix_fmr_area", "hud_fmr_records", ["area_name"])
    op.create_index("ix_fmr_year", "hud_fmr_records", ["year"])
    op.create_index("ix_fmr_beds", "hud_fmr_records", ["bedrooms"])


def downgrade():
    op.drop_index("ix_fmr_beds", table_name="hud_fmr_records")
    op.drop_index("ix_fmr_year", table_name="hud_fmr_records")
    op.drop_index("ix_fmr_area", table_name="hud_fmr_records")
    op.drop_index("ix_fmr_state", table_name="hud_fmr_records")
    op.drop_table("hud_fmr_records")

    op.drop_index("ix_hqs_add_code", table_name="hqs_addendum_rules")
    op.drop_index("ix_hqs_add_jp", table_name="hqs_addendum_rules")
    op.drop_index("ix_hqs_add_org", table_name="hqs_addendum_rules")
    op.drop_table("hqs_addendum_rules")

    op.drop_index("ix_hqs_category", table_name="hqs_rules")
    op.drop_index("ix_hqs_code", table_name="hqs_rules")
    op.drop_table("hqs_rules")

    op.drop_index("ix_jp_key", table_name="jurisdiction_profiles")
    op.drop_index("ix_jp_org_id", table_name="jurisdiction_profiles")
    op.drop_table("jurisdiction_profiles")