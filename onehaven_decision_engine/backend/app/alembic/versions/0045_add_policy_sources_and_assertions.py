"""
0045 add policy_sources + policy_assertions (collector/extractor pipeline)

Revision ID: 0045_add_policy_sources_and_assertions
Revises: 0044_fix_jp_created_at_default
Create Date: 2026-03-05
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0045_add_policy_sources_and_assertions"
down_revision = "0044_fix_jp_created_at_default"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "policy_sources",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), sa.ForeignKey("organizations.id"), nullable=True, index=True),

        sa.Column("state", sa.String(length=2), nullable=True),
        sa.Column("county", sa.String(length=80), nullable=True),
        sa.Column("city", sa.String(length=120), nullable=True),

        sa.Column("pha_name", sa.String(length=180), nullable=True),
        sa.Column("program_type", sa.String(length=40), nullable=True),

        sa.Column("publisher", sa.String(length=180), nullable=True),
        sa.Column("title", sa.String(length=260), nullable=True),

        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("content_type", sa.String(length=120), nullable=True),
        sa.Column("http_status", sa.Integer(), nullable=True),

        sa.Column("retrieved_at", sa.DateTime(), nullable=False),
        sa.Column("content_sha256", sa.String(length=64), nullable=True),

        sa.Column("raw_path", sa.Text(), nullable=True),
        sa.Column("extracted_text", sa.Text(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),

        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
    )

    op.create_index("ix_policy_sources_state_county_city", "policy_sources", ["state", "county", "city"])
    op.create_index("ix_policy_sources_org_state", "policy_sources", ["org_id", "state"])

    op.create_table(
        "policy_assertions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), sa.ForeignKey("organizations.id"), nullable=True, index=True),
        sa.Column("source_id", sa.Integer(), sa.ForeignKey("policy_sources.id", ondelete="SET NULL"), nullable=True, index=True),

        sa.Column("state", sa.String(length=2), nullable=True),
        sa.Column("county", sa.String(length=80), nullable=True),
        sa.Column("city", sa.String(length=120), nullable=True),

        sa.Column("pha_name", sa.String(length=180), nullable=True),
        sa.Column("program_type", sa.String(length=40), nullable=True),

        sa.Column("rule_key", sa.String(length=120), nullable=False),
        sa.Column("value_json", sa.Text(), nullable=True),

        sa.Column("confidence", sa.Float(), nullable=False, server_default="0.25"),
        sa.Column("review_status", sa.String(length=40), nullable=False, server_default="extracted"),
        sa.Column("review_notes", sa.Text(), nullable=True),

        sa.Column("extracted_at", sa.DateTime(), nullable=False),
        sa.Column("reviewed_at", sa.DateTime(), nullable=True),

        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
    )

    op.create_index("ix_policy_assertions_scope", "policy_assertions", ["state", "county", "city"])
    op.create_index("ix_policy_assertions_status", "policy_assertions", ["review_status"])
    op.create_index("ix_policy_assertions_rule_key", "policy_assertions", ["rule_key"])


def downgrade() -> None:
    op.drop_index("ix_policy_assertions_rule_key", table_name="policy_assertions")
    op.drop_index("ix_policy_assertions_status", table_name="policy_assertions")
    op.drop_index("ix_policy_assertions_scope", table_name="policy_assertions")
    op.drop_table("policy_assertions")

    op.drop_index("ix_policy_sources_org_state", table_name="policy_sources")
    op.drop_index("ix_policy_sources_state_county_city", table_name="policy_sources")
    op.drop_table("policy_sources")
    