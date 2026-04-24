"""0051_add_policy_catalog_entries

Revision ID: 0051_add_policy_catalog_entries
Revises: 0050_add_property_photos
Create Date: 2026-03-13
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0051_add_policy_catalog_entries"
down_revision = "0050_add_property_photos"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "policy_catalog_entries",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), nullable=True),
        sa.Column("state", sa.String(length=8), nullable=False, server_default="MI"),
        sa.Column("county", sa.String(length=120), nullable=True),
        sa.Column("city", sa.String(length=120), nullable=True),
        sa.Column("pha_name", sa.String(length=255), nullable=True),
        sa.Column("program_type", sa.String(length=64), nullable=True),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("publisher", sa.String(length=255), nullable=True),
        sa.Column("title", sa.String(length=500), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("source_kind", sa.String(length=120), nullable=True),
        sa.Column("is_authoritative", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("priority", sa.Integer(), nullable=False, server_default="100"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("is_override", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("baseline_url", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
    )

    op.create_index(
        "ix_policy_catalog_entries_market",
        "policy_catalog_entries",
        ["org_id", "state", "county", "city", "pha_name", "program_type", "is_active"],
        unique=False,
    )
    op.create_index(
        "ix_policy_catalog_entries_url",
        "policy_catalog_entries",
        ["url"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_policy_catalog_entries_url", table_name="policy_catalog_entries")
    op.drop_index("ix_policy_catalog_entries_market", table_name="policy_catalog_entries")
    op.drop_table("policy_catalog_entries")