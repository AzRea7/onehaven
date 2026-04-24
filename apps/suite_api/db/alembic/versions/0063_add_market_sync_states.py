"""add market sync cursor state table

Revision ID: 0063_add_market_sync_states
Revises: 0062_acq_doc_workflow
Create Date: 2026-03-26
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0063_add_market_sync_states"
down_revision = "0062_acq_doc_workflow"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "market_sync_states",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("source_id", sa.Integer(), nullable=False),
        sa.Column("provider", sa.String(length=50), nullable=False),
        sa.Column("market_slug", sa.String(length=120), nullable=False),
        sa.Column("state", sa.String(length=2), nullable=False, server_default=sa.text("'MI'")),
        sa.Column("county", sa.String(length=80), nullable=True),
        sa.Column("city", sa.String(length=120), nullable=True),
        sa.Column("status", sa.String(length=30), nullable=False, server_default=sa.text("'idle'")),
        sa.Column("cursor_json", sa.JSON(), nullable=True),
        sa.Column("last_page", sa.Integer(), nullable=True),
        sa.Column("last_shard", sa.Integer(), nullable=True),
        sa.Column("last_sort_mode", sa.String(length=40), nullable=True),
        sa.Column("last_requested_limit", sa.Integer(), nullable=True),
        sa.Column("last_sync_started_at", sa.DateTime(), nullable=True),
        sa.Column("last_sync_completed_at", sa.DateTime(), nullable=True),
        sa.Column("last_seen_provider_record_at", sa.DateTime(), nullable=True),
        sa.Column("last_page_fingerprint", sa.String(length=128), nullable=True),
        sa.Column("market_exhausted", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("backfill_completed_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"]),
        sa.ForeignKeyConstraint(["source_id"], ["ingestion_sources.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("org_id", "source_id", "market_slug", name="uq_market_sync_states_org_source_market"),
    )

    op.create_index(
        "ix_market_sync_states_org_market",
        "market_sync_states",
        ["org_id", "market_slug"],
        unique=False,
    )
    op.create_index(
        "ix_market_sync_states_org_provider_market",
        "market_sync_states",
        ["org_id", "provider", "market_slug"],
        unique=False,
    )
    op.create_index(
        "ix_market_sync_states_org_id",
        "market_sync_states",
        ["org_id"],
        unique=False,
    )
    op.create_index(
        "ix_market_sync_states_source_id",
        "market_sync_states",
        ["source_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_market_sync_states_source_id", table_name="market_sync_states")
    op.drop_index("ix_market_sync_states_org_id", table_name="market_sync_states")
    op.drop_index("ix_market_sync_states_org_provider_market", table_name="market_sync_states")
    op.drop_index("ix_market_sync_states_org_market", table_name="market_sync_states")
    op.drop_table("market_sync_states")