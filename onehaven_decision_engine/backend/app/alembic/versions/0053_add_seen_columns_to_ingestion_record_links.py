"""0053_add_seen_columns_to_ingestion_record_links

Revision ID: 0053_add_seen_columns_to_ingestion_record_links
Revises: 0052_add_ingestion_tables
Create Date: 2026-03-17
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0053_add_seen_columns_to_ingestion_record_links"
down_revision = "0052_add_ingestion_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "ingestion_record_links",
        sa.Column(
            "first_seen_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
    )
    op.add_column(
        "ingestion_record_links",
        sa.Column(
            "last_seen_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
    )


def downgrade() -> None:
    op.drop_column("ingestion_record_links", "last_seen_at")
    op.drop_column("ingestion_record_links", "first_seen_at")