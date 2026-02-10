"""compliance + rent policy fields

Revision ID: 0006_compliance_and_rent_policy_fields
Revises: 0005_api_usage
Create Date: 2026-02-10
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0006_compliance_and_rent_policy_fields"
down_revision = "0005_api_usage"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # rent_assumptions: rent_used
    op.add_column("rent_assumptions", sa.Column("rent_used", sa.Float(), nullable=True))

    # jurisdiction_rules: jurisdiction_type, inspection_frequency, notes
    op.add_column(
        "jurisdiction_rules",
        sa.Column("jurisdiction_type", sa.String(length=20), nullable=False, server_default="city"),
    )
    op.add_column("jurisdiction_rules", sa.Column("inspection_frequency", sa.String(length=40), nullable=True))
    op.add_column("jurisdiction_rules", sa.Column("notes", sa.Text(), nullable=True))

    # inspection_items: resolved_at, resolution_notes
    op.add_column("inspection_items", sa.Column("resolved_at", sa.DateTime(), nullable=True))
    op.add_column("inspection_items", sa.Column("resolution_notes", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("inspection_items", "resolution_notes")
    op.drop_column("inspection_items", "resolved_at")

    op.drop_column("jurisdiction_rules", "notes")
    op.drop_column("jurisdiction_rules", "inspection_frequency")
    op.drop_column("jurisdiction_rules", "jurisdiction_type")

    op.drop_column("rent_assumptions", "rent_used")
