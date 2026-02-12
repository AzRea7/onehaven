"""underwriting_results meta columns

Revision ID: 0009_add_underwriting_result_meta
Revises: 0008_ops_and_agents
Create Date: 2026-02-12
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0009_add_underwriting_result_meta"
down_revision = "0008_ops_and_agents"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # These exist in the app layer (schemas/models) but were missing in DB
    # which causes: UndefinedColumn: underwriting_results.decision_version does not exist

    op.add_column(
        "underwriting_results",
        sa.Column("decision_version", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "underwriting_results",
        sa.Column("payment_standard_pct_used", sa.Float(), nullable=True),
    )
    op.add_column(
        "underwriting_results",
        sa.Column("jurisdiction_multiplier", sa.Float(), nullable=True),
    )
    op.add_column(
        "underwriting_results",
        sa.Column("jurisdiction_reasons_json", sa.Text(), nullable=True),
    )
    op.add_column(
        "underwriting_results",
        sa.Column("rent_cap_reason", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "underwriting_results",
        sa.Column("fmr_adjusted", sa.Float(), nullable=True),
    )

    # Optional but helpful indexes if you query these often:
    # (uncomment if needed; safe to omit)
    # op.create_index("ix_underwriting_results_decision_version", "underwriting_results", ["decision_version"])


def downgrade() -> None:
    # op.drop_index("ix_underwriting_results_decision_version", table_name="underwriting_results")
    op.drop_column("underwriting_results", "fmr_adjusted")
    op.drop_column("underwriting_results", "rent_cap_reason")
    op.drop_column("underwriting_results", "jurisdiction_reasons_json")
    op.drop_column("underwriting_results", "jurisdiction_multiplier")
    op.drop_column("underwriting_results", "payment_standard_pct_used")
    op.drop_column("underwriting_results", "decision_version")
