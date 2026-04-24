# onehaven_decision_engine/backend/app/alembic/versions/0036_add_geo_risk_workflow_pipeline.py
"""0036 add geo/risk fields + workflow pipeline stages

Revision ID: 0036_geo_risk_pipe
Revises: 0035_ops_add_org_id_transactions_valuations
Create Date: 2026-03-04
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0036_geo_risk_pipe"
down_revision = "0035_ops_org_tx_vals"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # -----------------------------
    # Property geo + risk metadata
    # -----------------------------
    op.add_column("properties", sa.Column("lat", sa.Float(), nullable=True))
    op.add_column("properties", sa.Column("lng", sa.Float(), nullable=True))
    op.add_column("properties", sa.Column("county", sa.String(length=80), nullable=True))

    op.add_column("properties", sa.Column("is_red_zone", sa.Boolean(), nullable=False, server_default=sa.text("false")))
    op.add_column("properties", sa.Column("crime_density", sa.Float(), nullable=True))
    op.add_column("properties", sa.Column("crime_score", sa.Float(), nullable=True))
    op.add_column("properties", sa.Column("offender_count", sa.Integer(), nullable=True))

    op.create_index("ix_properties_county", "properties", ["county"], unique=False)
    op.create_index("ix_properties_is_red_zone", "properties", ["is_red_zone"], unique=False)

    # -----------------------------
    # PropertyState: no schema change required if it stores string stages.
    # But make sure current_stage column is wide enough (defensive).
    # -----------------------------
    # Some older schemas used String(32). We widen to 64 to hold new stage names safely.
    with op.batch_alter_table("property_states") as batch:
        try:
            batch.alter_column("current_stage", type_=sa.String(length=64), existing_type=sa.String(length=32))
        except Exception:
            # If it was already wide or different, ignore.
            pass


def downgrade() -> None:
    with op.batch_alter_table("property_states") as batch:
        try:
            batch.alter_column("current_stage", type_=sa.String(length=32), existing_type=sa.String(length=64))
        except Exception:
            pass

    op.drop_index("ix_properties_is_red_zone", table_name="properties")
    op.drop_index("ix_properties_county", table_name="properties")

    op.drop_column("properties", "offender_count")
    op.drop_column("properties", "crime_score")
    op.drop_column("properties", "crime_density")
    op.drop_column("properties", "is_red_zone")

    op.drop_column("properties", "county")
    op.drop_column("properties", "lng")
    op.drop_column("properties", "lat")