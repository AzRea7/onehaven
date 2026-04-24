"""rent learning: comps + observations + calibration

Revision ID: 0004_rent_learning
Revises: 0003_imports
Create Date: 2026-02-10
"""
from alembic import op
import sqlalchemy as sa


revision = "0004_rent_learning"
down_revision = "0003_imports"
branch_labels = None
depends_on = None


def upgrade():
    # ---- rent comps (rent reasonableness evidence) ----
    op.create_table(
        "rent_comps",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("property_id", sa.Integer(), sa.ForeignKey("properties.id", ondelete="CASCADE"), nullable=False),
        sa.Column("source", sa.String(length=40), nullable=False, server_default="manual"),
        sa.Column("address", sa.String(length=255), nullable=True),
        sa.Column("url", sa.Text(), nullable=True),
        sa.Column("rent", sa.Float(), nullable=False),
        sa.Column("bedrooms", sa.Integer(), nullable=True),
        sa.Column("bathrooms", sa.Float(), nullable=True),
        sa.Column("square_feet", sa.Integer(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_rent_comps_property_id", "rent_comps", ["property_id"])

    # ---- achieved rent observations (the feedback loop) ----
    op.create_table(
        "rent_observations",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("property_id", sa.Integer(), sa.ForeignKey("properties.id", ondelete="CASCADE"), nullable=False),
        sa.Column("strategy", sa.String(length=20), nullable=False),  # "section8" | "market"
        sa.Column("achieved_rent", sa.Float(), nullable=False),
        sa.Column("tenant_portion", sa.Float(), nullable=True),
        sa.Column("hap_portion", sa.Float(), nullable=True),
        sa.Column("lease_start", sa.DateTime(), nullable=True),
        sa.Column("lease_end", sa.DateTime(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_rent_obs_property_id", "rent_observations", ["property_id"])
    op.create_index("ix_rent_obs_strategy", "rent_observations", ["strategy"])

    # ---- calibration table (simple learning state) ----
    op.create_table(
        "rent_calibrations",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("zip", sa.String(length=10), nullable=False),
        sa.Column("bedrooms", sa.Integer(), nullable=False),
        sa.Column("strategy", sa.String(length=20), nullable=False),
        sa.Column("multiplier", sa.Float(), nullable=False, server_default="1.0"),
        sa.Column("samples", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("mape", sa.Float(), nullable=True),  # mean absolute percent error (tracked)
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("zip", "bedrooms", "strategy", name="uq_rent_calibration_key"),
    )
    op.create_index("ix_rent_cal_zip_bed_strat", "rent_calibrations", ["zip", "bedrooms", "strategy"])


def downgrade():
    op.drop_index("ix_rent_cal_zip_bed_strat", table_name="rent_calibrations")
    op.drop_table("rent_calibrations")

    op.drop_index("ix_rent_obs_strategy", table_name="rent_observations")
    op.drop_index("ix_rent_obs_property_id", table_name="rent_observations")
    op.drop_table("rent_observations")

    op.drop_index("ix_rent_comps_property_id", table_name="rent_comps")
    op.drop_table("rent_comps")
