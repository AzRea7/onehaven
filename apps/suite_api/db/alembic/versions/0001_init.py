"""init schema

Revision ID: 0001_init
Revises:
Create Date: 2026-02-09
"""
from alembic import op
import sqlalchemy as sa


revision = "0001_init"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "properties",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("address", sa.String(length=255), nullable=False),
        sa.Column("city", sa.String(length=120), nullable=False),
        sa.Column("state", sa.String(length=2), nullable=False),
        sa.Column("zip", sa.String(length=10), nullable=False),
        sa.Column("bedrooms", sa.Integer(), nullable=False),
        sa.Column("bathrooms", sa.Float(), nullable=False),
        sa.Column("square_feet", sa.Integer(), nullable=True),
        sa.Column("year_built", sa.Integer(), nullable=True),
        sa.Column("has_garage", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("property_type", sa.String(length=60), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )

    op.create_table(
        "deals",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("property_id", sa.Integer(), sa.ForeignKey("properties.id", ondelete="CASCADE"), nullable=False),
        sa.Column("source", sa.String(length=80), nullable=True),
        sa.Column("asking_price", sa.Float(), nullable=False),
        sa.Column("estimated_purchase_price", sa.Float(), nullable=True),
        sa.Column("rehab_estimate", sa.Float(), nullable=False, server_default="0"),
        sa.Column("financing_type", sa.String(length=40), nullable=False),
        sa.Column("interest_rate", sa.Float(), nullable=False),
        sa.Column("term_years", sa.Integer(), nullable=False),
        sa.Column("down_payment_pct", sa.Float(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )

    op.create_table(
        "rent_assumptions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("property_id", sa.Integer(), sa.ForeignKey("properties.id", ondelete="CASCADE"), nullable=False),
        sa.Column("market_rent_estimate", sa.Float(), nullable=True),
        sa.Column("section8_fmr", sa.Float(), nullable=True),
        sa.Column("approved_rent_ceiling", sa.Float(), nullable=True),
        sa.Column("rent_reasonableness_comp", sa.Float(), nullable=True),
        sa.Column("inventory_count", sa.Integer(), nullable=True),
        sa.Column("starbucks_minutes", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("property_id", name="uq_rent_assumptions_property"),
    )

    op.create_table(
        "jurisdiction_rules",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("city", sa.String(length=120), nullable=False),
        sa.Column("state", sa.String(length=2), nullable=False),
        sa.Column("rental_license_required", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("inspection_authority", sa.String(length=180), nullable=True),
        sa.Column("typical_fail_points_json", sa.Text(), nullable=True),
        sa.Column("registration_fee", sa.Float(), nullable=True),
        sa.Column("processing_days", sa.Integer(), nullable=True),
        sa.Column("tenant_waitlist_depth", sa.String(length=80), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("city", "state", name="uq_jurisdiction_city_state"),
    )

    op.create_table(
        "underwriting_results",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("deal_id", sa.Integer(), sa.ForeignKey("deals.id", ondelete="CASCADE"), nullable=False),
        sa.Column("decision", sa.String(length=12), nullable=False),
        sa.Column("score", sa.Integer(), nullable=False),
        sa.Column("reasons_json", sa.Text(), nullable=False),
        sa.Column("gross_rent_used", sa.Float(), nullable=False),
        sa.Column("mortgage_payment", sa.Float(), nullable=False),
        sa.Column("operating_expenses", sa.Float(), nullable=False),
        sa.Column("noi", sa.Float(), nullable=False),
        sa.Column("cash_flow", sa.Float(), nullable=False),
        sa.Column("dscr", sa.Float(), nullable=False),
        sa.Column("cash_on_cash", sa.Float(), nullable=False),
        sa.Column("break_even_rent", sa.Float(), nullable=False),
        sa.Column("min_rent_for_target_roi", sa.Float(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )


def downgrade():
    op.drop_table("underwriting_results")
    op.drop_table("jurisdiction_rules")
    op.drop_table("rent_assumptions")
    op.drop_table("deals")
    op.drop_table("properties")
