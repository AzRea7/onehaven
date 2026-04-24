"""add compliance logging tables

Revision ID: 0002_compliance
Revises: 0001_init
Create Date: 2026-02-09
"""
from alembic import op
import sqlalchemy as sa


revision = "0002_compliance"
down_revision = "0001_init"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "inspectors",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=180), nullable=False),
        sa.Column("agency", sa.String(length=180), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("name", "agency", name="uq_inspector_name_agency"),
    )

    op.create_table(
        "inspections",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("property_id", sa.Integer(), sa.ForeignKey("properties.id", ondelete="CASCADE"), nullable=False),
        sa.Column("inspector_id", sa.Integer(), sa.ForeignKey("inspectors.id", ondelete="SET NULL"), nullable=True),
        sa.Column("inspection_date", sa.DateTime(), nullable=False),
        sa.Column("passed", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("reinspect_required", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )

    op.create_table(
        "inspection_items",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("inspection_id", sa.Integer(), sa.ForeignKey("inspections.id", ondelete="CASCADE"), nullable=False),
        sa.Column("code", sa.String(length=80), nullable=False),
        sa.Column("failed", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("severity", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("location", sa.String(length=180), nullable=True),
        sa.Column("details", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("inspection_id", "code", name="uq_inspection_item_per_code"),
    )


def downgrade():
    op.drop_table("inspection_items")
    op.drop_table("inspections")
    op.drop_table("inspectors")
