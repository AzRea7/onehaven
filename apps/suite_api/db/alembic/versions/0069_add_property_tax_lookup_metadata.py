"""add property tax lookup metadata

Revision ID: 0069_add_property_tax_lookup_metadata
Revises: 0068_add_property_tax_insurance_fields
Create Date: 2026-03-31
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0069_add_property_tax_lookup_metadata"
down_revision = "0068_add_property_tax_insurance_fields"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("properties", sa.Column("parcel_id", sa.String(length=128), nullable=True))
    op.add_column("properties", sa.Column("tax_lookup_status", sa.String(length=64), nullable=True))
    op.add_column("properties", sa.Column("tax_lookup_provider", sa.String(length=128), nullable=True))
    op.add_column("properties", sa.Column("tax_lookup_url", sa.String(length=2048), nullable=True))
    op.add_column("properties", sa.Column("tax_last_verified_at", sa.DateTime(), nullable=True))

    op.create_index("ix_properties_parcel_id", "properties", ["parcel_id"])
    op.create_index("ix_properties_tax_lookup_status", "properties", ["tax_lookup_status"])
    op.create_index("ix_properties_tax_lookup_provider", "properties", ["tax_lookup_provider"])


def downgrade() -> None:
    op.drop_index("ix_properties_tax_lookup_provider", table_name="properties")
    op.drop_index("ix_properties_tax_lookup_status", table_name="properties")
    op.drop_index("ix_properties_parcel_id", table_name="properties")

    op.drop_column("properties", "tax_last_verified_at")
    op.drop_column("properties", "tax_lookup_url")
    op.drop_column("properties", "tax_lookup_provider")
    op.drop_column("properties", "tax_lookup_status")
    op.drop_column("properties", "parcel_id")