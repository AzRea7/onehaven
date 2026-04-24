"""add productionized crime and neighborhood risk fields

Revision ID: 0067_add_productionized_crime_risk_fields
Revises: 0066_add_property_listing_visibility_fields
Create Date: 2026-03-28 20:15:00.000000
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0067_add_productionized_crime_risk_fields"
down_revision = "0066_add_property_listing_visibility_fields"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # properties
    op.add_column("properties", sa.Column("crime_band", sa.String(length=20), nullable=True))
    op.add_column("properties", sa.Column("crime_source", sa.String(length=40), nullable=True))
    op.add_column("properties", sa.Column("crime_method", sa.String(length=80), nullable=True))
    op.add_column("properties", sa.Column("crime_radius_miles", sa.Float(), nullable=True))
    op.add_column("properties", sa.Column("crime_area_sq_miles", sa.Float(), nullable=True))
    op.add_column("properties", sa.Column("crime_area_type", sa.String(length=40), nullable=True))
    op.add_column("properties", sa.Column("crime_incident_count", sa.Integer(), nullable=True))
    op.add_column("properties", sa.Column("crime_weighted_incident_count", sa.Float(), nullable=True))
    op.add_column("properties", sa.Column("crime_nearest_incident_miles", sa.Float(), nullable=True))
    op.add_column("properties", sa.Column("crime_dataset_version", sa.String(length=80), nullable=True))
    op.add_column("properties", sa.Column("crime_confidence", sa.Float(), nullable=True))
    op.add_column("properties", sa.Column("investment_area_band", sa.String(length=20), nullable=True))
    op.add_column("properties", sa.Column("offender_band", sa.String(length=20), nullable=True))
    op.add_column("properties", sa.Column("offender_source", sa.String(length=40), nullable=True))
    op.add_column("properties", sa.Column("offender_radius_miles", sa.Float(), nullable=True))
    op.add_column("properties", sa.Column("nearest_offender_miles", sa.Float(), nullable=True))
    op.add_column("properties", sa.Column("risk_score", sa.Float(), nullable=True))
    op.add_column("properties", sa.Column("risk_band", sa.String(length=20), nullable=True))
    op.add_column("properties", sa.Column("risk_summary", sa.String(length=255), nullable=True))
    op.add_column("properties", sa.Column("risk_confidence", sa.Float(), nullable=True))
    op.add_column("properties", sa.Column("risk_last_computed_at", sa.DateTime(), nullable=True))

    op.create_index("ix_properties_org_risk_score", "properties", ["org_id", "risk_score"], unique=False)
    op.create_index("ix_properties_org_investment_area_band", "properties", ["org_id", "investment_area_band"], unique=False)

    # property inventory snapshots
    op.add_column("property_inventory_snapshots", sa.Column("crime_band", sa.String(length=20), nullable=True))
    op.add_column("property_inventory_snapshots", sa.Column("crime_source", sa.String(length=40), nullable=True))
    op.add_column("property_inventory_snapshots", sa.Column("crime_radius_miles", sa.Float(), nullable=True))
    op.add_column("property_inventory_snapshots", sa.Column("crime_incident_count", sa.Integer(), nullable=True))
    op.add_column("property_inventory_snapshots", sa.Column("crime_confidence", sa.Float(), nullable=True))
    op.add_column("property_inventory_snapshots", sa.Column("investment_area_band", sa.String(length=20), nullable=True))
    op.add_column("property_inventory_snapshots", sa.Column("offender_band", sa.String(length=20), nullable=True))
    op.add_column("property_inventory_snapshots", sa.Column("offender_source", sa.String(length=40), nullable=True))
    op.add_column("property_inventory_snapshots", sa.Column("risk_score", sa.Float(), nullable=True))
    op.add_column("property_inventory_snapshots", sa.Column("risk_band", sa.String(length=20), nullable=True))
    op.add_column("property_inventory_snapshots", sa.Column("risk_confidence", sa.Float(), nullable=True))

    op.create_index(
        "ix_property_inventory_snapshots_org_risk_score",
        "property_inventory_snapshots",
        ["org_id", "risk_score"],
        unique=False,
    )
    op.create_index(
        "ix_property_inventory_snapshots_org_investment_area_band",
        "property_inventory_snapshots",
        ["org_id", "investment_area_band"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_property_inventory_snapshots_org_investment_area_band", table_name="property_inventory_snapshots")
    op.drop_index("ix_property_inventory_snapshots_org_risk_score", table_name="property_inventory_snapshots")

    for col in [
        "risk_confidence",
        "risk_band",
        "risk_score",
        "offender_source",
        "offender_band",
        "investment_area_band",
        "crime_confidence",
        "crime_incident_count",
        "crime_radius_miles",
        "crime_source",
        "crime_band",
    ]:
        op.drop_column("property_inventory_snapshots", col)

    op.drop_index("ix_properties_org_investment_area_band", table_name="properties")
    op.drop_index("ix_properties_org_risk_score", table_name="properties")

    for col in [
        "risk_last_computed_at",
        "risk_confidence",
        "risk_summary",
        "risk_band",
        "risk_score",
        "nearest_offender_miles",
        "offender_radius_miles",
        "offender_source",
        "offender_band",
        "investment_area_band",
        "crime_confidence",
        "crime_dataset_version",
        "crime_nearest_incident_miles",
        "crime_weighted_incident_count",
        "crime_incident_count",
        "crime_area_type",
        "crime_area_sq_miles",
        "crime_radius_miles",
        "crime_method",
        "crime_source",
        "crime_band",
    ]:
        op.drop_column("properties", col)
