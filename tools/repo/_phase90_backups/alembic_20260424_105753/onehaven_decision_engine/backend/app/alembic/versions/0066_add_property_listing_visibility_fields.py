"""add property listing visibility fields

Revision ID: 0066_add_property_listing_visibility_fields
Revises: 0065_widen_property_acquisition_source_columns
Create Date: 2026-03-28 00:00:00.000000

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "0066_add_property_listing_visibility_fields"
down_revision = "0065_widen_property_acquisition_source_columns"
branch_labels = None
depends_on = None


def _has_table(inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _columns_by_name(inspector, table_name: str) -> dict[str, dict]:
    return {col["name"]: col for col in inspector.get_columns(table_name)}


def _indexes_by_name(inspector, table_name: str) -> set[str]:
    return {idx["name"] for idx in inspector.get_indexes(table_name)}


"""add property listing visibility fields

Revision ID: 0066_add_property_listing_visibility_fields
Revises: 0065_widen_property_acquisition_source_columns
Create Date: 2026-03-28
"""

from alembic import op
import sqlalchemy as sa


revision = "0066_add_property_listing_visibility_fields"
down_revision = "0065_widen_property_acquisition_source_columns"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add columns as nullable first so existing rows do not break.
    op.add_column("properties", sa.Column("listing_status", sa.String(length=20), nullable=True))
    op.add_column("properties", sa.Column("listing_hidden", sa.Boolean(), nullable=True))
    op.add_column("properties", sa.Column("listing_hidden_reason", sa.String(length=80), nullable=True))

    op.add_column("properties", sa.Column("listing_last_seen_at", sa.DateTime(), nullable=True))
    op.add_column("properties", sa.Column("listing_removed_at", sa.DateTime(), nullable=True))
    op.add_column("properties", sa.Column("listing_listed_at", sa.DateTime(), nullable=True))
    op.add_column("properties", sa.Column("listing_created_at", sa.DateTime(), nullable=True))

    op.add_column("properties", sa.Column("listing_days_on_market", sa.Integer(), nullable=True))
    op.add_column("properties", sa.Column("listing_price", sa.Float(), nullable=True))

    op.add_column("properties", sa.Column("listing_mls_name", sa.String(length=120), nullable=True))
    op.add_column("properties", sa.Column("listing_mls_number", sa.String(length=120), nullable=True))
    op.add_column("properties", sa.Column("listing_type", sa.String(length=80), nullable=True))

    op.add_column("properties", sa.Column("listing_zillow_url", sa.String(length=1024), nullable=True))

    op.add_column("properties", sa.Column("listing_agent_name", sa.String(length=160), nullable=True))
    op.add_column("properties", sa.Column("listing_agent_phone", sa.String(length=80), nullable=True))
    op.add_column("properties", sa.Column("listing_agent_email", sa.String(length=255), nullable=True))
    op.add_column("properties", sa.Column("listing_agent_website", sa.String(length=1024), nullable=True))

    op.add_column("properties", sa.Column("listing_office_name", sa.String(length=160), nullable=True))
    op.add_column("properties", sa.Column("listing_office_phone", sa.String(length=80), nullable=True))
    op.add_column("properties", sa.Column("listing_office_email", sa.String(length=255), nullable=True))

    # Backfill existing rows before making listing_hidden non-null.
    op.execute("UPDATE properties SET listing_hidden = FALSE WHERE listing_hidden IS NULL")

    # Optional: if you want to preserve existing inactive information from legacy JSON,
    # do that here before enforcing NOT NULL. Example only if acquisition_metadata_json exists:
    #
    # op.execute(\"\"\"
    # UPDATE properties
    # SET listing_hidden = CASE
    #   WHEN COALESCE(acquisition_metadata_json->>'listing_hidden', 'false') = 'true' THEN TRUE
    #   ELSE listing_hidden
    # END
    # WHERE acquisition_metadata_json IS NOT NULL
    # \"\"\")

    op.alter_column(
        "properties",
        "listing_hidden",
        existing_type=sa.Boolean(),
        nullable=False,
        server_default=sa.text("false"),
    )

    op.create_index("ix_properties_org_listing_hidden", "properties", ["org_id", "listing_hidden"], unique=False)
    op.create_index("ix_properties_org_listing_status", "properties", ["org_id", "listing_status"], unique=False)
    op.create_index("ix_properties_org_listing_last_seen_at", "properties", ["org_id", "listing_last_seen_at"], unique=False)
    op.create_index("ix_properties_org_listing_removed_at", "properties", ["org_id", "listing_removed_at"], unique=False)
    op.create_index(
        "ix_properties_org_listing_hidden_status",
        "properties",
        ["org_id", "listing_hidden", "listing_status"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_properties_org_listing_hidden_status", table_name="properties")
    op.drop_index("ix_properties_org_listing_removed_at", table_name="properties")
    op.drop_index("ix_properties_org_listing_last_seen_at", table_name="properties")
    op.drop_index("ix_properties_org_listing_status", table_name="properties")
    op.drop_index("ix_properties_org_listing_hidden", table_name="properties")

    op.drop_column("properties", "listing_office_email")
    op.drop_column("properties", "listing_office_phone")
    op.drop_column("properties", "listing_office_name")

    op.drop_column("properties", "listing_agent_website")
    op.drop_column("properties", "listing_agent_email")
    op.drop_column("properties", "listing_agent_phone")
    op.drop_column("properties", "listing_agent_name")

    op.drop_column("properties", "listing_zillow_url")

    op.drop_column("properties", "listing_type")
    op.drop_column("properties", "listing_mls_number")
    op.drop_column("properties", "listing_mls_name")

    op.drop_column("properties", "listing_price")
    op.drop_column("properties", "listing_days_on_market")

    op.drop_column("properties", "listing_created_at")
    op.drop_column("properties", "listing_listed_at")
    op.drop_column("properties", "listing_removed_at")
    op.drop_column("properties", "listing_last_seen_at")

    op.drop_column("properties", "listing_hidden_reason")
    op.drop_column("properties", "listing_hidden")
    op.drop_column("properties", "listing_status")


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    if not _has_table(inspector, "properties"):
        return

    idxs = _indexes_by_name(inspector, "properties")
    cols = _columns_by_name(inspector, "properties")

    for idx_name in [
        "ix_properties_org_listing_hidden_status",
        "ix_properties_org_listing_removed_at",
        "ix_properties_org_listing_last_seen_at",
        "ix_properties_org_listing_status",
        "ix_properties_org_listing_hidden",
    ]:
        if idx_name in idxs:
            op.drop_index(idx_name, table_name="properties")

    for col_name in [
        "listing_office_email",
        "listing_office_phone",
        "listing_office_name",
        "listing_agent_website",
        "listing_agent_email",
        "listing_agent_phone",
        "listing_agent_name",
        "listing_zillow_url",
        "listing_type",
        "listing_mls_number",
        "listing_mls_name",
        "listing_price",
        "listing_days_on_market",
        "listing_created_at",
        "listing_listed_at",
        "listing_removed_at",
        "listing_last_seen_at",
        "listing_hidden_reason",
        "listing_hidden",
        "listing_status",
    ]:
        if col_name in cols:
            op.drop_column("properties", col_name)