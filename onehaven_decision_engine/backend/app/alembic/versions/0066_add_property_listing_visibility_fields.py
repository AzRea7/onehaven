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


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    if not _has_table(inspector, "properties"):
        return

    cols = _columns_by_name(inspector, "properties")
    idxs = _indexes_by_name(inspector, "properties")

    new_columns = [
        ("listing_status", sa.String(length=20), True),
        ("listing_hidden", sa.Boolean(), False),
        ("listing_hidden_reason", sa.String(length=80), True),
        ("listing_last_seen_at", sa.DateTime(), True),
        ("listing_removed_at", sa.DateTime(), True),
        ("listing_listed_at", sa.DateTime(), True),
        ("listing_created_at", sa.DateTime(), True),
        ("listing_days_on_market", sa.Integer(), True),
        ("listing_price", sa.Float(), True),
        ("listing_mls_name", sa.String(length=120), True),
        ("listing_mls_number", sa.String(length=120), True),
        ("listing_type", sa.String(length=80), True),
        ("listing_zillow_url", sa.String(length=1024), True),
        ("listing_agent_name", sa.String(length=160), True),
        ("listing_agent_phone", sa.String(length=80), True),
        ("listing_agent_email", sa.String(length=255), True),
        ("listing_agent_website", sa.String(length=1024), True),
        ("listing_office_name", sa.String(length=160), True),
        ("listing_office_phone", sa.String(length=80), True),
        ("listing_office_email", sa.String(length=255), True),
    ]

    for name, coltype, nullable in new_columns:
        if name not in cols:
            op.add_column("properties", sa.Column(name, coltype, nullable=nullable))

    # ensure listing_hidden is non-null with a default
    if "listing_hidden" in _columns_by_name(inspector, "properties"):
        op.execute("UPDATE properties SET listing_hidden = FALSE WHERE listing_hidden IS NULL")
        op.alter_column(
            "properties",
            "listing_hidden",
            existing_type=sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        )

    # backfill from acquisition_metadata_json when available
    op.execute(
        """
        UPDATE properties
        SET
            listing_status = COALESCE(listing_status, acquisition_metadata_json->>'listing_status'),
            listing_hidden = COALESCE(
                listing_hidden,
                CASE
                    WHEN COALESCE(acquisition_metadata_json->>'listing_hidden', 'false') = 'true' THEN true
                    ELSE false
                END
            ),
            listing_hidden_reason = COALESCE(listing_hidden_reason, acquisition_metadata_json->>'listing_hidden_reason'),
            listing_last_seen_at = COALESCE(
                listing_last_seen_at,
                NULLIF(acquisition_metadata_json->>'listing_last_seen_at', '')::timestamp
            ),
            listing_removed_at = COALESCE(
                listing_removed_at,
                NULLIF(acquisition_metadata_json->>'listing_removed_at', '')::timestamp
            ),
            listing_listed_at = COALESCE(
                listing_listed_at,
                NULLIF(acquisition_metadata_json->>'listing_listed_at', '')::timestamp
            ),
            listing_created_at = COALESCE(
                listing_created_at,
                NULLIF(acquisition_metadata_json->>'listing_created_at', '')::timestamp
            ),
            listing_days_on_market = COALESCE(
                listing_days_on_market,
                NULLIF(acquisition_metadata_json->>'listing_days_on_market', '')::integer
            ),
            listing_price = COALESCE(
                listing_price,
                NULLIF(acquisition_metadata_json->>'listing_price', '')::double precision
            ),
            listing_mls_name = COALESCE(listing_mls_name, acquisition_metadata_json->>'listing_mls_name'),
            listing_mls_number = COALESCE(listing_mls_number, acquisition_metadata_json->>'listing_mls_number'),
            listing_type = COALESCE(listing_type, acquisition_metadata_json->>'listing_type'),
            listing_zillow_url = COALESCE(listing_zillow_url, acquisition_metadata_json->>'listing_zillow_url'),
            listing_agent_name = COALESCE(listing_agent_name, acquisition_metadata_json->>'listing_agent_name'),
            listing_agent_phone = COALESCE(listing_agent_phone, acquisition_metadata_json->>'listing_agent_phone'),
            listing_agent_email = COALESCE(listing_agent_email, acquisition_metadata_json->>'listing_agent_email'),
            listing_agent_website = COALESCE(listing_agent_website, acquisition_metadata_json->>'listing_agent_website'),
            listing_office_name = COALESCE(listing_office_name, acquisition_metadata_json->>'listing_office_name'),
            listing_office_phone = COALESCE(listing_office_phone, acquisition_metadata_json->>'listing_office_phone'),
            listing_office_email = COALESCE(listing_office_email, acquisition_metadata_json->>'listing_office_email')
        WHERE acquisition_metadata_json IS NOT NULL
        """
    )

    if "ix_properties_org_listing_hidden" not in idxs:
        op.create_index("ix_properties_org_listing_hidden", "properties", ["org_id", "listing_hidden"])
    if "ix_properties_org_listing_status" not in idxs:
        op.create_index("ix_properties_org_listing_status", "properties", ["org_id", "listing_status"])
    if "ix_properties_org_listing_last_seen_at" not in idxs:
        op.create_index("ix_properties_org_listing_last_seen_at", "properties", ["org_id", "listing_last_seen_at"])
    if "ix_properties_org_listing_removed_at" not in idxs:
        op.create_index("ix_properties_org_listing_removed_at", "properties", ["org_id", "listing_removed_at"])
    if "ix_properties_org_listing_hidden_status" not in idxs:
        op.create_index(
            "ix_properties_org_listing_hidden_status",
            "properties",
            ["org_id", "listing_hidden", "listing_status"],
        )


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