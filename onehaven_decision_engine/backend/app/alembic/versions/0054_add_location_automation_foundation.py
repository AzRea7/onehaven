"""add location automation foundation

Revision ID: 0054_add_location_automation_foundation
Revises: 0053_add_seen_columns_to_ingestion_record_links
Create Date: 2026-03-20
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0054_add_location_automation_foundation"
down_revision = "0053_add_seen_columns_to_ingestion_record_links"
branch_labels = None
depends_on = None


def _has_table(table_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return table_name in inspector.get_table_names()


def _has_column(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if table_name not in inspector.get_table_names():
        return False
    return column_name in {col["name"] for col in inspector.get_columns(table_name)}


def _has_index(table_name: str, index_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if table_name not in inspector.get_table_names():
        return False
    return index_name in {idx["name"] for idx in inspector.get_indexes(table_name)}


def upgrade() -> None:
    # ------------------------------------------------------------------
    # properties: add normalized/geocode metadata
    # NOTE:
    # We intentionally reuse existing lat/lng columns already present
    # in the schema instead of introducing duplicate latitude/longitude.
    # ------------------------------------------------------------------
    if not _has_column("properties", "normalized_address"):
        op.add_column(
            "properties",
            sa.Column("normalized_address", sa.String(length=400), nullable=True),
        )

    if not _has_column("properties", "geocode_source"):
        op.add_column(
            "properties",
            sa.Column("geocode_source", sa.String(length=40), nullable=True),
        )

    if not _has_column("properties", "geocode_confidence"):
        op.add_column(
            "properties",
            sa.Column("geocode_confidence", sa.Float(), nullable=True),
        )

    if not _has_column("properties", "geocode_last_refreshed"):
        op.add_column(
            "properties",
            sa.Column("geocode_last_refreshed", sa.DateTime(), nullable=True),
        )

    if not _has_index("properties", "ix_properties_org_normalized_address"):
        op.create_index(
            "ix_properties_org_normalized_address",
            "properties",
            ["org_id", "normalized_address"],
            unique=False,
        )

    if not _has_index("properties", "ix_properties_org_geocode_last_refreshed"):
        op.create_index(
            "ix_properties_org_geocode_last_refreshed",
            "properties",
            ["org_id", "geocode_last_refreshed"],
            unique=False,
        )

    # ------------------------------------------------------------------
    # geocode_cache: global address geocode cache
    # ------------------------------------------------------------------
    if not _has_table("geocode_cache"):
        op.create_table(
            "geocode_cache",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("normalized_address", sa.String(length=400), nullable=False),
            sa.Column("raw_address", sa.String(length=400), nullable=True),
            sa.Column("city", sa.String(length=120), nullable=True),
            sa.Column("state", sa.String(length=2), nullable=True),
            sa.Column("zip", sa.String(length=10), nullable=True),
            sa.Column("county", sa.String(length=80), nullable=True),
            sa.Column("lat", sa.Float(), nullable=True),
            sa.Column("lng", sa.Float(), nullable=True),
            sa.Column("source", sa.String(length=40), nullable=False, server_default="unknown"),
            sa.Column("confidence", sa.Float(), nullable=True),
            sa.Column("formatted_address", sa.String(length=400), nullable=True),
            sa.Column("provider_response_json", sa.JSON(), nullable=True),
            sa.Column("hit_count", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("last_refreshed_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.Column("expires_at", sa.DateTime(), nullable=True),
            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("normalized_address", name="uq_geocode_cache_normalized_address"),
        )

    if not _has_index("geocode_cache", "ix_geocode_cache_source"):
        op.create_index("ix_geocode_cache_source", "geocode_cache", ["source"], unique=False)

    if not _has_index("geocode_cache", "ix_geocode_cache_expires_at"):
        op.create_index("ix_geocode_cache_expires_at", "geocode_cache", ["expires_at"], unique=False)

    if not _has_index("geocode_cache", "ix_geocode_cache_last_refreshed_at"):
        op.create_index(
            "ix_geocode_cache_last_refreshed_at",
            "geocode_cache",
            ["last_refreshed_at"],
            unique=False,
        )


def downgrade() -> None:
    if _has_index("geocode_cache", "ix_geocode_cache_last_refreshed_at"):
        op.drop_index("ix_geocode_cache_last_refreshed_at", table_name="geocode_cache")

    if _has_index("geocode_cache", "ix_geocode_cache_expires_at"):
        op.drop_index("ix_geocode_cache_expires_at", table_name="geocode_cache")

    if _has_index("geocode_cache", "ix_geocode_cache_source"):
        op.drop_index("ix_geocode_cache_source", table_name="geocode_cache")

    if _has_table("geocode_cache"):
        op.drop_table("geocode_cache")

    if _has_index("properties", "ix_properties_org_geocode_last_refreshed"):
        op.drop_index("ix_properties_org_geocode_last_refreshed", table_name="properties")

    if _has_index("properties", "ix_properties_org_normalized_address"):
        op.drop_index("ix_properties_org_normalized_address", table_name="properties")

    if _has_column("properties", "geocode_last_refreshed"):
        op.drop_column("properties", "geocode_last_refreshed")

    if _has_column("properties", "geocode_confidence"):
        op.drop_column("properties", "geocode_confidence")

    if _has_column("properties", "geocode_source"):
        op.drop_column("properties", "geocode_source")

    if _has_column("properties", "normalized_address"):
        op.drop_column("properties", "normalized_address")
        