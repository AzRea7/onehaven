"""add acquisition search presets, watchlists, tags, and completeness metadata

Revision ID: 0061_add_acquisition_search_and_tagging
Revises: 0060_add_acquisition_workflow_tables
Create Date: 2026-03-24
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy import text

revision = "0061_add_acquisition_search_and_tagging"
down_revision = "0060_add_acquisition_workflow_tables"
branch_labels = None
depends_on = None


JSONB = postgresql.JSONB(astext_type=sa.Text())


def _col_exists(table: str, col: str) -> bool:
    bind = op.get_bind()
    q = text(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = :t
          AND column_name = :c
        LIMIT 1
        """
    )
    return bind.execute(q, {"t": table, "c": col}).first() is not None


def _index_exists(index_name: str) -> bool:
    bind = op.get_bind()
    q = text("SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname=:i LIMIT 1")
    return bind.execute(q, {"i": index_name}).first() is not None


def upgrade() -> None:
    op.create_table(
        "portfolio_watchlists",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=160), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("filters_json", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("sort_json", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("is_default", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("updated_by_user_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["app_users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["updated_by_user_id"], ["app_users.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("org_id", "name", name="uq_portfolio_watchlists_org_name"),
    )
    op.create_index("ix_portfolio_watchlists_org_updated", "portfolio_watchlists", ["org_id", "updated_at"], unique=False)

    op.create_table(
        "acquisition_search_presets",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=160), nullable=False),
        sa.Column("filters_json", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("sort_json", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("updated_by_user_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["app_users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["updated_by_user_id"], ["app_users.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("org_id", "name", name="uq_acquisition_search_presets_org_name"),
    )
    op.create_index("ix_acquisition_search_presets_org_updated", "acquisition_search_presets", ["org_id", "updated_at"], unique=False)

    op.create_table(
        "acquisition_property_tags",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("property_id", sa.Integer(), nullable=False),
        sa.Column("tag", sa.String(length=40), nullable=False),
        sa.Column("source", sa.String(length=40), nullable=False, server_default="operator"),
        sa.Column("created_by_user_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["property_id"], ["properties.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["app_users.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("org_id", "property_id", "tag", name="uq_acquisition_property_tags_org_property_tag"),
    )
    op.create_index("ix_acquisition_property_tags_scope", "acquisition_property_tags", ["org_id", "property_id", "tag"], unique=False)

    for col_name in [
        "acquisition_first_seen_at",
        "acquisition_last_seen_at",
        "acquisition_source_provider",
        "acquisition_source_slug",
        "acquisition_source_record_id",
        "acquisition_source_url",
        "completeness_geo_status",
        "completeness_rent_status",
        "completeness_rehab_status",
        "completeness_risk_status",
        "completeness_jurisdiction_status",
        "completeness_cashflow_status",
        "acquisition_metadata_json",
    ]:
        if col_name in {"acquisition_first_seen_at", "acquisition_last_seen_at"}:
            if not _col_exists("properties", col_name):
                op.add_column("properties", sa.Column(col_name, sa.DateTime(timezone=True), nullable=True))
        elif col_name == "acquisition_metadata_json":
            if not _col_exists("properties", col_name):
                op.add_column("properties", sa.Column(col_name, JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")))
        else:
            if not _col_exists("properties", col_name):
                op.add_column("properties", sa.Column(col_name, sa.String(length=40), nullable=True))

    if not _index_exists("ix_properties_acquisition_first_seen_at"):
        op.create_index("ix_properties_acquisition_first_seen_at", "properties", ["acquisition_first_seen_at"], unique=False)
    if not _index_exists("ix_properties_completeness_geo_status"):
        op.create_index("ix_properties_completeness_geo_status", "properties", ["completeness_geo_status"], unique=False)

    op.execute(
        text(
            """
            UPDATE properties
            SET completeness_geo_status = COALESCE(completeness_geo_status, 'missing'),
                completeness_rent_status = COALESCE(completeness_rent_status, 'missing'),
                completeness_rehab_status = COALESCE(completeness_rehab_status, 'missing'),
                completeness_risk_status = COALESCE(completeness_risk_status, 'missing'),
                completeness_jurisdiction_status = COALESCE(completeness_jurisdiction_status, 'missing'),
                completeness_cashflow_status = COALESCE(completeness_cashflow_status, 'missing')
            """
        )
    )


def downgrade() -> None:
    if _index_exists("ix_properties_completeness_geo_status"):
        op.drop_index("ix_properties_completeness_geo_status", table_name="properties")
    if _index_exists("ix_properties_acquisition_first_seen_at"):
        op.drop_index("ix_properties_acquisition_first_seen_at", table_name="properties")

    for col_name in [
        "acquisition_metadata_json",
        "completeness_cashflow_status",
        "completeness_jurisdiction_status",
        "completeness_risk_status",
        "completeness_rehab_status",
        "completeness_rent_status",
        "completeness_geo_status",
        "acquisition_source_url",
        "acquisition_source_record_id",
        "acquisition_source_slug",
        "acquisition_source_provider",
        "acquisition_last_seen_at",
        "acquisition_first_seen_at",
    ]:
        if _col_exists("properties", col_name):
            op.drop_column("properties", col_name)

    op.drop_index("ix_acquisition_property_tags_scope", table_name="acquisition_property_tags")
    op.drop_table("acquisition_property_tags")
    op.drop_index("ix_acquisition_search_presets_org_updated", table_name="acquisition_search_presets")
    op.drop_table("acquisition_search_presets")
    op.drop_index("ix_portfolio_watchlists_org_updated", table_name="portfolio_watchlists")
    op.drop_table("portfolio_watchlists")
