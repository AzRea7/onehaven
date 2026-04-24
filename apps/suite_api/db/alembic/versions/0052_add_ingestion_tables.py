"""0052_add_ingestion_tables

Revision ID: 0052_add_ingestion_tables
Revises: 0051_add_policy_catalog_entries
Create Date: 2026-03-17
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0052_add_ingestion_tables"
down_revision = "0051_add_policy_catalog_entries"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # =========================
    # ingestion_sources
    # =========================
    op.create_table(
        "ingestion_sources",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("provider", sa.String(length=80), nullable=False),
        sa.Column("slug", sa.String(length=120), nullable=False),
        sa.Column("display_name", sa.String(length=180), nullable=False),
        sa.Column("source_type", sa.String(length=40), nullable=False),
        sa.Column("status", sa.String(length=40), nullable=False, server_default="disconnected"),
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("base_url", sa.String(length=500), nullable=True),
        sa.Column("webhook_secret_hint", sa.String(length=255), nullable=True),
        sa.Column("credentials_json", sa.JSON(), nullable=True),
        sa.Column("config_json", sa.JSON(), nullable=True),
        sa.Column("cursor_json", sa.JSON(), nullable=True),
        sa.Column("schedule_cron", sa.String(length=120), nullable=True),
        sa.Column("sync_interval_minutes", sa.Integer(), nullable=True),
        sa.Column("last_synced_at", sa.DateTime(), nullable=True),
        sa.Column("last_success_at", sa.DateTime(), nullable=True),
        sa.Column("last_failure_at", sa.DateTime(), nullable=True),
        sa.Column("next_scheduled_at", sa.DateTime(), nullable=True),
        sa.Column("last_error_summary", sa.Text(), nullable=True),
        sa.Column("last_error_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.UniqueConstraint(
            "org_id",
            "provider",
            "slug",
            name="uq_ingestion_sources_org_provider_slug",
        ),
    )

    op.create_index("ix_ingestion_sources_org_id", "ingestion_sources", ["org_id"])
    op.create_index("ix_ingestion_sources_provider", "ingestion_sources", ["provider"])
    op.create_index("ix_ingestion_sources_status", "ingestion_sources", ["status"])
    op.create_index("ix_ingestion_sources_next_scheduled_at", "ingestion_sources", ["next_scheduled_at"])

    # =========================
    # ingestion_runs
    # =========================
    op.create_table(
        "ingestion_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("source_id", sa.Integer(), nullable=False),
        sa.Column("trigger_type", sa.String(length=40), nullable=False, server_default="manual"),
        sa.Column("status", sa.String(length=40), nullable=False, server_default="running"),
        sa.Column("started_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("records_seen", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("records_imported", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("properties_created", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("properties_updated", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("deals_created", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("deals_updated", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("rent_rows_upserted", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("photos_upserted", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("duplicates_skipped", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("invalid_rows", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_summary", sa.Text(), nullable=True),
        sa.Column("error_json", sa.JSON(), nullable=True),
        sa.Column("summary_json", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["source_id"], ["ingestion_sources.id"], ondelete="CASCADE"),
    )

    op.create_index("ix_ingestion_runs_org_id", "ingestion_runs", ["org_id"])
    op.create_index("ix_ingestion_runs_source_id", "ingestion_runs", ["source_id"])
    op.create_index("ix_ingestion_runs_status", "ingestion_runs", ["status"])
    op.create_index("ix_ingestion_runs_started_at", "ingestion_runs", ["started_at"])

    # =========================
    # ingestion_record_links
    # =========================
    op.create_table(
        "ingestion_record_links",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("provider", sa.String(length=80), nullable=False),
        sa.Column("source_id", sa.Integer(), nullable=False),
        sa.Column("external_record_id", sa.String(length=255), nullable=False),
        sa.Column("external_url", sa.String(length=1000), nullable=True),
        sa.Column("property_id", sa.Integer(), nullable=True),
        sa.Column("deal_id", sa.Integer(), nullable=True),
        sa.Column("fingerprint", sa.String(length=255), nullable=True),
        sa.Column("raw_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(["source_id"], ["ingestion_sources.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["property_id"], ["properties.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["deal_id"], ["deals.id"], ondelete="CASCADE"),
        sa.UniqueConstraint(
            "org_id",
            "provider",
            "external_record_id",
            name="uq_ingestion_record_links_org_provider_external",
        ),
    )

    op.create_index("ix_ingestion_record_links_org_id", "ingestion_record_links", ["org_id"])
    op.create_index("ix_ingestion_record_links_source_id", "ingestion_record_links", ["source_id"])
    op.create_index("ix_ingestion_record_links_property_id", "ingestion_record_links", ["property_id"])
    op.create_index("ix_ingestion_record_links_deal_id", "ingestion_record_links", ["deal_id"])


def downgrade() -> None:
    op.drop_index("ix_ingestion_record_links_deal_id", table_name="ingestion_record_links")
    op.drop_index("ix_ingestion_record_links_property_id", table_name="ingestion_record_links")
    op.drop_index("ix_ingestion_record_links_source_id", table_name="ingestion_record_links")
    op.drop_index("ix_ingestion_record_links_org_id", table_name="ingestion_record_links")
    op.drop_table("ingestion_record_links")

    op.drop_index("ix_ingestion_runs_started_at", table_name="ingestion_runs")
    op.drop_index("ix_ingestion_runs_status", table_name="ingestion_runs")
    op.drop_index("ix_ingestion_runs_source_id", table_name="ingestion_runs")
    op.drop_index("ix_ingestion_runs_org_id", table_name="ingestion_runs")
    op.drop_table("ingestion_runs")

    op.drop_index("ix_ingestion_sources_next_scheduled_at", table_name="ingestion_sources")
    op.drop_index("ix_ingestion_sources_status", table_name="ingestion_sources")
    op.drop_index("ix_ingestion_sources_provider", table_name="ingestion_sources")
    op.drop_index("ix_ingestion_sources_org_id", table_name="ingestion_sources")
    op.drop_table("ingestion_sources")
    