"""add acquisition workflow tables

Revision ID: 0060_add_acquisition_workflow_tables
Revises: 0059_add_inventory_and_pane_snapshot_tables
Create Date: 2026-03-24
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0060_add_acquisition_workflow_tables"
down_revision = "0059_add_inventory_and_pane_snapshot_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "acquisition_records",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("property_id", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=64), nullable=True),
        sa.Column("waiting_on", sa.String(length=255), nullable=True),
        sa.Column("next_step", sa.String(length=255), nullable=True),
        sa.Column("contract_date", sa.Date(), nullable=True),
        sa.Column("target_close_date", sa.Date(), nullable=True),
        sa.Column("closing_date", sa.Date(), nullable=True),
        sa.Column("purchase_price", sa.Float(), nullable=True),
        sa.Column("earnest_money", sa.Float(), nullable=True),
        sa.Column("loan_amount", sa.Float(), nullable=True),
        sa.Column("loan_type", sa.String(length=64), nullable=True),
        sa.Column("interest_rate", sa.Float(), nullable=True),
        sa.Column("cash_to_close", sa.Float(), nullable=True),
        sa.Column("closing_costs", sa.Float(), nullable=True),
        sa.Column("seller_credits", sa.Float(), nullable=True),
        sa.Column("title_company", sa.String(length=255), nullable=True),
        sa.Column("escrow_officer", sa.String(length=255), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("contacts_json", sa.Text(), nullable=True),
        sa.Column("milestones_json", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("org_id", "property_id", name="uq_acquisition_records_org_property"),
    )

    op.create_index(
        "ix_acquisition_records_org_property",
        "acquisition_records",
        ["org_id", "property_id"],
        unique=False,
    )
    op.create_index(
        "ix_acquisition_records_status",
        "acquisition_records",
        ["org_id", "status"],
        unique=False,
    )
    op.create_index(
        "ix_acquisition_records_target_close_date",
        "acquisition_records",
        ["org_id", "target_close_date"],
        unique=False,
    )

    op.create_table(
        "acquisition_documents",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("property_id", sa.Integer(), nullable=False),
        sa.Column("kind", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("original_filename", sa.String(length=255), nullable=True),
        sa.Column("storage_path", sa.Text(), nullable=True),
        sa.Column("storage_url", sa.Text(), nullable=True),
        sa.Column("content_type", sa.String(length=255), nullable=True),
        sa.Column("file_size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("sha256", sa.String(length=64), nullable=True),
        sa.Column("upload_status", sa.String(length=64), nullable=True),
        sa.Column("scan_status", sa.String(length=64), nullable=True),
        sa.Column("scan_result", sa.Text(), nullable=True),
        sa.Column("parse_status", sa.String(length=64), nullable=True),
        sa.Column("parser_version", sa.String(length=64), nullable=True),
        sa.Column("preview_text", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=64), nullable=True),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("extracted_text", sa.Text(), nullable=True),
        sa.Column("extracted_fields_json", sa.Text(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
    )

    op.create_index(
        "ix_acquisition_documents_org_property",
        "acquisition_documents",
        ["org_id", "property_id"],
        unique=False,
    )
    op.create_index(
        "ix_acquisition_documents_kind",
        "acquisition_documents",
        ["org_id", "kind"],
        unique=False,
    )
    op.create_index(
        "ix_acquisition_documents_sha256",
        "acquisition_documents",
        ["org_id", "sha256"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_acquisition_documents_sha256", table_name="acquisition_documents")
    op.drop_index("ix_acquisition_documents_kind", table_name="acquisition_documents")
    op.drop_index("ix_acquisition_documents_org_property", table_name="acquisition_documents")
    op.drop_table("acquisition_documents")

    op.drop_index("ix_acquisition_records_target_close_date", table_name="acquisition_records")
    op.drop_index("ix_acquisition_records_status", table_name="acquisition_records")
    op.drop_index("ix_acquisition_records_org_property", table_name="acquisition_records")
    op.drop_table("acquisition_records")