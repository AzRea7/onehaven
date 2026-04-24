"""add compliance_documents table

Revision ID: 0073_add_compliance_documents
Revises: 0072_add_inspection_appointment_fields_and_reminders
Create Date: 2026-04-06
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0073_add_compliance_documents"
down_revision = "0072_add_inspection_appointment_fields_and_reminders"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "compliance_documents",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), sa.ForeignKey("organizations.id"), nullable=False),
        sa.Column("property_id", sa.Integer(), sa.ForeignKey("properties.id", ondelete="CASCADE"), nullable=False),
        sa.Column("inspection_id", sa.Integer(), sa.ForeignKey("inspections.id", ondelete="SET NULL"), nullable=True),
        sa.Column("checklist_item_id", sa.Integer(), sa.ForeignKey("property_checklist_items.id", ondelete="SET NULL"), nullable=True),
        sa.Column("created_by_user_id", sa.Integer(), sa.ForeignKey("app_users.id"), nullable=True),

        sa.Column("category", sa.String(length=80), nullable=False, server_default="other_evidence"),
        sa.Column("source", sa.String(length=40), nullable=False, server_default="upload"),

        sa.Column("label", sa.String(length=255), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),

        sa.Column("original_filename", sa.String(length=255), nullable=True),
        sa.Column("storage_key", sa.String(length=255), nullable=True),
        sa.Column("public_url", sa.Text(), nullable=True),
        sa.Column("content_type", sa.String(length=120), nullable=True),
        sa.Column("file_size_bytes", sa.BigInteger(), nullable=True),

        sa.Column("parse_status", sa.String(length=40), nullable=True),
        sa.Column("extracted_text_preview", sa.Text(), nullable=True),
        sa.Column("parser_meta_json", sa.Text(), nullable=True),

        sa.Column("scan_status", sa.String(length=40), nullable=True),
        sa.Column("scan_result", sa.Text(), nullable=True),

        sa.Column("metadata_json", sa.Text(), nullable=True),

        sa.Column("deleted_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
    )

    op.create_index(
        "ix_compliance_documents_org_property",
        "compliance_documents",
        ["org_id", "property_id"],
    )
    op.create_index(
        "ix_compliance_documents_org_inspection",
        "compliance_documents",
        ["org_id", "inspection_id"],
    )
    op.create_index(
        "ix_compliance_documents_org_checklist_item",
        "compliance_documents",
        ["org_id", "checklist_item_id"],
    )
    op.create_index(
        "ix_compliance_documents_org_category",
        "compliance_documents",
        ["org_id", "category"],
    )
    op.create_index(
        "ix_compliance_documents_org_deleted_at",
        "compliance_documents",
        ["org_id", "deleted_at"],
    )
    op.create_index(
        "ix_compliance_documents_org_property_category",
        "compliance_documents",
        ["org_id", "property_id", "category"],
    )


def downgrade() -> None:
    op.drop_index("ix_compliance_documents_org_property_category", table_name="compliance_documents")
    op.drop_index("ix_compliance_documents_org_deleted_at", table_name="compliance_documents")
    op.drop_index("ix_compliance_documents_org_category", table_name="compliance_documents")
    op.drop_index("ix_compliance_documents_org_checklist_item", table_name="compliance_documents")
    op.drop_index("ix_compliance_documents_org_inspection", table_name="compliance_documents")
    op.drop_index("ix_compliance_documents_org_property", table_name="compliance_documents")
    op.drop_table("compliance_documents")