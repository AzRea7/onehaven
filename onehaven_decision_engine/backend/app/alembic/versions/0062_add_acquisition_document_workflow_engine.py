"""add acquisition workflow engine tables

Revision ID: 0062_add_acquisition_document_workflow_engine
Revises: 0061_add_acquisition_search_and_tagging
Create Date: 2026-03-24
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0062_add_acquisition_document_workflow_engine"
down_revision = "0061_add_acquisition_search_and_tagging"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("acquisition_documents") as batch:
        batch.add_column(sa.Column("replaced_by_document_id", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("deleted_at", sa.DateTime(), nullable=True))
        batch.add_column(sa.Column("deleted_reason", sa.Text(), nullable=True))

    op.create_table(
        "acquisition_contacts",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("property_id", sa.Integer(), nullable=False),
        sa.Column("role", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("email", sa.String(length=255), nullable=True),
        sa.Column("phone", sa.String(length=64), nullable=True),
        sa.Column("company", sa.String(length=255), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("source_document_id", sa.Integer(), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("extraction_version", sa.String(length=64), nullable=True),
        sa.Column("manually_overridden", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("ix_acquisition_contacts_org_property", "acquisition_contacts", ["org_id", "property_id"], unique=False)
    op.create_index("ix_acquisition_contacts_org_role", "acquisition_contacts", ["org_id", "role"], unique=False)

    op.create_table(
        "acquisition_deadlines",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("property_id", sa.Integer(), nullable=False),
        sa.Column("code", sa.String(length=64), nullable=False),
        sa.Column("label", sa.String(length=255), nullable=True),
        sa.Column("due_at", sa.DateTime(), nullable=False),
        sa.Column("status", sa.String(length=64), nullable=False, server_default=sa.text("'open'")),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("source_document_id", sa.Integer(), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("extraction_version", sa.String(length=64), nullable=True),
        sa.Column("manually_overridden", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("ix_acquisition_deadlines_org_property", "acquisition_deadlines", ["org_id", "property_id"], unique=False)
    op.create_index("ix_acquisition_deadlines_org_due_at", "acquisition_deadlines", ["org_id", "due_at"], unique=False)
    op.create_index("ix_acquisition_deadlines_org_code", "acquisition_deadlines", ["org_id", "code"], unique=False)

    op.create_table(
        "acquisition_field_values",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("property_id", sa.Integer(), nullable=False),
        sa.Column("field_name", sa.String(length=128), nullable=False),
        sa.Column("extracted_value", sa.Text(), nullable=True),
        sa.Column("normalized_value_json", sa.Text(), nullable=True),
        sa.Column("review_state", sa.String(length=32), nullable=False, server_default=sa.text("'suggested'")),
        sa.Column("source_document_id", sa.Integer(), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("extraction_version", sa.String(length=64), nullable=True),
        sa.Column("manually_overridden", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("ix_acquisition_field_values_org_property", "acquisition_field_values", ["org_id", "property_id"], unique=False)
    op.create_index("ix_acquisition_field_values_org_field_name", "acquisition_field_values", ["org_id", "field_name"], unique=False)
    op.create_index("ix_acquisition_field_values_org_review_state", "acquisition_field_values", ["org_id", "review_state"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_acquisition_field_values_org_review_state", table_name="acquisition_field_values")
    op.drop_index("ix_acquisition_field_values_org_field_name", table_name="acquisition_field_values")
    op.drop_index("ix_acquisition_field_values_org_property", table_name="acquisition_field_values")
    op.drop_table("acquisition_field_values")

    op.drop_index("ix_acquisition_deadlines_org_code", table_name="acquisition_deadlines")
    op.drop_index("ix_acquisition_deadlines_org_due_at", table_name="acquisition_deadlines")
    op.drop_index("ix_acquisition_deadlines_org_property", table_name="acquisition_deadlines")
    op.drop_table("acquisition_deadlines")

    op.drop_index("ix_acquisition_contacts_org_role", table_name="acquisition_contacts")
    op.drop_index("ix_acquisition_contacts_org_property", table_name="acquisition_contacts")
    op.drop_table("acquisition_contacts")

    with op.batch_alter_table("acquisition_documents") as batch:
        batch.drop_column("deleted_reason")
        batch.drop_column("deleted_at")
        batch.drop_column("replaced_by_document_id")
