"""agent slots

Revision ID: 0010_agent_slots
Revises: 0009_add_underwriting_result_meta
Create Date: 2026-02-12
"""
from alembic import op
import sqlalchemy as sa


revision = "0010_agent_slots"
down_revision = "0009_add_underwriting_result_meta"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "agent_slot_assignments",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("slot_key", sa.String(length=80), nullable=False),
        sa.Column("property_id", sa.Integer(), sa.ForeignKey("properties.id"), nullable=True),
        sa.Column("owner_type", sa.String(length=20), nullable=False, server_default="human"),
        sa.Column("assignee", sa.String(length=120), nullable=True),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="idle"),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_agent_slot_assignments_slot_key", "agent_slot_assignments", ["slot_key"])
    op.create_index("ix_agent_slot_assignments_property_id", "agent_slot_assignments", ["property_id"])


def downgrade():
    op.drop_index("ix_agent_slot_assignments_property_id", table_name="agent_slot_assignments")
    op.drop_index("ix_agent_slot_assignments_slot_key", table_name="agent_slot_assignments")
    op.drop_table("agent_slot_assignments")
