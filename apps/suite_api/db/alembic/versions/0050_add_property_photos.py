"""0050_add_property_photos

Revision ID: 0050_add_property_photos
Revises: 0049_add_deal_pipeline_fields
Create Date: 2026-03-12
"""
from alembic import op
import sqlalchemy as sa


revision = "0050_add_property_photos"
down_revision = "0049_add_deal_pipeline_fields"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "property_photos",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("org_id", sa.Integer(), nullable=False),
        sa.Column("property_id", sa.Integer(), nullable=False),
        sa.Column("source", sa.String(length=40), nullable=False),
        sa.Column("kind", sa.String(length=40), nullable=False),
        sa.Column("label", sa.String(length=160), nullable=True),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("storage_key", sa.String(length=255), nullable=True),
        sa.Column("content_type", sa.String(length=120), nullable=True),
        sa.Column("sort_order", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"]),
        sa.ForeignKeyConstraint(["property_id"], ["properties.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("org_id", "property_id", "url", name="uq_property_photos_org_property_url"),
    )
    op.create_index("ix_property_photos_org_property", "property_photos", ["org_id", "property_id"])
    op.create_index("ix_property_photos_org_source", "property_photos", ["org_id", "source"])
    op.create_index("ix_property_photos_org_kind", "property_photos", ["org_id", "kind"])


def downgrade() -> None:
    op.drop_index("ix_property_photos_org_kind", table_name="property_photos")
    op.drop_index("ix_property_photos_org_source", table_name="property_photos")
    op.drop_index("ix_property_photos_org_property", table_name="property_photos")
    op.drop_table("property_photos")