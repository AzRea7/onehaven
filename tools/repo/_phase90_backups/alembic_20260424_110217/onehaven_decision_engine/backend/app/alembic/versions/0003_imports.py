"""add import snapshots + deal fingerprint fields

Revision ID: 0003_imports
Revises: 0002_compliance
Create Date: 2026-02-09
"""
from alembic import op
import sqlalchemy as sa


revision = "0003_imports"
down_revision = "0002_compliance"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "import_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("source", sa.String(length=40), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )

    op.add_column("deals", sa.Column("snapshot_id", sa.Integer(), nullable=True))
    op.create_foreign_key(
        "fk_deals_snapshot_id",
        "deals",
        "import_snapshots",
        ["snapshot_id"],
        ["id"],
        ondelete="SET NULL",
    )

    op.add_column("deals", sa.Column("source_fingerprint", sa.String(length=128), nullable=True))
    op.add_column("deals", sa.Column("source_raw_json", sa.Text(), nullable=True))

    op.create_unique_constraint("uq_deals_source_fingerprint", "deals", ["source_fingerprint"])
    op.create_index("ix_deals_source_fingerprint", "deals", ["source_fingerprint"])


def downgrade():
    op.drop_index("ix_deals_source_fingerprint", table_name="deals")
    op.drop_constraint("uq_deals_source_fingerprint", "deals", type_="unique")

    op.drop_column("deals", "source_raw_json")
    op.drop_column("deals", "source_fingerprint")

    op.drop_constraint("fk_deals_snapshot_id", "deals", type_="foreignkey")
    op.drop_column("deals", "snapshot_id")

    op.drop_table("import_snapshots")
