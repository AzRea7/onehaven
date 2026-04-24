"""import snapshots org scoping

Revision ID: 0016_import_snapshots_org_scope
Revises: 0015_jurisdiction_org_scope
Create Date: 2026-02-16
"""

from alembic import op
import sqlalchemy as sa

revision = "0016_import_snapshots_org_scope"
down_revision = "0015_jurisdiction_org_scope"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("import_snapshots", sa.Column("org_id", sa.Integer(), nullable=True))
    op.create_index("ix_import_snapshots_org_id", "import_snapshots", ["org_id"])

    op.create_foreign_key(
        "fk_import_snapshots_org_id",
        "import_snapshots",
        "organizations",
        ["org_id"],
        ["id"],
        ondelete="CASCADE",
    )

    # dev backfill: assume org 1 if existing rows
    op.execute("UPDATE import_snapshots SET org_id = 1 WHERE org_id IS NULL")

    op.alter_column("import_snapshots", "org_id", nullable=False)


def downgrade() -> None:
    op.drop_constraint("fk_import_snapshots_org_id", "import_snapshots", type_="foreignkey")
    op.drop_index("ix_import_snapshots_org_id", table_name="import_snapshots")
    op.drop_column("import_snapshots", "org_id")
