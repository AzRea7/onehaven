# backend/app/alembic/versions/0021_inspections_add_org_id.py
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0021_inspections_add_org_id"
down_revision = "0020_policy_tables"
branch_labels = None
depends_on = None


def upgrade():
    # 1) add column nullable first (existing rows)
    op.add_column("inspections", sa.Column("org_id", sa.Integer(), nullable=True))
    op.create_index("ix_inspections_org_id", "inspections", ["org_id"], unique=False)

    # 2) backfill org_id using property -> org mapping
    # properties table is org-scoped already (properties.org_id exists)
    op.execute(
        """
        UPDATE inspections i
        SET org_id = p.org_id
        FROM properties p
        WHERE i.property_id = p.id
        """
    )

    # 3) set NOT NULL
    op.alter_column("inspections", "org_id", existing_type=sa.Integer(), nullable=False)

    # 4) FK to organizations
    op.create_foreign_key(
        "fk_inspections_org_id",
        "inspections",
        "organizations",
        ["org_id"],
        ["id"],
        ondelete="CASCADE",
    )


def downgrade():
    op.drop_constraint("fk_inspections_org_id", "inspections", type_="foreignkey")
    op.drop_index("ix_inspections_org_id", table_name="inspections")
    op.drop_column("inspections", "org_id")