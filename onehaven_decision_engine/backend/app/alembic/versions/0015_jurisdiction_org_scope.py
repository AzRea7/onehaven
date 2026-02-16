"""jurisdiction org scoping (global + org overrides)

Revision ID: 0015_jurisdiction_org_scope
Revises: 0014_rehab_tasks_and_basic_ops_tables
Create Date: 2026-02-14
"""

from alembic import op
import sqlalchemy as sa


revision = "0015_jurisdiction_org_scope"
down_revision = "0014_rehab_tasks_and_basic_ops_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add org_id nullable
    op.add_column("jurisdiction_rules", sa.Column("org_id", sa.Integer(), nullable=True))
    op.create_index("ix_jurisdiction_rules_org_id", "jurisdiction_rules", ["org_id"])

    # Drop old unique(city,state) and add unique(org_id,city,state)
    op.drop_constraint("uq_jurisdiction_city_state", "jurisdiction_rules", type_="unique")
    op.create_unique_constraint(
        "uq_jurisdiction_org_city_state",
        "jurisdiction_rules",
        ["org_id", "city", "state"],
    )

    # FK to organizations
    op.create_foreign_key(
        "fk_jurisdiction_rules_org_id",
        "jurisdiction_rules",
        "organizations",
        ["org_id"],
        ["id"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    op.drop_constraint("fk_jurisdiction_rules_org_id", "jurisdiction_rules", type_="foreignkey")
    op.drop_constraint("uq_jurisdiction_org_city_state", "jurisdiction_rules", type_="unique")
    op.create_unique_constraint("uq_jurisdiction_city_state", "jurisdiction_rules", ["city", "state"])
    op.drop_index("ix_jurisdiction_rules_org_id", table_name="jurisdiction_rules")
    op.drop_column("jurisdiction_rules", "org_id")
