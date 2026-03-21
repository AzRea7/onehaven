"""add is_authoritative to policy_sources"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "0057_add_is_authoritative_to_policy_sources"
down_revision = "0056_add_real_inspection_compliance_foundation"
branch_labels = None
depends_on = None


def _insp():
    return inspect(op.get_bind())


def _has_table(name: str) -> bool:
    return name in _insp().get_table_names()


def _has_column(table: str, col: str) -> bool:
    if not _has_table(table):
        return False
    return col in {c["name"] for c in _insp().get_columns(table)}


def upgrade() -> None:
    if _has_table("policy_sources") and not _has_column("policy_sources", "is_authoritative"):
        op.add_column(
            "policy_sources",
            sa.Column("is_authoritative", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        )

        op.execute(
            sa.text(
                """
                UPDATE policy_sources
                SET is_authoritative = COALESCE(is_authoritative, true)
                """
            )
        )

        with op.batch_alter_table("policy_sources") as batch:
            batch.alter_column("is_authoritative", server_default=None)


def downgrade() -> None:
    if _has_table("policy_sources") and _has_column("policy_sources", "is_authoritative"):
        op.drop_column("policy_sources", "is_authoritative")