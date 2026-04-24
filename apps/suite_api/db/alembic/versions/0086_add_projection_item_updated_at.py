# backend/app/alembic/versions/0086_add_projection_item_updated_at.py
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

revision = "0086_add_projection_item_updated_at"
down_revision = "0085_fix_jurisdiction_json_default_casts"
branch_labels = None
depends_on = None


def _col_exists(table: str, col: str) -> bool:
    bind = op.get_bind()
    q = text(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = :t
          AND column_name = :c
        LIMIT 1
        """
    )
    return bind.execute(q, {"t": table, "c": col}).first() is not None


def upgrade() -> None:
    if not _col_exists("property_compliance_projection_items", "updated_at"):
        op.add_column(
            "property_compliance_projection_items",
            sa.Column("updated_at", sa.DateTime(), nullable=True),
        )


def downgrade() -> None:
    pass