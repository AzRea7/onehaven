from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

revision = "0087_fix_hqs_rules_schema_drift"
down_revision = "0086_add_projection_item_updated_at"
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
    bind = op.get_bind()

    if not _col_exists("hqs_rules", "title"):
        op.add_column(
            "hqs_rules",
            sa.Column("title", sa.String(length=160), nullable=True),
        )

    if not _col_exists("hqs_rules", "is_active"):
        op.add_column(
            "hqs_rules",
            sa.Column(
                "is_active",
                sa.Boolean(),
                nullable=False,
                server_default=sa.true(),
            ),
        )

    if not _col_exists("hqs_rules", "created_at"):
        op.add_column(
            "hqs_rules",
            sa.Column(
                "created_at",
                sa.DateTime(),
                nullable=True,
                server_default=sa.text("now()"),
            ),
        )

    if not _col_exists("hqs_rules", "updated_at"):
        op.add_column(
            "hqs_rules",
            sa.Column(
                "updated_at",
                sa.DateTime(),
                nullable=True,
                server_default=sa.text("now()"),
            ),
        )

    bind.execute(
        text(
            """
            UPDATE hqs_rules
            SET title = COALESCE(NULLIF(title, ''), LEFT(COALESCE(description, code, 'Untitled Rule'), 160))
            WHERE title IS NULL OR title = ''
            """
        )
    )

    op.alter_column("hqs_rules", "title", nullable=False)

    op.alter_column("hqs_rules", "is_active", server_default=None)
    op.alter_column("hqs_rules", "created_at", server_default=None)
    op.alter_column("hqs_rules", "updated_at", server_default=None)


def downgrade() -> None:
    # forward-only drift repair
    pass