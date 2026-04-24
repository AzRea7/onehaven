from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

revision = "0070_repair_acquisition_contacts_columns"
down_revision = "0069_add_property_tax_lookup_metadata"
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
    if not _col_exists("acquisition_contacts", "is_primary"):
        op.add_column(
            "acquisition_contacts",
            sa.Column(
                "is_primary",
                sa.Boolean(),
                nullable=False,
                server_default=sa.text("false"),
            ),
        )

    if not _col_exists("acquisition_contacts", "waiting_on"):
        op.add_column(
            "acquisition_contacts",
            sa.Column(
                "waiting_on",
                sa.Boolean(),
                nullable=False,
                server_default=sa.text("false"),
            ),
        )

    if not _col_exists("acquisition_contacts", "source_type"):
        op.add_column(
            "acquisition_contacts",
            sa.Column("source_type", sa.String(length=64), nullable=True),
        )


def downgrade() -> None:
    if _col_exists("acquisition_contacts", "source_type"):
        op.drop_column("acquisition_contacts", "source_type")

    if _col_exists("acquisition_contacts", "waiting_on"):
        op.drop_column("acquisition_contacts", "waiting_on")

    if _col_exists("acquisition_contacts", "is_primary"):
        op.drop_column("acquisition_contacts", "is_primary")