from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

revision = "0068_add_property_tax_insurance_fields"
down_revision = "0067_add_productionized_crime_risk_fields"
branch_labels = None
depends_on = None


def _col_exists(table: str, col: str) -> bool:
    bind = op.get_bind()
    q = text("""
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = :t
          AND column_name = :c
        LIMIT 1
    """)
    return bind.execute(q, {"t": table, "c": col}).first() is not None


def _add_col(table: str, column: sa.Column) -> None:
    if not _col_exists(table, column.name):
        op.add_column(table, column)


def upgrade() -> None:
    _add_col("properties", sa.Column("property_tax_annual", sa.Float(), nullable=True))
    _add_col("properties", sa.Column("property_tax_rate_annual", sa.Float(), nullable=True))
    _add_col("properties", sa.Column("property_tax_source", sa.String(length=64), nullable=True))
    _add_col("properties", sa.Column("property_tax_confidence", sa.Float(), nullable=True))
    _add_col("properties", sa.Column("property_tax_year", sa.Integer(), nullable=True))

    _add_col("properties", sa.Column("insurance_annual", sa.Float(), nullable=True))
    _add_col("properties", sa.Column("insurance_source", sa.String(length=64), nullable=True))
    _add_col("properties", sa.Column("insurance_confidence", sa.Float(), nullable=True))

    _add_col("properties", sa.Column("monthly_taxes", sa.Float(), nullable=True))
    _add_col("properties", sa.Column("monthly_insurance", sa.Float(), nullable=True))


def downgrade() -> None:
    pass
