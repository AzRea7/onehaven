"""repair jurisdiction schema drift for missing profile and coverage columns

Revision ID: 0083_repair_jurisdiction_schema_drift
Revises: 0082_add_policy_override_ledger
Create Date: 2026-04-16
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0083_repair_jurisdiction_schema_drift"
down_revision = "0082_add_policy_override_ledger"
branch_labels = None
depends_on = None


def _table_exists(table_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    try:
        return table_name in inspector.get_table_names()
    except Exception:
        return False


def _column_exists(table_name: str, column_name: str) -> bool:
    if not _table_exists(table_name):
        return False
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    try:
        cols = inspector.get_columns(table_name)
    except Exception:
        return False
    return any(str(col.get("name")) == column_name for col in cols)


def _index_exists(index_name: str, table_name: str) -> bool:
    if not _table_exists(table_name):
        return False
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    try:
        indexes = inspector.get_indexes(table_name)
    except Exception:
        return False
    return any(str(idx.get("name")) == index_name for idx in indexes)


def _add_column(table_name: str, column: sa.Column) -> None:
    if _table_exists(table_name) and not _column_exists(table_name, str(column.name)):
        op.add_column(table_name, column)


def _drop_column(table_name: str, column_name: str) -> None:
    if _table_exists(table_name) and _column_exists(table_name, column_name):
        op.drop_column(table_name, column_name)


def _create_index(index_name: str, table_name: str, columns: list[str], *, unique: bool = False) -> None:
    if _table_exists(table_name) and not _index_exists(index_name, table_name):
        op.create_index(index_name, table_name, columns, unique=unique)


def _drop_index(index_name: str, table_name: str) -> None:
    if _table_exists(table_name) and _index_exists(index_name, table_name):
        op.drop_index(index_name, table_name=table_name)


def upgrade() -> None:
    # ------------------------------------------------------------------
    # jurisdiction_profiles
    # Fix runtime error:
    #   UndefinedColumn: jurisdiction_profiles.rental_registration_required
    # ------------------------------------------------------------------
    _add_column(
        "jurisdiction_profiles",
        sa.Column("rental_registration_required", sa.Boolean(), nullable=True),
    )

    # ------------------------------------------------------------------
    # jurisdiction_coverage_status
    # Fix runtime error:
    #   UndefinedColumn: jurisdiction_coverage_status.jurisdiction_slug
    # ------------------------------------------------------------------
    _add_column(
        "jurisdiction_coverage_status",
        sa.Column("jurisdiction_slug", sa.String(length=255), nullable=True),
    )

    # Helpful for lookups/debugging if the app starts using the slug directly.
    _create_index(
        "ix_jurisdiction_coverage_status_jurisdiction_slug",
        "jurisdiction_coverage_status",
        ["jurisdiction_slug"],
        unique=False,
    )

    # Backfill jurisdiction_slug from existing scope columns.
    # Uses PostgreSQL concat_ws so null parts are skipped cleanly.
    if _table_exists("jurisdiction_coverage_status") and _column_exists("jurisdiction_coverage_status", "jurisdiction_slug"):
        op.execute(
            sa.text(
                """
                UPDATE jurisdiction_coverage_status
                SET jurisdiction_slug = LOWER(
                    concat_ws(
                        '-',
                        NULLIF(TRIM(state), ''),
                        NULLIF(TRIM(county), ''),
                        NULLIF(TRIM(city), ''),
                        NULLIF(TRIM(pha_name), '')
                    )
                )
                WHERE jurisdiction_slug IS NULL
                   OR BTRIM(jurisdiction_slug) = ''
                """
            )
        )


def downgrade() -> None:
    _drop_index(
        "ix_jurisdiction_coverage_status_jurisdiction_slug",
        "jurisdiction_coverage_status",
    )
    _drop_column("jurisdiction_coverage_status", "jurisdiction_slug")
    _drop_column("jurisdiction_profiles", "rental_registration_required")