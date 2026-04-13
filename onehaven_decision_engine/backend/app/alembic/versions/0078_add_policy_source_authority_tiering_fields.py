# backend/app/alembic/versions/0078_add_policy_source_authority_tiering_fields.py
"""add policy source authority tiering fields

Revision ID: 0078_add_policy_source_authority_tiering_fields
Revises: 0077_add_jurisdiction_completeness_metadata_fields
Create Date: 2026-04-13
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0078_add_policy_source_authority_tiering_fields"
down_revision = "0077_add_jurisdiction_completeness_metadata_fields"
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
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    try:
        cols = inspector.get_columns(table_name)
    except Exception:
        return False
    return any(str(col.get("name")) == column_name for col in cols)


def _index_exists(index_name: str, table_name: str) -> bool:
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
    if _table_exists("policy_sources"):
        _add_column(
            "policy_sources",
            sa.Column(
                "authority_tier",
                sa.String(length=40),
                nullable=False,
                server_default=sa.text("'derived_or_inferred'"),
            ),
        )
        _add_column(
            "policy_sources",
            sa.Column(
                "authority_rank",
                sa.Integer(),
                nullable=False,
                server_default=sa.text("0"),
            ),
        )
        _add_column(
            "policy_sources",
            sa.Column(
                "publication_type",
                sa.String(length=80),
                nullable=True,
            ),
        )
        _add_column(
            "policy_sources",
            sa.Column(
                "domain_name",
                sa.String(length=255),
                nullable=True,
            ),
        )
        _add_column(
            "policy_sources",
            sa.Column(
                "authority_class",
                sa.String(length=80),
                nullable=True,
            ),
        )
        _add_column(
            "policy_sources",
            sa.Column(
                "authority_reason",
                sa.String(length=255),
                nullable=True,
            ),
        )
        _add_column(
            "policy_sources",
            sa.Column(
                "approved_supporting_source",
                sa.Boolean(),
                nullable=False,
                server_default=sa.text("false"),
            ),
        )
        _add_column(
            "policy_sources",
            sa.Column(
                "semi_authoritative",
                sa.Boolean(),
                nullable=False,
                server_default=sa.text("false"),
            ),
        )
        _add_column(
            "policy_sources",
            sa.Column(
                "derived_or_inferred",
                sa.Boolean(),
                nullable=False,
                server_default=sa.text("false"),
            ),
        )

        _create_index(
            "ix_policy_sources_authority_tier",
            "policy_sources",
            ["authority_tier"],
        )
        _create_index(
            "ix_policy_sources_authority_rank",
            "policy_sources",
            ["authority_rank"],
        )
        _create_index(
            "ix_policy_sources_domain_name",
            "policy_sources",
            ["domain_name"],
        )
        _create_index(
            "ix_policy_sources_authority_class",
            "policy_sources",
            ["authority_class"],
        )


def downgrade() -> None:
    _drop_index("ix_policy_sources_authority_class", "policy_sources")
    _drop_index("ix_policy_sources_domain_name", "policy_sources")
    _drop_index("ix_policy_sources_authority_rank", "policy_sources")
    _drop_index("ix_policy_sources_authority_tier", "policy_sources")

    for name in [
        "derived_or_inferred",
        "semi_authoritative",
        "approved_supporting_source",
        "authority_reason",
        "authority_class",
        "domain_name",
        "publication_type",
        "authority_rank",
        "authority_tier",
    ]:
        _drop_column("policy_sources", name)