"""add policy assertion validation gate

Revision ID: 0081_add_policy_assertion_validation_gate
Revises: 0080_add_policy_refresh_state_machine
Create Date: 2026-04-13
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0081_add_policy_assertion_validation_gate"
down_revision = "0080_add_policy_refresh_state_machine"
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
    _add_column(
        "policy_assertions",
        sa.Column("validation_state", sa.String(length=40), nullable=False, server_default=sa.text("'pending'")),
    )
    _add_column(
        "policy_assertions",
        sa.Column("validation_score", sa.Float(), nullable=False, server_default=sa.text("0")),
    )
    _add_column(
        "policy_assertions",
        sa.Column("validation_reason", sa.String(length=255), nullable=True),
    )
    _add_column(
        "policy_assertions",
        sa.Column("trust_state", sa.String(length=40), nullable=False, server_default=sa.text("'extracted'")),
    )
    _add_column(
        "policy_assertions",
        sa.Column("validated_at", sa.DateTime(), nullable=True),
    )
    _create_index("ix_policy_assertions_validation_state", "policy_assertions", ["validation_state"])
    _create_index("ix_policy_assertions_trust_state", "policy_assertions", ["trust_state"])


def downgrade() -> None:
    _drop_index("ix_policy_assertions_trust_state", "policy_assertions")
    _drop_index("ix_policy_assertions_validation_state", "policy_assertions")
    _drop_column("policy_assertions", "validated_at")
    _drop_column("policy_assertions", "trust_state")
    _drop_column("policy_assertions", "validation_reason")
    _drop_column("policy_assertions", "validation_score")
    _drop_column("policy_assertions", "validation_state")
