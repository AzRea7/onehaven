# backend/app/alembic/versions/0023_agent_messages_add_run_id.py
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "0023_agent_messages_add_run_id"
down_revision = "0022_agent_runs_reliability_fields"
branch_labels = None
depends_on = None


def _has_column(table: str, column: str) -> bool:
    bind = op.get_bind()
    insp = inspect(bind)
    cols = {c["name"] for c in insp.get_columns(table)}
    return column in cols


def _has_index(table: str, index_name: str) -> bool:
    bind = op.get_bind()
    insp = inspect(bind)
    idx = {i["name"] for i in insp.get_indexes(table)}
    # note: unique constraints are separate; here we only care about indexes
    return index_name in idx


def upgrade():
    # --- Columns ---
    if not _has_column("agent_messages", "run_id"):
        op.add_column("agent_messages", sa.Column("run_id", sa.Integer(), nullable=True))

    if not _has_column("agent_messages", "property_id"):
        op.add_column("agent_messages", sa.Column("property_id", sa.Integer(), nullable=True))

    # --- Indexes ---
    if not _has_index("agent_messages", "ix_agent_messages_run_id"):
        op.create_index("ix_agent_messages_run_id", "agent_messages", ["run_id"], unique=False)

    if not _has_index("agent_messages", "ix_agent_messages_property_id"):
        op.create_index("ix_agent_messages_property_id", "agent_messages", ["property_id"], unique=False)

    if not _has_index("agent_messages", "ix_agent_messages_org_run_id_id"):
        op.create_index(
            "ix_agent_messages_org_run_id_id",
            "agent_messages",
            ["org_id", "run_id", "id"],
            unique=False,
        )


def downgrade():
    # Drop indexes if present, then columns if present (reverse order of dependencies)

    if _has_index("agent_messages", "ix_agent_messages_org_run_id_id"):
        op.drop_index("ix_agent_messages_org_run_id_id", table_name="agent_messages")

    if _has_index("agent_messages", "ix_agent_messages_property_id"):
        op.drop_index("ix_agent_messages_property_id", table_name="agent_messages")

    if _has_column("agent_messages", "property_id"):
        op.drop_column("agent_messages", "property_id")

    if _has_index("agent_messages", "ix_agent_messages_run_id"):
        op.drop_index("ix_agent_messages_run_id", table_name="agent_messages")

    if _has_column("agent_messages", "run_id"):
        op.drop_column("agent_messages", "run_id")