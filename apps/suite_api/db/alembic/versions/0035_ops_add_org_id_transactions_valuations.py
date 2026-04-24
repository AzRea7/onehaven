"""ops org scoping: add org_id to transactions + valuations (backfill from properties)

Revision ID: 0035_ops_org_tx_vals
Revises: 0034_add_leases_org_id
Create Date: 2026-03-03
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect, text

revision = "0035_ops_org_tx_vals"
down_revision = "0034_add_leases_org_id"
branch_labels = None
depends_on = None


def _insp():
    return inspect(op.get_bind())


def _has_table(name: str) -> bool:
    return name in _insp().get_table_names()


def _cols(table: str) -> set[str]:
    if not _has_table(table):
        return set()
    return {c["name"] for c in _insp().get_columns(table)}


def _indexes(table: str) -> set[str]:
    if not _has_table(table):
        return set()
    return {i["name"] for i in _insp().get_indexes(table)}


def _fks(table: str) -> set[str]:
    if not _has_table(table):
        return set()
    return {fk.get("name") for fk in _insp().get_foreign_keys(table) if fk.get("name")}


def _org_table_name() -> str | None:
    # Your schema might call it organizations or orgs; choose safely.
    if _has_table("organizations"):
        return "organizations"
    if _has_table("orgs"):
        return "orgs"
    return None


def _ensure_org_id(table: str, backfill_from_property: bool = True) -> None:
    bind = op.get_bind()

    if not _has_table(table):
        return

    cols = _cols(table)
    if "org_id" not in cols:
        op.add_column(table, sa.Column("org_id", sa.Integer(), nullable=True))

    if backfill_from_property and _has_table("properties"):
        # assumes table has property_id FK (true for transactions/valuations in your app)
        bind.execute(
            text(
                f"""
                UPDATE {table} t
                SET org_id = p.org_id
                FROM properties p
                WHERE t.property_id = p.id
                  AND t.org_id IS NULL
                """
            )
        )

    # enforce NOT NULL only if fully backfilled
    remaining = bind.execute(text(f"SELECT COUNT(*) FROM {table} WHERE org_id IS NULL")).scalar()  # type: ignore
    if int(remaining or 0) == 0:
        op.alter_column(table, "org_id", existing_type=sa.Integer(), nullable=False)

    # indexes
    idxs = _indexes(table)
    idx_org = f"ix_{table}_org_id"
    if idx_org not in idxs:
        op.create_index(idx_org, table, ["org_id"], unique=False)

    # common query pattern: org_id + property_id
    if "property_id" in _cols(table):
        idx_org_prop = f"ix_{table}_org_property_id"
        if idx_org_prop not in idxs:
            op.create_index(idx_org_prop, table, ["org_id", "property_id"], unique=False)

    # optional FK
    fks = _fks(table)
    org_table = _org_table_name()
    if org_table and f"fk_{table}_org_id" not in fks:
        op.create_foreign_key(
            f"fk_{table}_org_id",
            table,
            org_table,
            ["org_id"],
            ["id"],
            ondelete="CASCADE",
        )


def upgrade() -> None:
    _ensure_org_id("transactions", backfill_from_property=True)
    _ensure_org_id("valuations", backfill_from_property=True)


def downgrade() -> None:
    # Conservative: do not drop columns automatically in drift-fix downgrade
    pass