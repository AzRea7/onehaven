"""add leases.org_id + backfill from properties/tenants

Revision ID: 0034_add_leases_org_id
Revises: 0033_force_schema_fix
Create Date: 2026-03-03
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect, text

revision = "0034_add_leases_org_id"
down_revision = "0033_force_schema_fix"
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


def upgrade() -> None:
    bind = op.get_bind()

    if not _has_table("leases"):
        return

    cols = _cols("leases")

    # 1) Add org_id nullable first (safe for existing rows)
    if "org_id" not in cols:
        op.add_column("leases", sa.Column("org_id", sa.Integer(), nullable=True))

    # 2) Backfill from properties (most reliable because leases already FK to properties)
    if _has_table("properties"):
        bind.execute(
            text(
                """
                UPDATE leases l
                SET org_id = p.org_id
                FROM properties p
                WHERE l.property_id = p.id
                  AND l.org_id IS NULL
                """
            )
        )

    # 3) Optional fallback: backfill from tenants if tenants has org_id
    if _has_table("tenants"):
        tcols = _cols("tenants")
        if "org_id" in tcols:
            bind.execute(
                text(
                    """
                    UPDATE leases l
                    SET org_id = t.org_id
                    FROM tenants t
                    WHERE l.tenant_id = t.id
                      AND l.org_id IS NULL
                    """
                )
            )

    # 4) If everything is filled, enforce NOT NULL (don’t brick your DB if old rows are messy)
    remaining_null = bind.execute(text("SELECT COUNT(*) FROM leases WHERE org_id IS NULL")).scalar()  # type: ignore
    if int(remaining_null or 0) == 0:
        op.alter_column("leases", "org_id", existing_type=sa.Integer(), nullable=False)

    # 5) Indexes used by your queries
    idxs = _indexes("leases")
    if "ix_leases_org_id" not in idxs:
        op.create_index("ix_leases_org_id", "leases", ["org_id"], unique=False)

    # Your failing query filters org_id + property_id then orders by id desc
    if "ix_leases_org_property_id" not in idxs:
        op.create_index("ix_leases_org_property_id", "leases", ["org_id", "property_id"], unique=False)

    # 6) Optional FK to org table — but we must guess the table name safely.
    # Your schema likely uses "organizations" or "orgs". We’ll create FK only if we find a match.
    fks = _fks("leases")
    org_table = None
    if _has_table("organizations"):
        org_table = "organizations"
    elif _has_table("orgs"):
        org_table = "orgs"

    if org_table and "fk_leases_org_id" not in fks:
        op.create_foreign_key(
            "fk_leases_org_id",
            "leases",
            org_table,
            ["org_id"],
            ["id"],
            ondelete="CASCADE",
        )


def downgrade() -> None:
    if not _has_table("leases"):
        return

    fks = _fks("leases")
    if "fk_leases_org_id" in fks:
        op.drop_constraint("fk_leases_org_id", "leases", type_="foreignkey")

    idxs = _indexes("leases")
    if "ix_leases_org_property_id" in idxs:
        op.drop_index("ix_leases_org_property_id", table_name="leases")
    if "ix_leases_org_id" in idxs:
        op.drop_index("ix_leases_org_id", table_name="leases")

    cols = _cols("leases")
    if "org_id" in cols:
        op.drop_column("leases", "org_id")