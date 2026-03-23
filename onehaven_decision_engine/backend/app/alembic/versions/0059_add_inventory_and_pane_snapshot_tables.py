"""add inventory and pane snapshot tables"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "0059_add_inventory_and_pane_snapshot_tables"
down_revision = "0058_add_org_locks"
branch_labels = None
depends_on = None


def _insp():
    return inspect(op.get_bind())


def _has_table(name: str) -> bool:
    return name in _insp().get_table_names()


def _has_column(table: str, col: str) -> bool:
    if not _has_table(table):
        return False
    return col in {c["name"] for c in _insp().get_columns(table)}


def _has_index(table: str, index_name: str) -> bool:
    if not _has_table(table):
        return False
    return index_name in {idx["name"] for idx in _insp().get_indexes(table)}


def _has_unique_constraint(table: str, constraint_name: str) -> bool:
    if not _has_table(table):
        return False
    return constraint_name in {
        c["name"] for c in _insp().get_unique_constraints(table) if c.get("name")
    }


def upgrade() -> None:
    if not _has_table("property_inventory_snapshots"):
        op.create_table(
            "property_inventory_snapshots",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column(
                "org_id",
                sa.Integer(),
                sa.ForeignKey("organizations.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column(
                "property_id",
                sa.Integer(),
                sa.ForeignKey("properties.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("address", sa.String(length=255), nullable=True),
            sa.Column("normalized_address", sa.String(length=400), nullable=True),
            sa.Column("city", sa.String(length=120), nullable=True),
            sa.Column("county", sa.String(length=80), nullable=True),
            sa.Column("state", sa.String(length=2), nullable=True),
            sa.Column("zip", sa.String(length=10), nullable=True),
            sa.Column("lat", sa.Float(), nullable=True),
            sa.Column("lng", sa.Float(), nullable=True),
            sa.Column("geocode_confidence", sa.Float(), nullable=True),
            sa.Column("crime_score", sa.Float(), nullable=True),
            sa.Column("offender_count", sa.Integer(), nullable=True),
            sa.Column("is_red_zone", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("property_type", sa.String(length=60), nullable=True),
            sa.Column("bedrooms", sa.Integer(), nullable=True),
            sa.Column("bathrooms", sa.Float(), nullable=True),
            sa.Column("square_feet", sa.Integer(), nullable=True),
            sa.Column("asking_price", sa.Float(), nullable=True),
            sa.Column("market_rent_estimate", sa.Float(), nullable=True),
            sa.Column("approved_rent_ceiling", sa.Float(), nullable=True),
            sa.Column("section8_fmr", sa.Float(), nullable=True),
            sa.Column("projected_monthly_cashflow", sa.Float(), nullable=True),
            sa.Column("dscr", sa.Float(), nullable=True),
            sa.Column("current_stage", sa.String(length=64), nullable=True),
            sa.Column("current_stage_label", sa.String(length=120), nullable=True),
            sa.Column("current_pane", sa.String(length=64), nullable=True),
            sa.Column("current_pane_label", sa.String(length=120), nullable=True),
            sa.Column("normalized_decision", sa.String(length=20), nullable=True),
            sa.Column("gate_status", sa.String(length=20), nullable=True),
            sa.Column("route_reason", sa.String(length=255), nullable=True),
            sa.Column("completeness", sa.String(length=20), nullable=True),
            sa.Column("is_fully_enriched", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("blockers_json", sa.JSON(), nullable=True),
            sa.Column("next_actions_json", sa.JSON(), nullable=True),
            sa.Column("source_updated_at", sa.DateTime(), nullable=True),
            sa.Column(
                "snapshot_updated_at",
                sa.DateTime(),
                nullable=False,
                server_default=sa.text("CURRENT_TIMESTAMP"),
            ),
            sa.UniqueConstraint(
                "org_id",
                "property_id",
                name="uq_property_inventory_snapshots_org_property",
            ),
        )

    if _has_table("property_inventory_snapshots"):
        if not _has_index("property_inventory_snapshots", "ix_property_inventory_snapshots_org_stage"):
            op.create_index(
                "ix_property_inventory_snapshots_org_stage",
                "property_inventory_snapshots",
                ["org_id", "current_stage"],
                unique=False,
            )
        if not _has_index("property_inventory_snapshots", "ix_property_inventory_snapshots_org_pane"):
            op.create_index(
                "ix_property_inventory_snapshots_org_pane",
                "property_inventory_snapshots",
                ["org_id", "current_pane"],
                unique=False,
            )
        if not _has_index("property_inventory_snapshots", "ix_property_inventory_snapshots_org_decision"):
            op.create_index(
                "ix_property_inventory_snapshots_org_decision",
                "property_inventory_snapshots",
                ["org_id", "normalized_decision"],
                unique=False,
            )
        if not _has_index("property_inventory_snapshots", "ix_property_inventory_snapshots_org_county"):
            op.create_index(
                "ix_property_inventory_snapshots_org_county",
                "property_inventory_snapshots",
                ["org_id", "county"],
                unique=False,
            )
        if not _has_index("property_inventory_snapshots", "ix_property_inventory_snapshots_org_city"):
            op.create_index(
                "ix_property_inventory_snapshots_org_city",
                "property_inventory_snapshots",
                ["org_id", "city"],
                unique=False,
            )
        if not _has_index("property_inventory_snapshots", "ix_property_inventory_snapshots_org_state"):
            op.create_index(
                "ix_property_inventory_snapshots_org_state",
                "property_inventory_snapshots",
                ["org_id", "state"],
                unique=False,
            )
        if not _has_index("property_inventory_snapshots", "ix_property_inventory_snapshots_org_enriched"):
            op.create_index(
                "ix_property_inventory_snapshots_org_enriched",
                "property_inventory_snapshots",
                ["org_id", "is_fully_enriched"],
                unique=False,
            )
        if not _has_index("property_inventory_snapshots", "ix_property_inventory_snapshots_org_updated"):
            op.create_index(
                "ix_property_inventory_snapshots_org_updated",
                "property_inventory_snapshots",
                ["org_id", "snapshot_updated_at"],
                unique=False,
            )

    if not _has_table("pane_summary_snapshots"):
        op.create_table(
            "pane_summary_snapshots",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column(
                "org_id",
                sa.Integer(),
                sa.ForeignKey("organizations.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("scope_hash", sa.String(length=96), nullable=False),
            sa.Column("pane_key", sa.String(length=64), nullable=False),
            sa.Column("state_filter", sa.String(length=2), nullable=True),
            sa.Column("county_filter", sa.String(length=80), nullable=True),
            sa.Column("city_filter", sa.String(length=120), nullable=True),
            sa.Column("q_filter", sa.String(length=255), nullable=True),
            sa.Column("assigned_user_id_filter", sa.Integer(), nullable=True),
            sa.Column("property_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
            sa.Column("kpis_json", sa.JSON(), nullable=True),
            sa.Column("top_blockers_json", sa.JSON(), nullable=True),
            sa.Column("top_actions_json", sa.JSON(), nullable=True),
            sa.Column(
                "snapshot_updated_at",
                sa.DateTime(),
                nullable=False,
                server_default=sa.text("CURRENT_TIMESTAMP"),
            ),
            sa.UniqueConstraint(
                "org_id",
                "scope_hash",
                "pane_key",
                name="uq_pane_summary_snapshots_org_scope_pane",
            ),
        )

    if _has_table("pane_summary_snapshots"):
        if not _has_index("pane_summary_snapshots", "ix_pane_summary_snapshots_org_pane"):
            op.create_index(
                "ix_pane_summary_snapshots_org_pane",
                "pane_summary_snapshots",
                ["org_id", "pane_key"],
                unique=False,
            )
        if not _has_index("pane_summary_snapshots", "ix_pane_summary_snapshots_org_updated"):
            op.create_index(
                "ix_pane_summary_snapshots_org_updated",
                "pane_summary_snapshots",
                ["org_id", "snapshot_updated_at"],
                unique=False,
            )
        if not _has_index("pane_summary_snapshots", "ix_pane_summary_snapshots_org_scope"):
            op.create_index(
                "ix_pane_summary_snapshots_org_scope",
                "pane_summary_snapshots",
                ["org_id", "scope_hash"],
                unique=False,
            )

    if _has_table("property_inventory_snapshots") and _has_column("property_inventory_snapshots", "snapshot_updated_at"):
        with op.batch_alter_table("property_inventory_snapshots") as batch:
            batch.alter_column("snapshot_updated_at", server_default=None)

    if _has_table("property_inventory_snapshots") and _has_column("property_inventory_snapshots", "is_red_zone"):
        with op.batch_alter_table("property_inventory_snapshots") as batch:
            batch.alter_column("is_red_zone", server_default=None)

    if _has_table("property_inventory_snapshots") and _has_column("property_inventory_snapshots", "is_fully_enriched"):
        with op.batch_alter_table("property_inventory_snapshots") as batch:
            batch.alter_column("is_fully_enriched", server_default=None)

    if _has_table("pane_summary_snapshots") and _has_column("pane_summary_snapshots", "snapshot_updated_at"):
        with op.batch_alter_table("pane_summary_snapshots") as batch:
            batch.alter_column("snapshot_updated_at", server_default=None)

    if _has_table("pane_summary_snapshots") and _has_column("pane_summary_snapshots", "property_count"):
        with op.batch_alter_table("pane_summary_snapshots") as batch:
            batch.alter_column("property_count", server_default=None)


def downgrade() -> None:
    if _has_index("pane_summary_snapshots", "ix_pane_summary_snapshots_org_scope"):
        op.drop_index("ix_pane_summary_snapshots_org_scope", table_name="pane_summary_snapshots")
    if _has_index("pane_summary_snapshots", "ix_pane_summary_snapshots_org_updated"):
        op.drop_index("ix_pane_summary_snapshots_org_updated", table_name="pane_summary_snapshots")
    if _has_index("pane_summary_snapshots", "ix_pane_summary_snapshots_org_pane"):
        op.drop_index("ix_pane_summary_snapshots_org_pane", table_name="pane_summary_snapshots")
    if _has_table("pane_summary_snapshots"):
        op.drop_table("pane_summary_snapshots")

    if _has_index("property_inventory_snapshots", "ix_property_inventory_snapshots_org_updated"):
        op.drop_index("ix_property_inventory_snapshots_org_updated", table_name="property_inventory_snapshots")
    if _has_index("property_inventory_snapshots", "ix_property_inventory_snapshots_org_enriched"):
        op.drop_index("ix_property_inventory_snapshots_org_enriched", table_name="property_inventory_snapshots")
    if _has_index("property_inventory_snapshots", "ix_property_inventory_snapshots_org_state"):
        op.drop_index("ix_property_inventory_snapshots_org_state", table_name="property_inventory_snapshots")
    if _has_index("property_inventory_snapshots", "ix_property_inventory_snapshots_org_city"):
        op.drop_index("ix_property_inventory_snapshots_org_city", table_name="property_inventory_snapshots")
    if _has_index("property_inventory_snapshots", "ix_property_inventory_snapshots_org_county"):
        op.drop_index("ix_property_inventory_snapshots_org_county", table_name="property_inventory_snapshots")
    if _has_index("property_inventory_snapshots", "ix_property_inventory_snapshots_org_decision"):
        op.drop_index("ix_property_inventory_snapshots_org_decision", table_name="property_inventory_snapshots")
    if _has_index("property_inventory_snapshots", "ix_property_inventory_snapshots_org_pane"):
        op.drop_index("ix_property_inventory_snapshots_org_pane", table_name="property_inventory_snapshots")
    if _has_index("property_inventory_snapshots", "ix_property_inventory_snapshots_org_stage"):
        op.drop_index("ix_property_inventory_snapshots_org_stage", table_name="property_inventory_snapshots")
    if _has_table("property_inventory_snapshots"):
        op.drop_table("property_inventory_snapshots")
        