from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "0091_add_jurisdiction_registry"
down_revision = "0090_add_property_compliance_resolution_snapshots"
branch_labels = None
depends_on = None


def _insp():
    return inspect(op.get_bind())


def _has_table(name: str) -> bool:
    return name in _insp().get_table_names()


def _has_column(table: str, column: str) -> bool:
    if not _has_table(table):
        return False
    return column in {col["name"] for col in _insp().get_columns(table)}


def _has_index(table: str, idx_name: str) -> bool:
    if not _has_table(table):
        return False
    return idx_name in {idx["name"] for idx in _insp().get_indexes(table)}


def _has_uc(table: str, uc_name: str) -> bool:
    if not _has_table(table):
        return False
    return uc_name in {uc["name"] for uc in _insp().get_unique_constraints(table)}


def _has_fk(table: str, fk_name: str) -> bool:
    if not _has_table(table):
        return False
    return fk_name in {fk["name"] for fk in _insp().get_foreign_keys(table) if fk.get("name")}


def _create_registry_table() -> None:
    op.create_table(
        "jurisdiction_registry",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), nullable=True),
        sa.Column("jurisdiction_type", sa.String(length=40), nullable=False),
        sa.Column("state_code", sa.String(length=8), nullable=True),
        sa.Column("state_name", sa.String(length=120), nullable=True),
        sa.Column("county_name", sa.String(length=120), nullable=True),
        sa.Column("city_name", sa.String(length=120), nullable=True),
        sa.Column("display_name", sa.String(length=255), nullable=False),
        sa.Column("slug", sa.String(length=255), nullable=False),
        sa.Column("geoid", sa.String(length=40), nullable=True),
        sa.Column("lsad", sa.String(length=40), nullable=True),
        sa.Column("census_class", sa.String(length=40), nullable=True),
        sa.Column("parent_jurisdiction_id", sa.Integer(), nullable=True),
        sa.Column("official_website", sa.Text(), nullable=True),
        sa.Column("onboarding_status", sa.String(length=40), nullable=False, server_default="discovered"),
        sa.Column("source_confidence", sa.Float(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("last_reviewed_at", sa.DateTime(), nullable=True),
        sa.Column("source_map_json", sa.Text(), nullable=True),
        sa.Column("metadata_json", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(
            ["org_id"],
            ["organizations.id"],
            name="fk_jurisdiction_registry_org_id",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["parent_jurisdiction_id"],
            ["jurisdiction_registry.id"],
            name="fk_jurisdiction_registry_parent_id",
        ),
    )


def upgrade() -> None:
    if not _has_table("jurisdiction_registry"):
        _create_registry_table()

    # Add any missing columns for drifted environments.
    missing_columns = [
        ("org_id", sa.Integer(), True),
        ("jurisdiction_type", sa.String(length=40), False),
        ("state_code", sa.String(length=8), True),
        ("state_name", sa.String(length=120), True),
        ("county_name", sa.String(length=120), True),
        ("city_name", sa.String(length=120), True),
        ("display_name", sa.String(length=255), False),
        ("slug", sa.String(length=255), False),
        ("geoid", sa.String(length=40), True),
        ("lsad", sa.String(length=40), True),
        ("census_class", sa.String(length=40), True),
        ("parent_jurisdiction_id", sa.Integer(), True),
        ("official_website", sa.Text(), True),
        ("onboarding_status", sa.String(length=40), False),
        ("source_confidence", sa.Float(), True),
        ("is_active", sa.Boolean(), False),
        ("last_reviewed_at", sa.DateTime(), True),
        ("source_map_json", sa.Text(), True),
        ("metadata_json", sa.Text(), True),
        ("created_at", sa.DateTime(), False),
        ("updated_at", sa.DateTime(), False),
    ]

    for name, col_type, nullable in missing_columns:
        if not _has_column("jurisdiction_registry", name):
            kwargs = {"nullable": nullable}
            if name == "onboarding_status":
                kwargs["server_default"] = "discovered"
            elif name == "is_active":
                kwargs["server_default"] = sa.text("true")
            elif name in {"created_at", "updated_at"}:
                kwargs["server_default"] = sa.text("now()")
            op.add_column("jurisdiction_registry", sa.Column(name, col_type, **kwargs))

    if not _has_fk("jurisdiction_registry", "fk_jurisdiction_registry_org_id"):
        with op.batch_alter_table("jurisdiction_registry") as batch:
            batch.create_foreign_key(
                "fk_jurisdiction_registry_org_id",
                "organizations",
                ["org_id"],
                ["id"],
                ondelete="CASCADE",
            )

    if not _has_fk("jurisdiction_registry", "fk_jurisdiction_registry_parent_id"):
        with op.batch_alter_table("jurisdiction_registry") as batch:
            batch.create_foreign_key(
                "fk_jurisdiction_registry_parent_id",
                "jurisdiction_registry",
                ["parent_jurisdiction_id"],
                ["id"],
            )

    if not _has_uc("jurisdiction_registry", "uq_jurisdiction_registry_org_slug"):
        with op.batch_alter_table("jurisdiction_registry") as batch:
            batch.create_unique_constraint(
                "uq_jurisdiction_registry_org_slug",
                ["org_id", "slug"],
            )

    if not _has_index("jurisdiction_registry", "ix_jurisdiction_registry_slug"):
        op.create_index(
            "ix_jurisdiction_registry_slug",
            "jurisdiction_registry",
            ["slug"],
        )

    if not _has_index("jurisdiction_registry", "ix_jurisdiction_registry_scope"):
        op.create_index(
            "ix_jurisdiction_registry_scope",
            "jurisdiction_registry",
            ["state_code", "county_name", "city_name", "jurisdiction_type"],
        )

    if not _has_index("jurisdiction_registry", "ix_jurisdiction_registry_org_id"):
        op.create_index(
            "ix_jurisdiction_registry_org_id",
            "jurisdiction_registry",
            ["org_id"],
        )

    if not _has_index("jurisdiction_registry", "ix_jurisdiction_registry_parent_id"):
        op.create_index(
            "ix_jurisdiction_registry_parent_id",
            "jurisdiction_registry",
            ["parent_jurisdiction_id"],
        )

    if not _has_index("jurisdiction_registry", "ix_jurisdiction_registry_onboarding_status"):
        op.create_index(
            "ix_jurisdiction_registry_onboarding_status",
            "jurisdiction_registry",
            ["onboarding_status"],
        )

    if not _has_index("jurisdiction_registry", "ix_jurisdiction_registry_is_active"):
        op.create_index(
            "ix_jurisdiction_registry_is_active",
            "jurisdiction_registry",
            ["is_active"],
        )

    # Backfill conservative defaults for drifted rows.
    op.execute(
        sa.text(
            """
            UPDATE jurisdiction_registry
            SET onboarding_status = COALESCE(NULLIF(onboarding_status, ''), 'discovered')
            """
        )
    )
    op.execute(
        sa.text(
            """
            UPDATE jurisdiction_registry
            SET is_active = COALESCE(is_active, true)
            """
        )
    )
    op.execute(
        sa.text(
            """
            UPDATE jurisdiction_registry
            SET display_name = COALESCE(NULLIF(display_name, ''), slug, 'Unknown jurisdiction')
            """
        )
    )


def downgrade() -> None:
    # Conservative downgrade: leave table/data intact.
    pass
