from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "0092_add_registry_completeness_metadata"
down_revision = "0091_add_jurisdiction_registry"
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


def upgrade() -> None:
    if not _has_table("jurisdiction_registry"):
        return

    missing_columns = [
        ("source_authority_score", sa.Float(), True, None),
        ("expected_categories_json", sa.JSON(), False, sa.text("'{}'")),
        ("required_categories_json", sa.JSON(), False, sa.text("'{}'")),
        ("coverage_metadata_json", sa.JSON(), False, sa.text("'{}'")),
    ]
    for name, col_type, nullable, default in missing_columns:
        if not _has_column("jurisdiction_registry", name):
            kwargs = {"nullable": nullable}
            if default is not None:
                kwargs["server_default"] = default
            op.add_column("jurisdiction_registry", sa.Column(name, col_type, **kwargs))

    op.execute(
        sa.text(
            '''
            UPDATE jurisdiction_registry
            SET expected_categories_json = COALESCE(expected_categories_json, '{}'::json),
                required_categories_json = COALESCE(required_categories_json, '{}'::json),
                coverage_metadata_json = COALESCE(coverage_metadata_json, '{}'::json)
            '''
        )
    )


def downgrade() -> None:
    # Conservative downgrade: leave data intact.
    pass
