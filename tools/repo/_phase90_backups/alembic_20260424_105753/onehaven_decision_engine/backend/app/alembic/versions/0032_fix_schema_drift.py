"""fix schema drift agent_runs + rehab_tasks + add underwriting_results.rent_explain_run_id

Revision ID: 0032_fix_schema_drift
Revises: 0031_add_saas_tables
Create Date: 2026-02-28
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect, text

revision = "0032_fix_schema_drift"
down_revision = "0031_add_saas_tables"
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

    # -----------------------------
    # underwriting_results
    # add: rent_explain_run_id
    # -----------------------------
    if _has_table("underwriting_results"):
        cols = _cols("underwriting_results")
        if "rent_explain_run_id" not in cols:
            op.add_column("underwriting_results", sa.Column("rent_explain_run_id", sa.Integer(), nullable=True))

        # FK + index (safe-create)
        fks = _fks("underwriting_results")
        if "fk_underwriting_results_rent_explain_run_id" not in fks:
            op.create_foreign_key(
                "fk_underwriting_results_rent_explain_run_id",
                "underwriting_results",
                "rent_explain_runs",
                ["rent_explain_run_id"],
                ["id"],
                ondelete="SET NULL",
            )

        idxs = _indexes("underwriting_results")
        if "ix_underwriting_results_rent_explain_run_id" not in idxs:
            op.create_index(
                "ix_underwriting_results_rent_explain_run_id",
                "underwriting_results",
                ["rent_explain_run_id"],
                unique=False,
            )

    # -----------------------------
    # agent_runs
    # Code expects: input_json, output_json
    # DB currently has: payload_json, result_json
    # -----------------------------
    if _has_table("agent_runs"):
        cols = _cols("agent_runs")

        if "input_json" not in cols:
            op.add_column("agent_runs", sa.Column("input_json", sa.Text(), nullable=True))
        if "output_json" not in cols:
            op.add_column("agent_runs", sa.Column("output_json", sa.Text(), nullable=True))

        cols = _cols("agent_runs")

        if "payload_json" in cols:
            bind.execute(text("UPDATE agent_runs SET input_json = payload_json WHERE input_json IS NULL"))
        if "result_json" in cols:
            bind.execute(text("UPDATE agent_runs SET output_json = result_json WHERE output_json IS NULL"))

        idxs = _indexes("agent_runs")
        if "ix_agent_runs_input_json" not in idxs:
            op.create_index("ix_agent_runs_input_json", "agent_runs", ["input_json"], unique=False)
        if "ix_agent_runs_output_json" not in idxs:
            op.create_index("ix_agent_runs_output_json", "agent_runs", ["output_json"], unique=False)

    # -----------------------------
    # rehab_tasks
    # Code expects: rehab_tasks.org_id
    # DB currently: no org_id column
    # Backfill from properties.org_id
    # -----------------------------
    if _has_table("rehab_tasks"):
        cols = _cols("rehab_tasks")

        if "org_id" not in cols:
            op.add_column("rehab_tasks", sa.Column("org_id", sa.Integer(), nullable=True))

        bind.execute(
            text(
                """
                UPDATE rehab_tasks rt
                SET org_id = p.org_id
                FROM properties p
                WHERE rt.property_id = p.id
                  AND rt.org_id IS NULL
                """
            )
        )

        idxs = _indexes("rehab_tasks")
        if "ix_rehab_tasks_org_id" not in idxs:
            op.create_index("ix_rehab_tasks_org_id", "rehab_tasks", ["org_id"], unique=False)


def downgrade() -> None:
    # rehab_tasks rollback
    if _has_table("rehab_tasks"):
        cols = _cols("rehab_tasks")
        idxs = _indexes("rehab_tasks")

        if "ix_rehab_tasks_org_id" in idxs:
            op.drop_index("ix_rehab_tasks_org_id", table_name="rehab_tasks")
        if "org_id" in cols:
            op.drop_column("rehab_tasks", "org_id")

    # agent_runs rollback
    if _has_table("agent_runs"):
        cols = _cols("agent_runs")
        idxs = _indexes("agent_runs")

        if "ix_agent_runs_output_json" in idxs:
            op.drop_index("ix_agent_runs_output_json", table_name="agent_runs")
        if "ix_agent_runs_input_json" in idxs:
            op.drop_index("ix_agent_runs_input_json", table_name="agent_runs")

        if "output_json" in cols:
            op.drop_column("agent_runs", "output_json")
        if "input_json" in cols:
            op.drop_column("agent_runs", "input_json")

    # underwriting_results rollback
    if _has_table("underwriting_results"):
        idxs = _indexes("underwriting_results")
        fks = _fks("underwriting_results")
        cols = _cols("underwriting_results")

        if "ix_underwriting_results_rent_explain_run_id" in idxs:
            op.drop_index("ix_underwriting_results_rent_explain_run_id", table_name="underwriting_results")
        if "fk_underwriting_results_rent_explain_run_id" in fks:
            op.drop_constraint(
                "fk_underwriting_results_rent_explain_run_id",
                "underwriting_results",
                type_="foreignkey",
            )
        if "rent_explain_run_id" in cols:
            op.drop_column("underwriting_results", "rent_explain_run_id")
            