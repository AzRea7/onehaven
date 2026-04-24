"""seed jurisdiction defaults (global)

Revision ID: 0018_seed_jurisdiction_defaults
Revises: 0017_rent_explain_runs
Create Date: 2026-02-20
"""
from __future__ import annotations

from datetime import datetime
import json

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = "0018_seed_jurisdiction_defaults"
down_revision = "0017_rent_explain_runs"
branch_labels = None
depends_on = None


def _col_exists(conn, table: str, col: str) -> bool:
    insp = inspect(conn)
    return any(c["name"] == col for c in insp.get_columns(table))


def _add_col_if_missing(table: str, col: sa.Column) -> None:
    conn = op.get_bind()
    if not _col_exists(conn, table, col.name):
        op.add_column(table, col)


def upgrade() -> None:
    conn = op.get_bind()
    now = datetime.utcnow()

    # ---- 1) Bring legacy table up to current schema (bridge) ----
    # These columns are referenced by the seed insert below,
    # but older DBs may not have them yet.
    _add_col_if_missing("jurisdiction_rules", sa.Column("notes", sa.Text(), nullable=True))

    _add_col_if_missing(
        "jurisdiction_rules",
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("NOW()")),
    )
    _add_col_if_missing(
        "jurisdiction_rules",
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("NOW()")),
    )

    # ---- 2) Seed defaults (global scope: org_id IS NULL) ----
    jr = sa.table(
        "jurisdiction_rules",
        sa.column("id", sa.Integer),
        sa.column("org_id", sa.Integer),
        sa.column("city", sa.String),
        sa.column("state", sa.String),
        sa.column("rental_license_required", sa.Boolean),
        sa.column("inspection_authority", sa.String),
        sa.column("typical_fail_points_json", sa.Text),
        sa.column("registration_fee", sa.Float),
        sa.column("processing_days", sa.Integer),
        sa.column("inspection_frequency", sa.String),
        sa.column("tenant_waitlist_depth", sa.String),
        sa.column("notes", sa.Text),
        sa.column("created_at", sa.DateTime),
        sa.column("updated_at", sa.DateTime),
    )

    defaults = [
        dict(
            city="Detroit",
            state="MI",
            rental_license_required=True,
            inspection_authority="City of Detroit",
            inspection_frequency="annual",
            typical_fail_points_json=json.dumps(
                ["GFCI missing", "handrails", "peeling paint", "smoke/CO detectors", "broken windows"],
                sort_keys=True,
            ),
            processing_days=21,
            tenant_waitlist_depth="high",
            notes="Baseline default. Override per neighborhood/authority if needed.",
        ),
        dict(
            city="Pontiac",
            state="MI",
            rental_license_required=True,
            inspection_authority="City of Pontiac",
            inspection_frequency="annual",
            typical_fail_points_json=json.dumps(
                ["GFCI missing", "peeling paint", "egress issues", "utilities not secured"],
                sort_keys=True,
            ),
            processing_days=14,
            tenant_waitlist_depth="medium",
            notes="Baseline default. Confirm local registration/fees.",
        ),
        dict(
            city="Southfield",
            state="MI",
            rental_license_required=True,
            inspection_authority="City of Southfield",
            inspection_frequency="periodic",
            typical_fail_points_json=json.dumps(
                ["GFCI missing", "smoke/CO detectors", "handrails", "trip hazards"],
                sort_keys=True,
            ),
            processing_days=14,
            tenant_waitlist_depth="medium",
            notes="Baseline default. Verify rental certification steps.",
        ),
        dict(
            city="Inkster",
            state="MI",
            rental_license_required=True,
            inspection_authority="City of Inkster",
            inspection_frequency="annual",
            typical_fail_points_json=json.dumps(
                ["peeling paint", "broken windows", "missing detectors", "handrails", "GFCI missing"],
                sort_keys=True,
            ),
            processing_days=21,
            tenant_waitlist_depth="high",
            notes="Baseline default. Many older housing stock issues.",
        ),
        dict(
            city="Dearborn",
            state="MI",
            rental_license_required=True,
            inspection_authority="City of Dearborn",
            inspection_frequency="periodic",
            typical_fail_points_json=json.dumps(
                ["handrails", "GFCI missing", "egress", "detectors"],
                sort_keys=True,
            ),
            processing_days=10,
            tenant_waitlist_depth="medium",
            notes="Baseline default. Verify frequency by license type.",
        ),
        dict(
            city="Warren",
            state="MI",
            rental_license_required=True,
            inspection_authority="City of Warren",
            inspection_frequency="periodic",
            typical_fail_points_json=json.dumps(
                ["GFCI missing", "detectors", "handrails", "egress"],
                sort_keys=True,
            ),
            processing_days=10,
            tenant_waitlist_depth="medium",
            notes="Baseline default.",
        ),
        dict(
            city="Royal Oak",
            state="MI",
            rental_license_required=True,
            inspection_authority="City of Royal Oak",
            inspection_frequency="periodic",
            typical_fail_points_json=json.dumps(
                ["handrails", "GFCI missing", "smoke/CO detectors", "egress"],
                sort_keys=True,
            ),
            processing_days=10,
            tenant_waitlist_depth="medium",
            notes="Baseline default. Verify frequency by rental license type.",
        ),
    ]

    for d in defaults:
        exists = conn.execute(
            sa.text(
                """
                SELECT 1 FROM jurisdiction_rules
                WHERE org_id IS NULL AND lower(city)=lower(:city) AND state=:state
                LIMIT 1
                """
            ),
            {"city": d["city"], "state": d["state"]},
        ).fetchone()

        if exists:
            continue

        conn.execute(
            sa.insert(jr).values(
                org_id=None,
                city=d["city"].strip().title(),
                state=d["state"].strip().upper(),
                rental_license_required=bool(d.get("rental_license_required", False)),
                inspection_authority=d.get("inspection_authority"),
                inspection_frequency=d.get("inspection_frequency"),
                typical_fail_points_json=d.get("typical_fail_points_json") or "[]",
                registration_fee=d.get("registration_fee"),
                processing_days=d.get("processing_days"),
                tenant_waitlist_depth=d.get("tenant_waitlist_depth"),
                notes=d.get("notes"),
                created_at=now,
                updated_at=now,
            )
        )


def downgrade() -> None:
    # Keep seeded rows on downgrade (safe no-op).
    pass