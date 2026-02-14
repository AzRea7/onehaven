from alembic import op
import sqlalchemy as sa

revision = "0013_property_checklist_items_and_org_scoping"
down_revision = "0012_fix_org_scoping_not_null"
branch_labels = None
depends_on = None


def _has_table(conn, name: str) -> bool:
    return sa.inspect(conn).has_table(name)


def _has_column(conn, table: str, col: str) -> bool:
    insp = sa.inspect(conn)
    cols = [c["name"] for c in insp.get_columns(table)]
    return col in cols


def upgrade():
    conn = op.get_bind()

    # ------------------------------------------------------------
    # 0) Ensure property_checklists exists (dev-safe / reset-safe)
    # ------------------------------------------------------------
    if not _has_table(conn, "property_checklists"):
        op.create_table(
            "property_checklists",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("property_id", sa.Integer(), sa.ForeignKey("properties.id", ondelete="CASCADE"), nullable=False, index=True),
            sa.Column("strategy", sa.String(20), nullable=False, server_default="section8"),
            sa.Column("version", sa.String(32), nullable=False, server_default="v1"),
            sa.Column("generated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.Column("items_json", sa.Text(), nullable=False, server_default="[]"),
        )

    # ------------------------------------------------------------
    # 1) Add org_id to property_checklists (org-safe reads/writes)
    # ------------------------------------------------------------
    if not _has_column(conn, "property_checklists", "org_id"):
        with op.batch_alter_table("property_checklists") as batch:
            batch.add_column(sa.Column("org_id", sa.Integer(), nullable=True))
            batch.create_index("ix_property_checklists_org_id", ["org_id"])
            batch.create_foreign_key("fk_property_checklists_org", "organizations", ["org_id"], ["id"])

        # Backfill org_id from properties.org_id
        conn.execute(
            sa.text(
                """
                update property_checklists pc
                set org_id = p.org_id
                from properties p
                where pc.property_id = p.id
                  and pc.org_id is null
                """
            )
        )

        # Enforce NOT NULL after backfill
        op.alter_column("property_checklists", "org_id", existing_type=sa.Integer(), nullable=False)

    # Optional but strongly recommended: uniqueness to avoid duplicates
    # (org, property, strategy, version) represents "a checklist snapshot" key
    # Create it only if it doesn't already exist
    # Alembic doesn't have "IF NOT EXISTS" for constraints; we use raw SQL guard.
    conn.execute(
        sa.text(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_constraint
                    WHERE conname = 'uq_property_checklists_org_property_strategy_version'
                ) THEN
                    ALTER TABLE property_checklists
                    ADD CONSTRAINT uq_property_checklists_org_property_strategy_version
                    UNIQUE (org_id, property_id, strategy, version);
                END IF;
            END$$;
            """
        )
    )

    # ------------------------------------------------------------
    # 2) Create property_checklist_items table (normalized state)
    # ------------------------------------------------------------
    if not _has_table(conn, "property_checklist_items"):
        op.create_table(
            "property_checklist_items",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("org_id", sa.Integer(), sa.ForeignKey("organizations.id"), nullable=False, index=True),
            sa.Column("property_id", sa.Integer(), sa.ForeignKey("properties.id", ondelete="CASCADE"), nullable=False, index=True),
            sa.Column("checklist_id", sa.Integer(), sa.ForeignKey("property_checklists.id", ondelete="CASCADE"), nullable=True, index=True),

            sa.Column("item_code", sa.String(80), nullable=False),
            sa.Column("category", sa.String(80), nullable=False),
            sa.Column("description", sa.Text(), nullable=False),
            sa.Column("severity", sa.Integer(), nullable=False, server_default="2"),
            sa.Column("common_fail", sa.Boolean(), nullable=False, server_default=sa.text("true")),
            sa.Column("applies_if_json", sa.Text(), nullable=True),

            # workflow fields (Phase 3 DoD)
            sa.Column("status", sa.String(20), nullable=False, server_default="todo"),  # todo|in_progress|done|blocked
            sa.Column("marked_by_user_id", sa.Integer(), sa.ForeignKey("app_users.id"), nullable=True),
            sa.Column("marked_at", sa.DateTime(), nullable=True),
            sa.Column("proof_url", sa.Text(), nullable=True),
            sa.Column("notes", sa.Text(), nullable=True),

            sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
            sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),

            sa.UniqueConstraint("org_id", "property_id", "item_code", name="uq_checklist_item_org_property_code"),
        )

        op.create_index(
            "ix_property_checklist_items_property_status",
            "property_checklist_items",
            ["property_id", "status"],
        )
    else:
        # If table already exists, ensure helpful index exists
        conn.execute(
            sa.text(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM pg_class c
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE c.relname = 'ix_property_checklist_items_property_status'
                    ) THEN
                        CREATE INDEX ix_property_checklist_items_property_status
                        ON property_checklist_items (property_id, status);
                    END IF;
                END$$;
                """
            )
        )


def downgrade():
    # drop items index + table
    try:
        op.drop_index("ix_property_checklist_items_property_status", table_name="property_checklist_items")
    except Exception:
        pass
    try:
        op.drop_table("property_checklist_items")
    except Exception:
        pass

    # remove unique constraint on property_checklists (if exists)
    conn = op.get_bind()
    if _has_table(conn, "property_checklists"):
        try:
            op.drop_constraint(
                "uq_property_checklists_org_property_strategy_version",
                "property_checklists",
                type_="unique",
            )
        except Exception:
            pass

        # keep org_id column (safer) OR drop it (your call).
        # If you insist on dropping:
        # with op.batch_alter_table("property_checklists") as batch:
        #     batch.drop_index("ix_property_checklists_org_id")
        #     batch.drop_column("org_id")
