"""multitenant rbac audit + workflow log

Revision ID: 0011_multitenant_rbac_audit_eventlog
Revises: 0010_agent_slots
Create Date: 2026-02-14
"""
from alembic import op
import sqlalchemy as sa


revision = "0011_multitenant_rbac_audit_eventlog"
down_revision = "0010_agent_slots"
branch_labels = None
depends_on = None


def upgrade():
    # -------------------------
    # Orgs / Users / Membership
    # -------------------------
    op.create_table(
        "organizations",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("slug", sa.String(length=80), nullable=False),
        sa.Column("name", sa.String(length=160), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_organizations_slug", "organizations", ["slug"], unique=True)

    op.create_table(
        "app_users",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("email", sa.String(length=200), nullable=False),
        sa.Column("display_name", sa.String(length=160), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_app_users_email", "app_users", ["email"], unique=True)

    op.create_table(
        "org_memberships",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), sa.ForeignKey("organizations.id"), nullable=False),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("app_users.id"), nullable=False),
        sa.Column("role", sa.String(length=20), nullable=False, server_default="owner"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("org_id", "user_id", name="uq_org_memberships_org_user"),
    )
    op.create_index("ix_org_memberships_org_id", "org_memberships", ["org_id"])
    op.create_index("ix_org_memberships_user_id", "org_memberships", ["user_id"])

    # -------------------------
    # Tenant scoping columns
    # -------------------------
    # properties
    op.add_column("properties", sa.Column("org_id", sa.Integer(), nullable=True))
    op.create_index("ix_properties_org_id", "properties", ["org_id"])
    op.create_foreign_key("fk_properties_org_id", "properties", "organizations", ["org_id"], ["id"])

    # deals
    op.add_column("deals", sa.Column("org_id", sa.Integer(), nullable=True))
    op.create_index("ix_deals_org_id", "deals", ["org_id"])
    op.create_foreign_key("fk_deals_org_id", "deals", "organizations", ["org_id"], ["id"])

    # rent_assumptions
    op.add_column("rent_assumptions", sa.Column("org_id", sa.Integer(), nullable=True))
    op.create_index("ix_rent_assumptions_org_id", "rent_assumptions", ["org_id"])
    op.create_foreign_key("fk_rent_assumptions_org_id", "rent_assumptions", "organizations", ["org_id"], ["id"])

    # agent_run + agent_message + agent_slot_assignments
    op.add_column("agent_runs", sa.Column("org_id", sa.Integer(), nullable=True))
    op.create_index("ix_agent_runs_org_id", "agent_runs", ["org_id"])
    op.create_foreign_key("fk_agent_runs_org_id", "agent_runs", "organizations", ["org_id"], ["id"])

    op.add_column("agent_messages", sa.Column("org_id", sa.Integer(), nullable=True))
    op.create_index("ix_agent_messages_org_id", "agent_messages", ["org_id"])
    op.create_foreign_key("fk_agent_messages_org_id", "agent_messages", "organizations", ["org_id"], ["id"])

    op.add_column("agent_slot_assignments", sa.Column("org_id", sa.Integer(), nullable=True))
    op.create_index("ix_agent_slot_assignments_org_id", "agent_slot_assignments", ["org_id"])
    op.create_foreign_key("fk_agent_slot_assignments_org_id", "agent_slot_assignments", "organizations", ["org_id"], ["id"])

    # underwriting_results
    op.add_column("underwriting_results", sa.Column("org_id", sa.Integer(), nullable=True))
    op.create_index("ix_underwriting_results_org_id", "underwriting_results", ["org_id"])
    op.create_foreign_key("fk_underwriting_results_org_id", "underwriting_results", "organizations", ["org_id"], ["id"])

    # -------------------------
    # Audit (append-only)
    # -------------------------
    op.create_table(
        "audit_events",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), sa.ForeignKey("organizations.id"), nullable=False),
        sa.Column("actor_user_id", sa.Integer(), sa.ForeignKey("app_users.id"), nullable=True),
        sa.Column("action", sa.String(length=80), nullable=False),
        sa.Column("entity_type", sa.String(length=80), nullable=False),
        sa.Column("entity_id", sa.String(length=80), nullable=False),
        sa.Column("before_json", sa.Text(), nullable=True),
        sa.Column("after_json", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_audit_events_org_id", "audit_events", ["org_id"])
    op.create_index("ix_audit_events_entity", "audit_events", ["entity_type", "entity_id"])

    # -------------------------
    # Blackboard + Event Log
    # -------------------------
    op.create_table(
        "property_states",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), sa.ForeignKey("organizations.id"), nullable=False),
        sa.Column("property_id", sa.Integer(), sa.ForeignKey("properties.id"), nullable=False),
        sa.Column("current_stage", sa.String(length=30), nullable=False, server_default="deal"),
        sa.Column("constraints_json", sa.Text(), nullable=True),
        sa.Column("outstanding_tasks_json", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("org_id", "property_id", name="uq_property_states_org_property"),
    )
    op.create_index("ix_property_states_org_id", "property_states", ["org_id"])
    op.create_index("ix_property_states_property_id", "property_states", ["property_id"])

    op.create_table(
        "workflow_events",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("org_id", sa.Integer(), sa.ForeignKey("organizations.id"), nullable=False),
        sa.Column("property_id", sa.Integer(), sa.ForeignKey("properties.id"), nullable=True),
        sa.Column("actor_user_id", sa.Integer(), sa.ForeignKey("app_users.id"), nullable=True),
        sa.Column("event_type", sa.String(length=80), nullable=False),
        sa.Column("payload_json", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_workflow_events_org_id", "workflow_events", ["org_id"])
    op.create_index("ix_workflow_events_property_id", "workflow_events", ["property_id"])
    op.create_index("ix_workflow_events_type", "workflow_events", ["event_type"])


def downgrade():
    op.drop_index("ix_workflow_events_type", table_name="workflow_events")
    op.drop_index("ix_workflow_events_property_id", table_name="workflow_events")
    op.drop_index("ix_workflow_events_org_id", table_name="workflow_events")
    op.drop_table("workflow_events")

    op.drop_index("ix_property_states_property_id", table_name="property_states")
    op.drop_index("ix_property_states_org_id", table_name="property_states")
    op.drop_table("property_states")

    op.drop_index("ix_audit_events_entity", table_name="audit_events")
    op.drop_index("ix_audit_events_org_id", table_name="audit_events")
    op.drop_table("audit_events")

    op.drop_constraint("fk_underwriting_results_org_id", "underwriting_results", type_="foreignkey")
    op.drop_index("ix_underwriting_results_org_id", table_name="underwriting_results")
    op.drop_column("underwriting_results", "org_id")

    op.drop_constraint("fk_agent_slot_assignments_org_id", "agent_slot_assignments", type_="foreignkey")
    op.drop_index("ix_agent_slot_assignments_org_id", table_name="agent_slot_assignments")
    op.drop_column("agent_slot_assignments", "org_id")

    op.drop_constraint("fk_agent_messages_org_id", "agent_messages", type_="foreignkey")
    op.drop_index("ix_agent_messages_org_id", table_name="agent_messages")
    op.drop_column("agent_messages", "org_id")

    op.drop_constraint("fk_agent_runs_org_id", "agent_runs", type_="foreignkey")
    op.drop_index("ix_agent_runs_org_id", table_name="agent_runs")
    op.drop_column("agent_runs", "org_id")

    op.drop_constraint("fk_rent_assumptions_org_id", "rent_assumptions", type_="foreignkey")
    op.drop_index("ix_rent_assumptions_org_id", table_name="rent_assumptions")
    op.drop_column("rent_assumptions", "org_id")

    op.drop_constraint("fk_deals_org_id", "deals", type_="foreignkey")
    op.drop_index("ix_deals_org_id", table_name="deals")
    op.drop_column("deals", "org_id")

    op.drop_constraint("fk_properties_org_id", "properties", type_="foreignkey")
    op.drop_index("ix_properties_org_id", table_name="properties")
    op.drop_column("properties", "org_id")

    op.drop_index("ix_org_memberships_user_id", table_name="org_memberships")
    op.drop_index("ix_org_memberships_org_id", table_name="org_memberships")
    op.drop_table("org_memberships")

    op.drop_index("ix_app_users_email", table_name="app_users")
    op.drop_table("app_users")

    op.drop_index("ix_organizations_slug", table_name="organizations")
    op.drop_table("organizations")
