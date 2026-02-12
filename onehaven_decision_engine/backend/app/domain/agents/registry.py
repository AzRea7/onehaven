# backend/app/domain/agents/registry.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional


# -----------------------------
# Agent "Specs" (automation-capable)
# -----------------------------
@dataclass(frozen=True)
class AgentSpec:
    key: str
    name: str
    description: str
    default_payload_schema: dict[str, Any]


# NOTE: keep these lightweight: "specs" are contracts.
# Execution comes later (Phase: orchestration).
AGENTS: Dict[str, AgentSpec] = {
    "deal_intake": AgentSpec(
        key="deal_intake",
        name="Deal Intake Scanner",
        description="Normalizes property inputs and flags missing/unsafe data before underwriting.",
        default_payload_schema={"property_id": "int", "source": "zillow|investorlift|manual"},
    ),
    "rent_reasonableness": AgentSpec(
        key="rent_reasonableness",
        name="Rent Reasonableness Packager",
        description="Organizes comps and generates a rent reasonableness narrative for HA packets.",
        default_payload_schema={"property_id": "int", "zip": "str", "bedrooms": "int"},
    ),
    "hqs_precheck": AgentSpec(
        key="hqs_precheck",
        name="HQS Pre-Inspection Checklist",
        description="Generates checklist and highlights predicted fail points from history (when available).",
        default_payload_schema={"property_id": "int", "strategy": "section8"},
    ),
}


# -----------------------------
# Human/AI Workflow Slots (sidebar stations)
# -----------------------------
@dataclass(frozen=True)
class AgentSlotSpec:
    slot_key: str
    title: str
    description: str
    owner_type: str  # "human" | "ai" | "hybrid"
    default_status: str  # "idle" | "queued" | "in_progress" | "blocked" | "done"


# These are your "Section 8 process stations".
# They map 1:1 to operational steps that must be done right.
SLOTS: list[AgentSlotSpec] = [
    AgentSlotSpec(
        slot_key="s8_realtor_intake",
        title="Realtor Intake",
        description="Confirm comps/condition, verify no major red flags, validate neighborhood heuristics.",
        owner_type="human",
        default_status="idle",
    ),
    AgentSlotSpec(
        slot_key="public_records_check",
        title="Public Records & Taxes",
        description="Pull county records: delinquent taxes, liens, prior sale, assessor history, water liens.",
        owner_type="human",
        default_status="idle",
    ),
    AgentSlotSpec(
        slot_key="rent_comps_pack",
        title="Rent Comps Pack",
        description="Assemble rent comps (AffordableHousing/Zillow/etc) for reasonableness + HA packet.",
        owner_type="human",
        default_status="idle",
    ),
    AgentSlotSpec(
        slot_key="hqs_checklist",
        title="HQS / Pre-Inspection",
        description="Run HUD checklist; capture photos; open fixes as rehab tasks; predict fail points later.",
        owner_type="hybrid",
        default_status="idle",
    ),
    AgentSlotSpec(
        slot_key="housing_authority_call",
        title="Housing Authority Call",
        description="Verify process time, landlord/tenant waitlist reality, required city certifications.",
        owner_type="human",
        default_status="idle",
    ),
    AgentSlotSpec(
        slot_key="lease_packet",
        title="Lease & Packet Assembly",
        description="Assemble voucher packet, HAP contract, utility clauses, renewal/calendar reminders.",
        owner_type="human",
        default_status="idle",
    ),
    AgentSlotSpec(
        slot_key="automation_monitor",
        title="Automation Monitor",
        description="Monitors ingest/enrich/evaluate runs and flags anomalies, budget, missing fields.",
        owner_type="ai",
        default_status="idle",
    ),
]
