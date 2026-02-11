# backend/app/domain/agents/registry.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional


@dataclass(frozen=True)
class AgentSpec:
    key: str
    name: str
    description: str
    kind: str  # "human" for now; later "llm"
    default_payload_schema: dict


# For now these are HUMAN workflow slots.
# Later: attach tools, permissions, and an LLM runner.
AGENTS: dict[str, AgentSpec] = {
    "intake_specialist": AgentSpec(
        key="intake_specialist",
        name="Intake Specialist",
        description="Verifies property details, flags missing fields, validates filters (no garage, min price, etc.).",
        kind="human",
        default_payload_schema={
            "property_id": "int",
            "notes": "string",
        },
    ),
    "rent_comps_collector": AgentSpec(
        key="rent_comps_collector",
        name="Rent Comps Collector",
        description="Collects AffordableHousing/Zillow comps and enters rent reasonableness comps.",
        kind="human",
        default_payload_schema={
            "property_id": "int",
            "comp_entries": [
                {"source": "string", "address": "string", "rent": "number", "url": "string"}
            ],
        },
    ),
    "jurisdiction_researcher": AgentSpec(
        key="jurisdiction_researcher",
        name="Jurisdiction Researcher",
        description="Finds city requirements: license, inspection frequency, processing days, waitlist depth.",
        kind="human",
        default_payload_schema={
            "city": "string",
            "state": "string",
            "findings": "string",
        },
    ),
    "hqs_preinspector": AgentSpec(
        key="hqs_preinspector",
        name="HQS Pre-Inspector",
        description="Runs checklist, flags likely fail points, assigns rehab tickets to remediate before inspection.",
        kind="human",
        default_payload_schema={
            "property_id": "int",
            "strategy": "section8|market",
            "notes": "string",
        },
    ),
    "leasing_coordinator": AgentSpec(
        key="leasing_coordinator",
        name="Leasing Coordinator",
        description="Tracks voucher status, packet submission, inspection scheduling, HAP start date.",
        kind="human",
        default_payload_schema={
            "property_id": "int",
            "tenant_name": "string",
            "voucher_status": "string",
            "notes": "string",
        },
    ),
}
