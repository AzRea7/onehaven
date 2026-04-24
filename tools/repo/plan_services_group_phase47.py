#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, asdict
from pathlib import Path


@dataclass
class ServicePlan:
    source: str
    target: str | None
    owner: str
    confidence: str
    reason: str


SOURCE_ROOT = "onehaven_decision_engine/backend/app/services"

PLATFORM_EXACT = {
    "address_normalization.py": "onehaven_onehaven_platform/backend/src/services/address_normalization.py",
    "agent_actions.py": "onehaven_onehaven_platform/backend/src/services/agent_actions.py",
    "agent_concurrency.py": "onehaven_onehaven_platform/backend/src/services/agent_concurrency.py",
    "agent_engine.py": "onehaven_onehaven_platform/backend/src/services/agent_engine.py",
    "agent_orchestrator.py": "onehaven_onehaven_platform/backend/src/services/agent_orchestrator.py",
    "agent_orchestrator_runtime.py": "onehaven_onehaven_platform/backend/src/services/agent_orchestrator_runtime.py",
    "agent_threads.py": "onehaven_onehaven_platform/backend/src/services/agent_threads.py",
    "agent_trace.py": "onehaven_onehaven_platform/backend/src/services/agent_trace.py",
    "auth_service.py": "onehaven_onehaven_platform/backend/src/services/auth_service.py",
    "budget_service.py": "onehaven_onehaven_platform/backend/src/services/budget_service.py",
    "events_facade.py": "onehaven_onehaven_platform/backend/src/services/events_facade.py",
    "locks_service.py": "onehaven_onehaven_platform/backend/src/services/locks_service.py",
    "onboarding_flows.py": "onehaven_onehaven_platform/backend/src/services/onboarding_flows.py",
    "ownership.py": "onehaven_onehaven_platform/backend/src/services/ownership.py",
    "pane_routing_service.py": "onehaven_onehaven_platform/backend/src/services/pane_routing_service.py",
    "plan_service.py": "onehaven_onehaven_platform/backend/src/services/plan_service.py",
    "product_surfaces.py": "onehaven_onehaven_platform/backend/src/services/product_surfaces.py",
    "runtime_metrics.py": "onehaven_onehaven_platform/backend/src/observability/runtime_metrics.py",
    "usage_service.py": "onehaven_onehaven_platform/backend/src/services/usage_service.py",
    "virus_scanning_service.py": "onehaven_onehaven_platform/backend/src/services/virus_scanning_service.py",
}

PLATFORM_PREFIXES = [
    "agent_",
]

COMPLIANCE_EXACT = {
    "inspection_scheduling_service.py": "products/compliance/backend/src/services/inspection_scheduling_service.py",
    "inspector_communication_service.py": "products/compliance/backend/src/services/inspector_communication_service.py",
    "jurisdiction_profile_service.py": "products/compliance/backend/src/services/jurisdiction_profile_service.py",
    "jurisdiction_registry_service.py": "products/compliance/backend/src/services/jurisdiction_registry_service.py",
    "jurisdiction_source_family_service.py": "products/compliance/backend/src/services/jurisdiction_source_family_service.py",
    "jurisdiction_task_mapper.py": "products/compliance/backend/src/services/jurisdiction_task_mapper.py",
    "policy_catalog.py": "products/compliance/backend/src/services/policy_catalog.py",
    "policy_change_detection_service.py": "products/compliance/backend/src/services/policy_change_detection_service.py",
    "policy_crawl_service.py": "products/compliance/backend/src/services/policy_crawl_service.py",
    "policy_evidence_service.py": "products/compliance/backend/src/services/policy_evidence_service.py",
    "policy_evidence_version_service.py": "products/compliance/backend/src/services/policy_evidence_version_service.py",
    "policy_market_seed_service.py": "products/compliance/backend/src/services/policy_market_seed_service.py",
    "policy_rule_normalizer.py": "products/compliance/backend/src/services/policy_rule_normalizer.py",
    "policy_seed.py": "products/compliance/backend/src/services/policy_seed.py",
    "trust_service.py": "products/compliance/backend/src/services/trust_service.py",
}

COMPLIANCE_PREFIXES = [
    "compliance_",
    "inspection",
    "inspector_",
    "jurisdiction_",
    "policy_",
    "workflow_gate",
    "property_compliance",
    "nspire",
    "hqs",
    "trust_",
]

INTELLIGENCE_EXACT = {
    "crime_index.py": "products/intelligence/backend/src/services/crime_index.py",
    "external_budget.py": "products/intelligence/backend/src/services/external_budget.py",
    "fmr.py": "products/intelligence/backend/src/services/fmr.py",
    "hud_fmr_service.py": "products/intelligence/backend/src/services/hud_fmr_service.py",
    "market_catalog_service.py": "products/intelligence/backend/src/services/market_catalog_service.py",
    "market_sync_service.py": "products/intelligence/backend/src/services/market_sync_service.py",
    "offender_index.py": "products/intelligence/backend/src/services/offender_index.py",
    "portfolio_watchlist_service.py": "products/intelligence/backend/src/services/portfolio_watchlist_service.py",
    "property_insurance_enrichment_service.py": "products/intelligence/backend/src/services/property_insurance_enrichment_service.py",
    "property_price_resolution_service.py": "products/intelligence/backend/src/services/property_price_resolution_service.py",
    "property_tax_enrichment_service.py": "products/intelligence/backend/src/services/property_tax_enrichment_service.py",
    "public_tax_lookup_service.py": "products/intelligence/backend/src/services/public_tax_lookup_service.py",
    "rent_comp_selection.py": "products/intelligence/backend/src/services/rent_comp_selection.py",
    "rent_refresh_queue_service.py": "products/intelligence/backend/src/services/rent_refresh_queue_service.py",
    "rentcast_listing_source.py": "products/intelligence/backend/src/services/rentcast_listing_source.py",
    "rentcast_service.py": "products/intelligence/backend/src/services/rentcast_service.py",
    "zillow_api_source.py": "products/intelligence/backend/src/services/zillow_api_source.py",
    "zillow_photo_source.py": "products/intelligence/backend/src/services/zillow_photo_source.py",
}

INTELLIGENCE_PREFIXES = [
    "rent",
    "cashflow",
    "equity",
    "valuation",
    "underwrite",
    "deal_",
]

ACQUIRE_EXACT = {
    "csv_import_mapping_service.py": "products/acquire/backend/src/services/csv_import_mapping_service.py",
    "ingestion_dedupe_service.py": "products/acquire/backend/src/services/ingestion_dedupe_service.py",
    "ingestion_enrichment_service.py": "products/acquire/backend/src/services/ingestion_enrichment_service.py",
    "ingestion_run_execute.py": "products/acquire/backend/src/services/ingestion_run_execute.py",
    "ingestion_run_service.py": "products/acquire/backend/src/services/ingestion_run_service.py",
    "ingestion_scheduler_service.py": "products/acquire/backend/src/services/ingestion_scheduler_service.py",
    "ingestion_source_service.py": "products/acquire/backend/src/services/ingestion_source_service.py",
    "portfolio_ingestion_service.py": "products/acquire/backend/src/services/portfolio_ingestion_service.py",
    "product_ingestion_router_service.py": "products/acquire/backend/src/services/product_ingestion_router_service.py",
}

ACQUIRE_PREFIXES = [
    "import",
    "ingestion_",
    "document_review",
    "document_parsing",
    "closing",
    "acquisition",
    "deadline",
    "participant",
    "portfolio_ingestion",
]

OPS_EXACT = {
    "photo_rehab_agent.py": "products/ops/backend/src/services/photo_rehab_agent.py",
    "property_photo_service.py": "products/ops/backend/src/services/property_photo_service.py",
    "stage_guard.py": "products/ops/backend/src/services/stage_guard.py",
}

OPS_PREFIXES = [
    "rehab",
    "turnover",
    "inventory",
    "dashboard",
    "task",
    "stage_guard",
]

TENANTS_EXACT = {
    "lease_rules.py": "products/tenants/backend/src/services/lease_rules.py",
}

TENANTS_PREFIXES = [
    "tenant",
    "voucher",
    "applicant",
    "lease_",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plan backend/app/services migration.")
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    required = [
        repo_root / SOURCE_ROOT,
        repo_root / "tools" / "repo",
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise SystemExit("Missing required paths:\n- " + "\n- ".join(missing))
    return repo_root


def starts_with_any(name: str, prefixes: list[str]) -> bool:
    return any(name.startswith(p) for p in prefixes)


def classify(name: str) -> tuple[str, str | None, str, str]:
    lower = name.lower()

    if name in PLATFORM_EXACT:
        return "platform", PLATFORM_EXACT[name], "high", "platform_exact_match"
    if starts_with_any(lower, PLATFORM_PREFIXES):
        return "platform", f"onehaven_onehaven_platform/backend/src/services/{name}", "high", "platform_prefix_match"

    if name in COMPLIANCE_EXACT:
        return "compliance", COMPLIANCE_EXACT[name], "high", "compliance_exact_match"
    if starts_with_any(lower, COMPLIANCE_PREFIXES):
        return "compliance", f"products/compliance/backend/src/services/{name}", "high", "compliance_prefix_match"

    if name in INTELLIGENCE_EXACT:
        return "intelligence", INTELLIGENCE_EXACT[name], "medium", "intelligence_exact_match"
    if starts_with_any(lower, INTELLIGENCE_PREFIXES):
        return "intelligence", f"products/intelligence/backend/src/services/{name}", "medium", "intelligence_prefix_match"

    if name in ACQUIRE_EXACT:
        return "acquire", ACQUIRE_EXACT[name], "medium", "acquire_exact_match"
    if starts_with_any(lower, ACQUIRE_PREFIXES):
        return "acquire", f"products/acquire/backend/src/services/{name}", "medium", "acquire_prefix_match"

    if name in OPS_EXACT:
        return "ops", OPS_EXACT[name], "medium", "ops_exact_match"
    if starts_with_any(lower, OPS_PREFIXES):
        return "ops", f"products/ops/backend/src/services/{name}", "medium", "ops_prefix_match"

    if name in TENANTS_EXACT:
        return "tenants", TENANTS_EXACT[name], "medium", "tenants_exact_match"
    if starts_with_any(lower, TENANTS_PREFIXES):
        return "tenants", f"products/tenants/backend/src/services/{name}", "medium", "tenants_prefix_match"

    return "manual_review", None, "low", "no_prefix_match"


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root))
    services_root = repo_root / SOURCE_ROOT

    plans: list[ServicePlan] = []

    for p in sorted(services_root.glob("*.py")):
        owner, target, confidence, reason = classify(p.name)
        plans.append(
            ServicePlan(
                source=p.relative_to(repo_root).as_posix(),
                target=target,
                owner=owner,
                confidence=confidence,
                reason=reason,
            )
        )

    payload = {
        "phase": 47,
        "summary": {
            "total": len(plans),
            "platform": sum(1 for p in plans if p.owner == "platform"),
            "compliance": sum(1 for p in plans if p.owner == "compliance"),
            "intelligence": sum(1 for p in plans if p.owner == "intelligence"),
            "acquire": sum(1 for p in plans if p.owner == "acquire"),
            "ops": sum(1 for p in plans if p.owner == "ops"),
            "tenants": sum(1 for p in plans if p.owner == "tenants"),
            "manual_review": sum(1 for p in plans if p.owner == "manual_review"),
        },
        "plans": [asdict(p) for p in plans],
    }

    out = repo_root / "tools" / "repo" / "services-group-phase47-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 47 complete.")
    print(f"Total services planned: {payload['summary']['total']}")
    print(f"Platform: {payload['summary']['platform']}")
    print(f"Compliance: {payload['summary']['compliance']}")
    print(f"Intelligence: {payload['summary']['intelligence']}")
    print(f"Acquire: {payload['summary']['acquire']}")
    print(f"Ops: {payload['summary']['ops']}")
    print(f"Tenants: {payload['summary']['tenants']}")
    print(f"Manual review: {payload['summary']['manual_review']}")
    print("Report written to tools/repo/services-group-phase47-report.json")


if __name__ == "__main__":
    main()