#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path


ROOTS = [
    "products",
    "apps",
    "onehaven_platform",
]


REPLS = {
    # Acquire files incorrectly importing Intelligence-owned services relatively
    "from .market_catalog_service import": "from products.intelligence.backend.src.services.market_catalog_service import",
    "from .market_sync_service import": "from products.intelligence.backend.src.services.market_sync_service import",
    "from .rent_refresh_queue_service import": "from products.intelligence.backend.src.services.rent_refresh_queue_service import",
    "from .rentcast_service import": "from products.intelligence.backend.src.services.rentcast_service import",
    "from .rentcast_listing_source import": "from products.intelligence.backend.src.services.rentcast_listing_source import",
    "from .zillow_api_source import": "from products.intelligence.backend.src.services.zillow_api_source import",
    "from .zillow_photo_source import": "from products.intelligence.backend.src.services.zillow_photo_source import",
    "from .property_price_resolution_service import": "from products.intelligence.backend.src.services.property_price_resolution_service import",
    "from .property_tax_enrichment_service import": "from products.intelligence.backend.src.services.property_tax_enrichment_service import",
    "from .property_insurance_enrichment_service import": "from products.intelligence.backend.src.services.property_insurance_enrichment_service import",
    "from .public_tax_lookup_service import": "from products.intelligence.backend.src.services.public_tax_lookup_service import",
    "from .crime_index import": "from products.intelligence.backend.src.services.crime_index import",
    "from .offender_index import": "from products.intelligence.backend.src.services.offender_index import",
    "from .hud_fmr_service import": "from products.intelligence.backend.src.services.hud_fmr_service import",
    "from .fmr import": "from products.intelligence.backend.src.services.fmr import",
    "from .external_budget import": "from products.intelligence.backend.src.services.external_budget import",

    # Shared platform services
    "from .address_normalization import": "from onehaven_platform.backend.src.services.address_normalization import",
    "from .geo_enrichment import": "from onehaven_platform.backend.src.services.geo_enrichment import",
    "from .geocoding_service import": "from onehaven_platform.backend.src.services.geocoding_service import",
    "from .geocode_cache_service import": "from onehaven_platform.backend.src.services.geocode_cache_service import",
    "from .property_normalization_service import": "from onehaven_platform.backend.src.services.property_normalization_service import",
    "from .locks_service import": "from onehaven_platform.backend.src.services.locks_service import",
    "from .events_facade import": "from onehaven_platform.backend.src.services.events_facade import",
    "from .usage_service import": "from onehaven_platform.backend.src.services.usage_service import",
    "from .plan_service import": "from onehaven_platform.backend.src.services.plan_service import",
    "from .auth_service import": "from onehaven_platform.backend.src.services.auth_service import",
    "from .ownership import": "from onehaven_platform.backend.src.services.ownership import",
    "from .product_surfaces import": "from onehaven_platform.backend.src.services.product_surfaces import",
    "from .pane_routing_service import": "from onehaven_platform.backend.src.services.pane_routing_service import",

    # Legacy app namespace
    "from app.models import": "from onehaven_platform.backend.src.models import",
    "from app.policy_models import": "from onehaven_platform.backend.src.policy_models import",
    "from app.db import": "from onehaven_platform.backend.src.db import",
    "from app.config import": "from onehaven_platform.backend.src.config import",
    "from app.auth import": "from onehaven_platform.backend.src.auth import",
    "from app.schemas import": "from onehaven_platform.backend.src.schemas import",
    "from app.middleware.": "from onehaven_platform.backend.src.middleware.",
    "from app.workers.": "from onehaven_platform.backend.src.jobs.",
    "from app.tasks.": "from onehaven_platform.backend.src.jobs.",
    "from app.clients.": "from onehaven_platform.backend.src.integrations.",
    "from app.integrations.": "from onehaven_platform.backend.src.integrations.",
    "from app.services.": "from onehaven_platform.backend.src.services.",
    "from app.services import": "from onehaven_platform.backend.src.services import",
    "from app.domain.": "from onehaven_platform.backend.src.domain.",

        # Intelligence files incorrectly importing Acquire-owned ingestion services relatively
    "from .ingestion_source_service import": "from products.acquire.backend.src.services.ingestion_source_service import",
    "from .ingestion_scheduler_service import": "from products.acquire.backend.src.services.ingestion_scheduler_service import",
    "from .ingestion_run_service import": "from products.acquire.backend.src.services.ingestion_run_service import",
    "from .ingestion_run_execute import": "from products.acquire.backend.src.services.ingestion_run_execute import",
    "from .ingestion_dedupe_service import": "from products.acquire.backend.src.services.ingestion_dedupe_service import",
    "from .ingestion_enrichment_service import": "from products.acquire.backend.src.services.ingestion_enrichment_service import",
    "from .portfolio_ingestion_service import": "from products.acquire.backend.src.services.portfolio_ingestion_service import",
    "from .product_ingestion_router_service import": "from products.acquire.backend.src.services.product_ingestion_router_service import",
    "from .csv_import_mapping_service import": "from products.acquire.backend.src.services.csv_import_mapping_service import",
}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    root = Path(args.repo_root).resolve()

    changed = 0
    replacements = 0

    for root_name in ROOTS:
        scan_root = root / root_name
        if not scan_root.exists():
            continue

        for p in scan_root.rglob("*.py"):
            if "__pycache__" in p.parts:
                continue

            text = p.read_text(encoding="utf-8")
            new = text

            for old, replacement in REPLS.items():
                count = new.count(old)
                if count:
                    replacements += count
                    new = new.replace(old, replacement)

            if new != text:
                changed += 1
                if args.dry_run:
                    print(f"[DRY RUN] would update {p}")
                else:
                    p.write_text(new, encoding="utf-8")
                    print(f"updated {p}")

    print("Phase 82 complete.")
    print({
        "files_changed": changed,
        "replacements": replacements,
        "dry_run": args.dry_run,
    })


if __name__ == "__main__":
    main()