#!/usr/bin/env python3
from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path


MODULES = [
    "apps.suite_api.app.main",
    "onehaven_platform.backend.src.services.trust",
    "products.compliance.backend.src.services.product.compliance_brief_service",
    "products.acquire.backend.src.services.product.acquisition_workspace_service",
    "products.intelligence.backend.src.services.product.deal_intelligence_service",
    "products.ops.backend.src.services.product.property_ops_summary_service",
]


def main():
    repo_root = Path(__file__).resolve().parents[2]

    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    results = []

    for module in MODULES:
        try:
            importlib.import_module(module)
            results.append({"module": module, "status": "ok"})
        except Exception as exc:
            results.append({"module": module, "status": "error", "error": repr(exc)})

    payload = {
        "phase": 95,
        "repo_root": str(repo_root),
        "modules": results,
        "ok": all(r["status"] == "ok" for r in results),
    }

    out = repo_root / "tools/repo/platform-state-phase95-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 95 complete.")
    print({"ok": payload["ok"], "module_count": len(results)})
    print("Report written to tools/repo/platform-state-phase95-report.json")


if __name__ == "__main__":
    main()