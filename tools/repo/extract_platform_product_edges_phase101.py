#!/usr/bin/env python3
from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path

IN = Path("tools/repo/cross-owner-analysis-phase100-report.json")
OUT = Path("tools/repo/platform-product-edges-phase101-report.json")


def main():
    data = json.loads(IN.read_text(encoding="utf-8"))
    edges = data.get("disallowed_edges", [])

    platform_edges = [
        e for e in edges
        if e.get("from_owner") == "platform"
    ]

    by_target_owner = Counter(e["to_owner"] for e in platform_edges)
    by_module = Counter(e["module"] for e in platform_edges)
    by_file = defaultdict(list)

    for e in platform_edges:
        by_file[e["file"]].append(e["module"])

    payload = {
        "phase": 101,
        "summary": {
            "platform_to_product_edges_excluding_adapters": len(platform_edges),
            "files": len(by_file),
            "target_owners": dict(by_target_owner),
            "unique_modules": len(by_module),
        },
        "top_modules": [
            {"module": m, "count": c}
            for m, c in by_module.most_common()
        ],
        "files": [
            {
                "file": f,
                "imports": sorted(set(mods)),
                "count": len(mods),
            }
            for f, mods in sorted(by_file.items())
        ],
        "edges": platform_edges,
    }

    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 101 complete.")
    print(payload["summary"])
    print("Report written to tools/repo/platform-product-edges-phase101-report.json")


if __name__ == "__main__":
    main()