#!/usr/bin/env python3
from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path

REPORT = Path("tools/repo/product-cycle-audit-phase85-report.json")
OUT = Path("tools/repo/cross-owner-analysis-phase92-report.json")

ALLOWED = {
    ("acquire", "platform"),
    ("compliance", "platform"),
    ("intelligence", "platform"),
    ("ops", "platform"),
    ("tenants", "platform"),
    ("app", "platform"),
    ("app", "acquire"),
    ("app", "compliance"),
    ("app", "intelligence"),
    ("app", "ops"),
    ("app", "tenants"),
}

def main():
    data = json.loads(REPORT.read_text(encoding="utf-8"))
    edges = data.get("edges", [])

    disallowed = []
    by_pair = Counter()
    by_module = Counter()
    by_file = defaultdict(list)

    for e in edges:
        pair = (e["from_owner"], e["to_owner"])
        by_pair[pair] += 1
        by_module[e["module"]] += 1

        if pair not in ALLOWED:
            disallowed.append(e)
            by_file[e["file"]].append(e["module"])

    payload = {
        "phase": 92,
        "summary": {
            "total_edges": len(edges),
            "allowed_edges": len(edges) - len(disallowed),
            "disallowed_edges": len(disallowed),
            "files_with_disallowed_edges": len(by_file),
        },
        "edge_pairs": [
            {"from": a, "to": b, "count": c}
            for (a, b), c in by_pair.most_common()
        ],
        "top_modules": [
            {"module": m, "count": c}
            for m, c in by_module.most_common(50)
        ],
        "disallowed_by_file": [
            {"file": f, "imports": sorted(set(mods)), "count": len(mods)}
            for f, mods in sorted(by_file.items())
        ],
        "disallowed_edges": disallowed,
    }

    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print("Phase 92 complete.")
    print(payload["summary"])
    print("Report written to tools/repo/cross-owner-analysis-phase92-report.json")

if __name__ == "__main__":
    main()