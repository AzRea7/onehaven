#!/usr/bin/env python3
from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path


REPORT = Path("tools/repo/product-cycle-audit-phase85-report.json")
OUT = Path("tools/repo/ops-compliance-cycles-phase86-report.json")


def main():
    data = json.loads(REPORT.read_text(encoding="utf-8"))
    risky = data.get("risky_edges", [])

    by_module = Counter(edge["module"] for edge in risky)
    by_file = defaultdict(list)

    for edge in risky:
        by_file[edge["file"]].append(edge["module"])

    payload = {
        "phase": 86,
        "summary": {
            "risky_edges": len(risky),
            "files_with_risk": len(by_file),
            "unique_target_modules": len(by_module),
        },
        "top_target_modules": [
            {"module": module, "count": count}
            for module, count in by_module.most_common()
        ],
        "files": [
            {
                "file": file,
                "imports": sorted(set(modules)),
                "import_count": len(modules),
            }
            for file, modules in sorted(by_file.items())
        ],
    }

    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 86 complete.")
    print(payload["summary"])
    print("Report written to tools/repo/ops-compliance-cycles-phase86-report.json")


if __name__ == "__main__":
    main()