from __future__ import annotations

from typing import Any


class InvestorLiftSource:
    provider = "investorlift"

    def fetch_incremental(self, *, credentials: dict[str, Any], config: dict[str, Any], cursor: dict[str, Any]) -> dict[str, Any]:
        rows = config.get("sample_rows") or []
        next_cursor = {"last_seen": config.get("mock_last_seen", "done")}
        return {"rows": rows, "next_cursor": next_cursor}