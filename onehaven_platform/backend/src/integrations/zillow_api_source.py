from __future__ import annotations

from typing import Any


class ZillowAPISource:
    provider = "zillow"

    def fetch_incremental(self, *, credentials: dict[str, Any], config: dict[str, Any], cursor: dict[str, Any]) -> dict[str, Any]:
        """
        Adapter seam only.
        Replace this stub with the real Zillow partner/API integration you are allowed to use.
        """
        rows = config.get("sample_rows") or []
        next_cursor = {"last_seen": config.get("mock_last_seen", "done")}
        return {"rows": rows, "next_cursor": next_cursor}
    