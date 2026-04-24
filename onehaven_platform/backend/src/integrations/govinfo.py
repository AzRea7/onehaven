from __future__ import annotations

from typing import Any, Optional
from urllib.parse import quote

import httpx

from onehaven_platform.backend.src.config import settings


class GovInfoClient:
    """
    GovInfo API client.

    Requires an api.data.gov key.
    Docs:
    https://api.govinfo.gov/docs/
    """

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        timeout: float = 30.0,
    ) -> None:
        self.api_key = api_key or getattr(settings, "govinfo_api_key", "")
        self.base_url = (base_url or getattr(settings, "govinfo_base_url", None) or "https://api.govinfo.gov").rstrip("/")
        self.timeout = timeout

        if not self.api_key:
            raise ValueError("GovInfo API key is missing. Set GOVINFO_API_KEY.")

    def _get(self, path: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        query = dict(params or {})
        query["api_key"] = self.api_key

        with httpx.Client(timeout=self.timeout, follow_redirects=True) as client:
            resp = client.get(f"{self.base_url}/{path.lstrip('/')}", params=query)
            resp.raise_for_status()
            return resp.json()

    def build_public_url(self, path: str) -> str:
        return f"{self.base_url}/{path.lstrip('/')}"

    def get_package_summary(self, package_id: str) -> dict[str, Any]:
        return self._get(f"packages/{quote(package_id, safe='')}/summary")

    def get_package_metadata(self, package_id: str) -> dict[str, Any]:
        return self._get(f"packages/{quote(package_id, safe='')}")

    def get_granules(self, package_id: str, *, offset_mark: str = "*", page_size: int = 100) -> dict[str, Any]:
        return self._get(
            f"packages/{quote(package_id, safe='')}/granules",
            params={
                "offsetMark": offset_mark,
                "pageSize": int(page_size),
            },
        )

    def get_granule(self, package_id: str, granule_id: str) -> dict[str, Any]:
        return self._get(f"packages/{quote(package_id, safe='')}/granules/{quote(granule_id, safe='')}")

    def search_collections(
        self,
        *,
        collections: list[str],
        query: str,
        page_size: int = 20,
        offset_mark: str = "*",
    ) -> dict[str, Any]:
        return self._get(
            "search",
            params={
                "collections": ",".join(collections),
                "query": query,
                "pageSize": int(page_size),
                "offsetMark": offset_mark,
            },
        )

    # --- Step 8 additive helpers ---
    def search_cfr_title_24(
        self,
        *,
        query: str,
        page_size: int = 20,
        offset_mark: str = "*",
    ) -> dict[str, Any]:
        return self.search_collections(
            collections=["CFR"],
            query=f'(title:24) AND ({query})',
            page_size=page_size,
            offset_mark=offset_mark,
        )

    def search_hud_hcv_materials(
        self,
        *,
        query: str = 'Section 8 OR Housing Choice Voucher OR NSPIRE OR tenant-based assistance',
        page_size: int = 20,
        offset_mark: str = "*",
    ) -> dict[str, Any]:
        return self.search_collections(
            collections=["CFR", "FR", "CHRG", "PLAW"],
            query=query,
            page_size=page_size,
            offset_mark=offset_mark,
        )

    def search_title24_part(self, *, part_number: str, page_size: int = 20, offset_mark: str = "*") -> dict[str, Any]:
        return self.search_cfr_title_24(
            query=f'part:{part_number}',
            page_size=page_size,
            offset_mark=offset_mark,
        )
