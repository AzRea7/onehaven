from __future__ import annotations

from typing import Any

import httpx


class FederalRegisterClient:
    """
    Public FederalRegister.gov API client.

    No API key is required.
    Docs:
    https://www.federalregister.gov/developers/documentation/api/v1
    """

    def __init__(
        self,
        *,
        base_url: str = "https://www.federalregister.gov/api/v1",
        timeout: float = 30.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _get(self, path: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        with httpx.Client(timeout=self.timeout, follow_redirects=True) as client:
            resp = client.get(f"{self.base_url}/{path.lstrip('/')}", params=params or {})
            resp.raise_for_status()
            return resp.json()

    def get_document(self, document_number: str) -> dict[str, Any]:
        return self._get(f"documents/{document_number}.json")

    def get_documents(self, document_numbers: list[str]) -> dict[str, Any]:
        return self._get("documents.json", params={"conditions[document_numbers][]": document_numbers})

    def search_documents(
        self,
        *,
        conditions: dict[str, Any],
        per_page: int = 20,
        page: int = 1,
        order: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "per_page": int(per_page),
            "page": int(page),
        }
        if order:
            params["order"] = order

        for key, value in conditions.items():
            if isinstance(value, list):
                params[f"conditions[{key}][]"] = value
            else:
                params[f"conditions[{key}]"] = value

        return self._get("documents.json", params=params)

    def current_public_inspection(self) -> dict[str, Any]:
        return self._get("public-inspection-documents/current.json")

    def search_public_inspection(
        self,
        *,
        conditions: dict[str, Any],
        per_page: int = 20,
        page: int = 1,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "per_page": int(per_page),
            "page": int(page),
        }
        for key, value in conditions.items():
            if isinstance(value, list):
                params[f"conditions[{key}][]"] = value
            else:
                params[f"conditions[{key}]"] = value

        return self._get("public-inspection-documents.json", params=params)