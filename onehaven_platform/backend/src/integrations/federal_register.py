from __future__ import annotations

from datetime import date
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

    def _normalize_params(self, params: dict[str, Any] | None = None) -> dict[str, Any]:
        query: dict[str, Any] = {}
        for key, value in (params or {}).items():
            if value is None:
                continue
            if isinstance(value, (list, tuple, set)):
                cleaned = [item for item in value if item is not None and str(item).strip()]
                if cleaned:
                    query[key] = list(cleaned)
            else:
                query[key] = value
        return query

    def _get(self, path: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        with httpx.Client(timeout=self.timeout, follow_redirects=True) as client:
            resp = client.get(
                f"{self.base_url}/{path.lstrip('/')}",
                params=self._normalize_params(params),
                headers={"Accept": "application/json"},
            )
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
        fields: list[str] | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "per_page": int(per_page),
            "page": int(page),
        }
        if order:
            params["order"] = order
        if fields:
            params["fields[]"] = fields

        for key, value in conditions.items():
            if isinstance(value, (list, tuple, set)):
                params[f"conditions[{key}][]"] = list(value)
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
            if isinstance(value, (list, tuple, set)):
                params[f"conditions[{key}][]"] = list(value)
            else:
                params[f"conditions[{key}]"] = value

        return self._get("public-inspection-documents.json", params=params)

    # --- Step 8 additive helpers ---
    def search_by_agencies(
        self,
        *,
        agencies: list[str],
        term: str | None = None,
        per_page: int = 20,
        page: int = 1,
        order: str = "newest",
    ) -> dict[str, Any]:
        conditions: dict[str, Any] = {"agencies": agencies}
        if term:
            conditions["term"] = term
        return self.search_documents(
            conditions=conditions,
            per_page=per_page,
            page=page,
            order=order,
        )

    def search_hud_documents(
        self,
        *,
        term: str | None = None,
        per_page: int = 20,
        page: int = 1,
        order: str = "newest",
        document_types: list[str] | None = None,
    ) -> dict[str, Any]:
        conditions: dict[str, Any] = {"agencies": ["housing-and-urban-development-department"]}
        if term:
            conditions["term"] = term
        if document_types:
            conditions["type"] = document_types
        return self.search_documents(
            conditions=conditions,
            per_page=per_page,
            page=page,
            order=order,
        )

    def search_section8_updates(
        self,
        *,
        term: str = 'Section 8 OR Housing Choice Voucher OR HCV OR HUD',
        per_page: int = 20,
        page: int = 1,
        order: str = "newest",
    ) -> dict[str, Any]:
        return self.search_hud_documents(
            term=term,
            per_page=per_page,
            page=page,
            order=order,
        )

    def search_final_rules(
        self,
        *,
        agencies: list[str] | None = None,
        term: str | None = None,
        per_page: int = 20,
        page: int = 1,
        order: str = "newest",
    ) -> dict[str, Any]:
        conditions: dict[str, Any] = {"type": ["RULE"]}
        if agencies:
            conditions["agencies"] = agencies
        if term:
            conditions["term"] = term
        return self.search_documents(conditions=conditions, per_page=per_page, page=page, order=order)

    def search_proposed_rules(
        self,
        *,
        agencies: list[str] | None = None,
        term: str | None = None,
        per_page: int = 20,
        page: int = 1,
        order: str = "newest",
    ) -> dict[str, Any]:
        conditions: dict[str, Any] = {"type": ["PRORULE"]}
        if agencies:
            conditions["agencies"] = agencies
        if term:
            conditions["term"] = term
        return self.search_documents(conditions=conditions, per_page=per_page, page=page, order=order)

    def latest_hud_rule_updates(
        self,
        *,
        since_date: date | None = None,
        term: str = 'Housing Choice Voucher OR Section 8 OR NSPIRE',
        per_page: int = 20,
        page: int = 1,
    ) -> dict[str, Any]:
        conditions: dict[str, Any] = {
            "agencies": ["housing-and-urban-development-department"],
            "term": term,
        }
        if since_date is not None:
            conditions["publication_date][gte"] = since_date.isoformat()
        return self.search_documents(
            conditions=conditions,
            per_page=per_page,
            page=page,
            order="newest",
        )
