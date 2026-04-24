
from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from typing import Any


CSV_TEMPLATE_KEYS = {
    "candidate_deals",
    "portfolio_properties",
    "units",
    "leases",
    "applicants",
    "inspection_issues",
}


@dataclass(frozen=True)
class CSVImportTemplate:
    key: str
    required_columns: tuple[str, ...]
    optional_columns: tuple[str, ...]
    description: str


CSV_IMPORT_TEMPLATES: dict[str, CSVImportTemplate] = {
    "candidate_deals": CSVImportTemplate(
        key="candidate_deals",
        required_columns=("address", "city", "state"),
        optional_columns=("zip", "asking_price", "beds", "baths", "square_feet", "estimated_rent", "property_type", "notes"),
        description="Deal candidates for OneHaven Intelligence.",
    ),
    "portfolio_properties": CSVImportTemplate(
        key="portfolio_properties",
        required_columns=("address", "city", "state"),
        optional_columns=("zip", "county", "portfolio", "year_built", "property_type", "unit_count", "occupancy_status", "voucher_status"),
        description="Portfolio property import for compliance and ops onboarding.",
    ),
    "units": CSVImportTemplate(
        key="units",
        required_columns=("address", "city", "state", "unit_label"),
        optional_columns=("zip", "bedrooms", "bathrooms", "square_feet", "occupancy_status", "market_rent", "voucher_eligible"),
        description="Unit-level import.",
    ),
    "leases": CSVImportTemplate(
        key="leases",
        required_columns=("address", "city", "state", "tenant_name", "start_date"),
        optional_columns=("zip", "unit_label", "end_date", "total_rent", "tenant_portion", "housing_authority_portion", "hap_contract_status"),
        description="Lease import.",
    ),
    "applicants": CSVImportTemplate(
        key="applicants",
        required_columns=("full_name",),
        optional_columns=("email", "phone", "voucher_status", "desired_bedrooms", "desired_move_date", "status", "notes"),
        description="Applicant import for tenant workflows.",
    ),
    "inspection_issues": CSVImportTemplate(
        key="inspection_issues",
        required_columns=("address", "city", "state", "issue_code"),
        optional_columns=("zip", "category", "severity", "location", "details", "inspection_date", "status"),
        description="Inspection and issue import.",
    ),
}


def _decode_csv_input(data: bytes | str) -> str:
    if isinstance(data, bytes):
        return data.decode("utf-8-sig")
    return str(data)


def parse_csv_rows(data: bytes | str) -> list[dict[str, Any]]:
    text = _decode_csv_input(data)
    reader = csv.DictReader(io.StringIO(text))
    rows: list[dict[str, Any]] = []
    for row in reader:
        normalized = {str(k or "").strip(): (str(v).strip() if v is not None else None) for k, v in row.items()}
        if any(v not in {None, ""} for v in normalized.values()):
            rows.append(normalized)
    return rows


def validate_csv_template(template_key: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    if template_key not in CSV_IMPORT_TEMPLATES:
        return {"ok": False, "error": "unknown_template", "template_key": template_key}
    template = CSV_IMPORT_TEMPLATES[template_key]
    if not rows:
        return {
            "ok": True,
            "template_key": template_key,
            "row_count": 0,
            "missing_required_columns": list(template.required_columns),
            "detected_columns": [],
        }
    detected_columns = set(rows[0].keys())
    missing = [col for col in template.required_columns if col not in detected_columns]
    return {
        "ok": len(missing) == 0,
        "template_key": template_key,
        "row_count": len(rows),
        "missing_required_columns": missing,
        "detected_columns": sorted(detected_columns),
        "optional_columns": list(template.optional_columns),
    }


def map_csv_payload(template_key: str, data: bytes | str) -> dict[str, Any]:
    rows = parse_csv_rows(data)
    validation = validate_csv_template(template_key, rows)
    return {
        "ok": bool(validation.get("ok")),
        "template_key": template_key,
        "rows": rows,
        "validation": validation,
    }
