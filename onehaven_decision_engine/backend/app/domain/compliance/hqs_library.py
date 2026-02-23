# backend/app/domain/compliance/hqs_library.py
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Iterable, List, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from ...policy_models import HqsRule, HqsAddendumRule


@dataclass(frozen=True)
class HqsItem:
    code: str
    category: str
    severity: str
    description: str
    evidence: List[str]
    remediation_hints: List[str]


def _loads_list(s: Optional[str]) -> List[str]:
    if not s:
        return []
    try:
        v = json.loads(s)
        return v if isinstance(v, list) else []
    except Exception:
        return []


def load_hqs_items(db: Session, *, org_id: int, jurisdiction_profile_id: int | None = None) -> List[HqsItem]:
    """
    Baseline HQS rules + optional local addendum overlays.
    Overlay semantics:
      - if addendum code matches baseline code: override non-null fields
      - else: add new item
    """
    base = list(db.scalars(select(HqsRule).order_by(HqsRule.code.asc())).all())
    items_by_code: dict[str, HqsItem] = {}

    for r in base:
        items_by_code[r.code] = HqsItem(
            code=r.code,
            category=r.category,
            severity=r.severity,
            description=r.description,
            evidence=_loads_list(r.evidence_json),
            remediation_hints=_loads_list(r.remediation_hints_json),
        )

    if jurisdiction_profile_id is not None:
        adds = list(
            db.scalars(
                select(HqsAddendumRule)
                .where(HqsAddendumRule.org_id == org_id)
                .where(HqsAddendumRule.jurisdiction_profile_id == jurisdiction_profile_id)
                .order_by(HqsAddendumRule.code.asc())
            ).all()
        )

        for a in adds:
            existing = items_by_code.get(a.code)
            if existing is None:
                items_by_code[a.code] = HqsItem(
                    code=a.code,
                    category=a.category or "safety",
                    severity=a.severity or "fail",
                    description=a.description or a.code,
                    evidence=_loads_list(a.evidence_json),
                    remediation_hints=_loads_list(a.remediation_hints_json),
                )
            else:
                items_by_code[a.code] = HqsItem(
                    code=a.code,
                    category=a.category or existing.category,
                    severity=a.severity or existing.severity,
                    description=a.description or existing.description,
                    evidence=_loads_list(a.evidence_json) or existing.evidence,
                    remediation_hints=_loads_list(a.remediation_hints_json) or existing.remediation_hints,
                )

    return list(items_by_code.values())


def required_categories_present(items: Iterable[HqsItem]) -> bool:
    required = {"safety", "electrical", "plumbing", "egress", "interior", "exterior", "structure", "thermal"}
    have = {i.category for i in items}
    return required.issubset(have)