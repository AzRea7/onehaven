# backend/app/services/policy_extractor_service.py
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.policy_models import PolicyAssertion, PolicySource


def _dumps(v: Any) -> str:
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return "{}"


def extract_assertions_for_source(
    db: Session,
    *,
    source: PolicySource,
    org_id: Optional[int],
    org_scope: bool = True,
) -> list[PolicyAssertion]:
    """
    Turn one PolicySource into PolicyAssertions.

    Design rule:
    - If we can't parse deterministically, we emit a low-confidence "document_reference"
      assertion that points to the source. Human review can then create real rule values.

    org_scope:
      - True: extracted assertions are org-scoped (org_id=principal.org_id)
      - False: extracted assertions are global (org_id=NULL)
    """
    target_org_id = org_id if org_scope else None

    now = datetime.utcnow()
    created: list[PolicyAssertion] = []

    # Always emit a reference assertion so the source shows in UI even if parsing fails.
    base_value = {
        "type": "document_reference",
        "url": source.url,
        "publisher": source.publisher,
        "title": source.title,
        "content_type": source.content_type,
        "retrieved_at": source.retrieved_at.isoformat() if source.retrieved_at else None,
        "sha256": source.content_sha256,
        "notes": source.notes,
    }

    created.append(
        PolicyAssertion(
            org_id=target_org_id,
            source_id=source.id,
            state=source.state,
            county=source.county,
            city=source.city,
            pha_name=source.pha_name,
            program_type=source.program_type,
            rule_key="document_reference",
            value_json=_dumps(base_value),
            confidence=0.15,
            review_status="extracted",
            extracted_at=now,
        )
    )

    # If source is eCFR CFR part 982, emit a higher-confidence "federal_anchor" rule.
    url = (source.url or "").lower()
    if "ecfr.gov" in url and "part-982" in url:
        created.append(
            PolicyAssertion(
                org_id=target_org_id,
                source_id=source.id,
                state=source.state,
                county=source.county,
                city=source.city,
                pha_name=source.pha_name,
                program_type=source.program_type or "hcv",
                rule_key="federal_hcv_regulations_anchor",
                value_json=_dumps(
                    {
                        "summary": "HCV regulations live in 24 CFR Part 982 (eCFR).",
                        "url": source.url,
                    }
                ),
                confidence=0.85,
                review_status="extracted",
                extracted_at=now,
            )
        )

    # NSPIRE / part 5 anchor
    if "ecfr.gov" in url and "/part-5" in url:
        created.append(
            PolicyAssertion(
                org_id=target_org_id,
                source_id=source.id,
                state=source.state,
                county=source.county,
                city=source.city,
                pha_name=source.pha_name,
                program_type=source.program_type or "hcv",
                rule_key="federal_nspire_anchor",
                value_json=_dumps(
                    {
                        "summary": "HUD program requirements and inspection standards (NSPIRE) are reflected in 24 CFR Part 5 (see current Subpart G).",
                        "url": source.url,
                    }
                ),
                confidence=0.75,
                review_status="extracted",
                extracted_at=now,
            )
        )

    # Michigan legislature anchor
    if "legislature.mi.gov" in url and "mcl" in url:
        created.append(
            PolicyAssertion(
                org_id=target_org_id,
                source_id=source.id,
                state="MI",
                county=source.county,
                city=source.city,
                pha_name=source.pha_name,
                program_type=source.program_type,
                rule_key="mi_statute_anchor",
                value_json=_dumps(
                    {
                        "summary": "Michigan statutory landlord/tenant baseline. Treat as primary for state-level rules referenced by underwriting/leases.",
                        "url": source.url,
                    }
                ),
                confidence=0.70,
                review_status="extracted",
                extracted_at=now,
            )
        )

    db.add_all(created)
    db.commit()

    # refresh IDs
    for a in created:
        db.refresh(a)

    return created