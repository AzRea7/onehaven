# backend/app/domain/agents/impl/photo_rehab_agent.py
from __future__ import annotations

import json
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.domain.agents.llm_router import run_llm_agent
from app.models import Property


def _extract_photo_urls(prop: Any, input_payload: dict[str, Any]) -> list[str]:
    urls: list[str] = []

    if isinstance(input_payload.get("image_urls"), list):
        urls.extend([str(x).strip() for x in input_payload["image_urls"] if str(x).strip()])

    for field in (
        "photos_json",
        "image_urls_json",
        "zillow_photos_json",
        "listing_photos_json",
        "photo_urls_json",
    ):
        raw = getattr(prop, field, None)
        if isinstance(raw, list):
            urls.extend([str(x).strip() for x in raw if str(x).strip()])
        elif isinstance(raw, str) and raw.strip():
            try:
                decoded = json.loads(raw)
                if isinstance(decoded, list):
                    urls.extend([str(x).strip() for x in decoded if str(x).strip()])
                else:
                    urls.append(raw.strip())
            except Exception:
                urls.append(raw.strip())

    deduped: list[str] = []
    seen: set[str] = set()
    for url in urls:
        if url and url not in seen:
            deduped.append(url)
            seen.add(url)
    return deduped[:32]


def run_photo_rehab_agent(
    db: Session,
    org_id: int,
    property_id: Optional[int],
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    if property_id is None:
        return {
            "agent_key": "photo_rehab",
            "summary": "Photo rehab skipped because property_id is missing.",
            "facts": {"property_id": property_id},
            "actions": [],
            "recommendations": [
                {
                    "type": "missing_property_id",
                    "reason": "A property_id is required before photo rehab can run.",
                    "priority": "high",
                }
            ],
        }

    prop = db.scalar(select(Property).where(Property.org_id == int(org_id), Property.id == int(property_id)))
    if prop is None:
        return {
            "agent_key": "photo_rehab",
            "summary": "No property found.",
            "facts": {"property_id": property_id},
            "actions": [],
            "recommendations": [],
        }

    image_urls = _extract_photo_urls(prop, input_payload)
    if not image_urls:
        return {
            "agent_key": "photo_rehab",
            "summary": "Photo rehab cannot run because no property/listing photos were found.",
            "facts": {"property_id": int(property_id), "photos_available": False, "photo_count": 0},
            "actions": [],
            "recommendations": [
                {
                    "type": "missing_property_photos",
                    "reason": "Attach property/listing photos before running photo-based rehab analysis.",
                    "priority": "high",
                }
            ],
        }

    # deterministic seed action for fallback
    fallback_actions = [
        {
            "entity_type": "workflow_event",
            "op": "create",
            "data": {
                "property_id": int(property_id),
                "event_type": "photo_rehab_review_requested",
                "payload": {"photo_count": len(image_urls)},
            },
            "reason": "Create an audit breadcrumb that photo rehab review was requested.",
        }
    ]

    deterministic = {
        "agent_key": "photo_rehab",
        "summary": "Photo rehab vision pass is ready to analyze property photos.",
        "facts": {
            "property_id": int(property_id),
            "photos_available": True,
            "photo_count": len(image_urls),
            "sample_image_urls": image_urls[:5],
        },
        "actions": fallback_actions,
        "recommendations": [
            {
                "type": "photo_rehab_ready",
                "reason": "Photos are available; run a human-reviewed defect extraction pass.",
                "priority": "medium",
            }
        ],
        "confidence": 0.70,
        "needs_human_review": True,
    }

    try:
        llm_output = run_llm_agent(
            agent_key="photo_rehab",
            context={"property_id": int(property_id), "input_payload": input_payload},
            mode="llm_vision",
            image_urls=image_urls,
        )
        llm_output["agent_key"] = "photo_rehab"
        llm_output["facts"] = {
            "property_id": int(property_id),
            "photos_available": True,
            "photo_count": len(image_urls),
            "sample_image_urls": image_urls[:5],
            **(llm_output.get("facts") or {}),
        }
        if not isinstance(llm_output.get("actions"), list):
            llm_output["actions"] = fallback_actions
        llm_output["needs_human_review"] = True
        return llm_output
    except Exception:
        return deterministic
    