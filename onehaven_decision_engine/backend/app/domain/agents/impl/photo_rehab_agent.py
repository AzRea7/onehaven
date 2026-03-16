from __future__ import annotations

from typing import Any, Optional

from sqlalchemy.orm import Session

from app.services.photo_rehab_agent import analyze_property_photos


def run(db: Session, *, org_id: int, property_id: Optional[int], input_payload: dict[str, Any]) -> dict[str, Any]:
    if property_id is None:
        return {
            "agent_key": "photo_rehab",
            "summary": "No property found for photo rehab.",
            "facts": {"property_id": property_id, "photo_count": 0},
            "actions": [],
        }

    analysis = analyze_property_photos(db, org_id=int(org_id), property_id=int(property_id))
    if not analysis.get("ok"):
        return {
            "agent_key": "photo_rehab",
            "summary": "Photo rehab analysis could not run because no photos are available.",
            "facts": {"property_id": property_id, "photo_count": 0, "code": analysis.get("code")},
            "actions": [
                {
                    "entity_type": "workflow_event",
                    "op": "create",
                    "data": {
                        "event_type": "photo_rehab_missing_photos",
                        "payload": {"property_id": int(property_id), "code": analysis.get("code")},
                    },
                    "reason": "Record that photo rehab was attempted without usable source images.",
                }
            ],
            "recommendations": [
                {
                    "type": "missing_photos",
                    "title": "Upload interior and exterior photos",
                    "reason": "Vision-driven rehab is only as smart as the pixels you feed the beast.",
                    "priority": "high",
                }
            ],
        }

    actions: list[dict[str, Any]] = []
    for issue in analysis.get("issues", []):
        actions.append(
            {
                "entity_type": "rehab_task",
                "op": "create",
                "data": {
                    "property_id": int(property_id),
                    "title": str(issue.get("title") or "Photo rehab issue"),
                    "category": str(issue.get("category") or "rehab"),
                    "status": "blocked" if bool(issue.get("blocker")) else "todo",
                    "inspection_relevant": bool(issue.get("blocker")),
                    "cost_estimate": float(issue.get("estimated_cost") or 0.0),
                    "notes": str(issue.get("notes") or ""),
                },
                "reason": "Translate structured photo issues into approval-gated rehab tasks.",
            }
        )

    actions.append(
        {
            "entity_type": "workflow_event",
            "op": "create",
            "data": {
                "event_type": "photo_rehab_analyzed",
                "payload": {
                    "property_id": int(property_id),
                    "photo_count": int(analysis.get("photo_count") or 0),
                    "issue_count": len(analysis.get("issues") or []),
                },
            },
            "reason": "Record photo-based rehab analysis completion.",
        }
    )

    return {
        "agent_key": "photo_rehab",
        "summary": "Photo-based rehab review produced structured issues and proposed tasks for human approval.",
        "facts": {
            "property_id": property_id,
            "photo_count": int(analysis.get("photo_count") or 0),
            "summary": analysis.get("summary") or {},
        },
        "actions": actions,
        "recommendations": [
            {
                "type": "rehab_scope_review",
                "title": "Review proposed rehab scope",
                "reason": "Use the image-driven task list as a fast first-pass scope, then sanity-check with a human eyeball.",
                "priority": "high",
            }
        ],
    }
