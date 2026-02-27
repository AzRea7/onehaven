# backend/app/domain/agents/apply.py
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from app.domain.agents.contracts import get_contract
from app.domain.agents.validate import validate_actions, ValidationResult


@dataclass(frozen=True)
class PreparedActions:
    """
    Pure-domain artifact: safe to compute without DB.
    Intended use:
      - normalize + validate actions before persisting to AgentRun.proposed_actions_json
      - compute fingerprints for idempotency/deduplication
      - produce a small summary for UI/logging
    """
    ok: bool
    errors: list[str]
    actions: list[dict[str, Any]]
    fingerprints: list[str]
    summary: dict[str, Any]


def _loads_json(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, (dict, list, int, float, bool)):
        return val
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        try:
            return json.loads(s)
        except Exception:
            return val
    return val


def _canonical_action(a: dict[str, Any]) -> dict[str, Any]:
    """
    Canonical shape so hashing + comparisons are stable.
    We intentionally preserve only the keys we care about.
    """
    return {
        "entity_type": str(a.get("entity_type") or "").strip(),
        "op": str(a.get("op") or "").strip(),
        "data": a.get("data") if isinstance(a.get("data"), dict) else {},
        # optional metadata
        "reason": str(a.get("reason") or "").strip() or None,
    }


def _fingerprint(obj: Any) -> str:
    blob = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:32]


def _summarize(actions: list[dict[str, Any]]) -> dict[str, Any]:
    by_type: dict[str, int] = {}
    by_op: dict[str, int] = {}
    pairs: dict[str, int] = {}

    for a in actions:
        et = str(a.get("entity_type") or "unknown")
        op = str(a.get("op") or "unknown")
        by_type[et] = by_type.get(et, 0) + 1
        by_op[op] = by_op.get(op, 0) + 1
        k = f"{et}:{op}"
        pairs[k] = pairs.get(k, 0) + 1

    return {
        "count": len(actions),
        "by_entity_type": by_type,
        "by_operation": by_op,
        "by_pair": pairs,
    }


def prepare_actions(agent_key: str, actions_raw: Any) -> PreparedActions:
    """
    Pure function. No DB writes.

    Steps:
      1) Decode/normalize raw input into list[dict]
      2) Contract validate (allowed entity_type/op + minimal shape)
      3) Canonicalize for stable hashing
      4) Fingerprint each action for dedupe/idempotency
    """
    decoded = _loads_json(actions_raw)

    if decoded is None:
        return PreparedActions(ok=True, errors=[], actions=[], fingerprints=[], summary=_summarize([]))

    if isinstance(decoded, dict):
        decoded = [decoded]

    if not isinstance(decoded, list):
        return PreparedActions(
            ok=False,
            errors=[f"actions must be list or object; got {type(decoded).__name__}"],
            actions=[],
            fingerprints=[],
            summary=_summarize([]),
        )

    # Keep only dicts; fail if anything else is present (strict by default)
    raw_actions: list[Any] = decoded
    for i, a in enumerate(raw_actions):
        if not isinstance(a, dict):
            return PreparedActions(
                ok=False,
                errors=[f"actions[{i}] must be object; got {type(a).__name__}"],
                actions=[],
                fingerprints=[],
                summary=_summarize([]),
            )

    # Contract allow-list validation
    vr: ValidationResult = validate_actions(str(agent_key), raw_actions)
    if not vr.ok:
        return PreparedActions(
            ok=False,
            errors=list(vr.errors),
            actions=[],
            fingerprints=[],
            summary=_summarize([]),
        )

    # Canonicalize + fingerprint
    canonical: list[dict[str, Any]] = [_canonical_action(a) for a in raw_actions]  # type: ignore[arg-type]
    fps = [_fingerprint(a) for a in canonical]

    # Optional: remove exact duplicates while preserving order
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    deduped_fps: list[str] = []
    for a, fp in zip(canonical, fps):
        if fp in seen:
            continue
        seen.add(fp)
        deduped.append(a)
        deduped_fps.append(fp)

    return PreparedActions(
        ok=True,
        errors=[],
        actions=deduped,
        fingerprints=deduped_fps,
        summary=_summarize(deduped),
    )
