# backend/app/domain/agents/llm_router.py
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from app.config import settings
from app.domain.agents.contracts import canonical_agent_key, get_contract
from app.integrations.lm_studio_client import LMStudioClient


class LLMProvider:
    def chat_complete(
        self,
        *,
        system: str,
        user: str,
        temperature: float = 0.2,
        tools: Optional[List[dict[str, Any]]] = None,
        tool_choice: Optional[str] = None,
        json_schema: Optional[dict[str, Any]] = None,
        model: Optional[str] = None,
    ) -> Dict[str, Any]:
        raise NotImplementedError

    def vision_complete(
        self,
        *,
        system: str,
        user: str,
        image_urls: List[str],
        temperature: float = 0.2,
        json_schema: Optional[dict[str, Any]] = None,
        model: Optional[str] = None,
    ) -> Dict[str, Any]:
        raise NotImplementedError


def _loads_maybe_json(text: Any) -> Any:
    if text is None:
        return None
    if isinstance(text, (dict, list)):
        return text
    if not isinstance(text, str):
        return text
    s = text.strip()
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        return s


def _ensure_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {"raw": value}


def _schema_for_agent(agent_key: str) -> dict[str, Any]:
    contract = get_contract(agent_key)
    wants_actions = contract.mode != "recommend_only"

    base = {
        "type": "object",
        "additionalProperties": True,
        "properties": {
            "agent_key": {"type": "string"},
            "summary": {"type": "string"},
            "facts": {"type": "object"},
            "recommendations": {"type": "array"},
            "citations": {"type": "array"},
            "confidence": {"type": "number"},
            "needs_human_review": {"type": "boolean"},
        },
        "required": ["agent_key", "summary", "facts"],
    }
    if wants_actions:
        base["properties"]["actions"] = {"type": "array"}
        base["required"].append("actions")
    else:
        base["properties"]["actions"] = {"type": "array"}
        base["properties"]["recommendations"] = {"type": "array"}
    return base


def _extract_message_content(resp: Any) -> Any:
    if isinstance(resp, dict):
        for key in ("output", "content", "message", "text", "response"):
            if key in resp:
                return resp[key]

        choices = resp.get("choices")
        if isinstance(choices, list) and choices:
            first = choices[0]
            if isinstance(first, dict):
                if isinstance(first.get("message"), dict):
                    msg = first["message"]
                    if "content" in msg:
                        return msg["content"]
                if "text" in first:
                    return first["text"]
    return resp


class LMStudioProvider(LLMProvider):
    def __init__(self) -> None:
        self.client = LMStudioClient()

    def chat_complete(
        self,
        *,
        system: str,
        user: str,
        temperature: float = 0.2,
        tools: Optional[List[dict[str, Any]]] = None,
        tool_choice: Optional[str] = None,
        json_schema: Optional[dict[str, Any]] = None,
        model: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "system": system,
            "user": user,
            "temperature": float(temperature),
        }
        if tools:
            payload["tools"] = tools
        if tool_choice:
            payload["tool_choice"] = tool_choice
        if json_schema is not None:
            payload["json_schema"] = json_schema
        if model:
            payload["model"] = model

        if hasattr(self.client, "chat_complete"):
            try:
                return _ensure_object(self.client.chat_complete(**payload))
            except TypeError:
                return _ensure_object(
                    self.client.chat_complete(
                        system=system,
                        user=user,
                        temperature=float(temperature),
                    )
                )

        raise RuntimeError("LMStudioClient.chat_complete is not available")

    def vision_complete(
        self,
        *,
        system: str,
        user: str,
        image_urls: List[str],
        temperature: float = 0.2,
        json_schema: Optional[dict[str, Any]] = None,
        model: Optional[str] = None,
    ) -> Dict[str, Any]:
        if hasattr(self.client, "vision_complete"):
            return _ensure_object(
                self.client.vision_complete(
                    system=system,
                    user=user,
                    image_urls=image_urls,
                    temperature=float(temperature),
                    json_schema=json_schema,
                    model=model or getattr(settings, "lm_studio_vision_model", None),
                )
            )

        # graceful fallback: stuff image URLs into text prompt if only chat is available
        synthetic_user = (
            f"{user}\n\nIMAGE_URLS:\n" + "\n".join(image_urls[:24])
            if image_urls
            else user
        )
        return self.chat_complete(
            system=system,
            user=synthetic_user,
            temperature=temperature,
            json_schema=json_schema,
            model=model or getattr(settings, "lm_studio_vision_model", None),
        )


def _normalize_llm_output(agent_key: str, raw: Any) -> Dict[str, Any]:
    expected_key = canonical_agent_key(agent_key)

    content = _extract_message_content(raw)
    parsed = _loads_maybe_json(content)

    if isinstance(parsed, list):
        parsed = {
            "agent_key": expected_key,
            "summary": f"{expected_key} returned list output",
            "facts": {"items_count": len(parsed)},
            "recommendations": [{"type": "raw_list_output", "reason": "LLM returned a list payload", "items": parsed}],
            "actions": [],
        }

    if not isinstance(parsed, dict):
        parsed = {
            "agent_key": expected_key,
            "summary": f"{expected_key} returned non-JSON output",
            "facts": {"raw_type": type(parsed).__name__},
            "recommendations": [{"type": "raw_output", "reason": "LLM returned non-object output", "raw": parsed}],
            "actions": [],
        }

    parsed["agent_key"] = expected_key
    parsed.setdefault("summary", f"{expected_key} completed")
    parsed.setdefault("facts", {})
    parsed.setdefault("recommendations", [])
    parsed.setdefault("actions", [])
    parsed.setdefault("citations", [])
    parsed.setdefault("confidence", 0.65)
    parsed.setdefault("needs_human_review", False)
    return parsed


def _default_system_prompt(agent_key: str) -> str:
    contract = get_contract(agent_key)
    action_rule = (
        "You may propose actions, but only if they match the requested schema exactly."
        if contract.mode != "recommend_only"
        else "Do not propose actions. Return actions as an empty array."
    )

    return (
        "You are a backend specialist agent for a real-estate SaaS system. "
        "Return valid JSON only. No markdown, no prose outside JSON. "
        "Be conservative, factual, and audit-friendly. "
        f"{action_rule} "
        "Never invent database IDs. "
        "Summaries should be short and operational. "
        "Facts should be deterministic or clearly labeled as estimated. "
        "Recommendations should be ranked and practical."
    )


def run_llm_agent(
    *,
    agent_key: str,
    context: Dict[str, Any],
    mode: Optional[str] = None,
    image_urls: Optional[List[str]] = None,
) -> Dict[str, Any]:
    if not bool(getattr(settings, "lm_studio_enabled", True)):
        raise RuntimeError("LM Studio is disabled")

    resolved_key = canonical_agent_key(agent_key)
    contract = get_contract(resolved_key)
    provider = LMStudioProvider()

    system = _default_system_prompt(resolved_key)
    user = json.dumps(
        {
            "agent_key": resolved_key,
            "contract_mode": contract.mode,
            "llm_mode": mode or contract.llm_mode,
            "context": context,
        },
        ensure_ascii=False,
        default=str,
    )
    schema = _schema_for_agent(resolved_key)

    llm_mode = (mode or contract.llm_mode or "deterministic").strip().lower()
    if llm_mode == "llm_vision":
        raw = provider.vision_complete(
            system=system,
            user=user,
            image_urls=image_urls or [],
            temperature=float(getattr(settings, "lm_studio_temperature", 0.2)),
            json_schema=schema,
            model=getattr(settings, "lm_studio_vision_model", None),
        )
    else:
        raw = provider.chat_complete(
            system=system,
            user=user,
            temperature=float(getattr(settings, "lm_studio_temperature", 0.2)),
            json_schema=schema,
            model=getattr(settings, "lm_studio_model", None),
        )

    return _normalize_llm_output(resolved_key, raw)
