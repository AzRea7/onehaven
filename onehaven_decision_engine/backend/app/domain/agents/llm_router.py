from __future__ import annotations

import json
from typing import Any, Dict

from ...integrations.lm_studio_client import LMStudioClient


def _schema_response_format(schema: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "json_schema",
        "json_schema": {
            "name": "onehaven_agent_output",
            "schema": schema,
        },
    }


class LMStudioProvider:
    def __init__(self) -> None:
        self.client = LMStudioClient()

    def chat_complete(
        self,
        *,
        system: str,
        user: str,
        schema: dict[str, Any] | None = None,
        model: str | None = None,
        temperature: float = 0.2,
    ) -> Dict[str, Any]:
        response = self.client.chat_complete(
            system=system,
            user=user,
            temperature=temperature,
            response_format=_schema_response_format(schema) if schema else None,
            model=model,
        )
        content = response.get("content")
        if isinstance(content, list):
            content = "".join(str(part.get("text") or "") for part in content if isinstance(part, dict))
        if isinstance(content, dict):
            return content
        if isinstance(content, str):
            return json.loads(content)
        raise ValueError("LM Studio returned empty or unsupported content")


def run_llm_agent(*, agent_key: str, context: Dict[str, Any], prompt: str, schema: dict[str, Any], model: str | None = None) -> Dict[str, Any]:
    system = (
        "You are a structured real-estate workflow agent for OneHaven. "
        "Return only JSON that exactly follows the provided schema. "
        "Never invent properties, values, or citations. If data is missing, state that plainly in JSON."
    )
    user = json.dumps({"agent_key": agent_key, "context": context, "prompt": prompt}, ensure_ascii=False)
    provider = LMStudioProvider()
    return provider.chat_complete(system=system, user=user, schema=schema, model=model)
