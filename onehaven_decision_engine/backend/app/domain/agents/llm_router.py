# backend/app/domain/agents/llm_router.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from ...integrations.lm_studio_client import LMStudioClient


class LLMProvider:
    """
    Provider interface. Later you can implement:
      - OpenAIProvider
      - AnthropicProvider
      - LMStudioProvider (OpenAI-compatible local)
    """
    def chat_complete(
        self,
        *,
        system: str,
        user: str,
        temperature: float = 0.2,
        tools: Optional[List[dict[str, Any]]] = None,
        tool_choice: Optional[str] = None,
    ) -> Dict[str, Any]:
        raise NotImplementedError


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
    ) -> Dict[str, Any]:
        # LMStudioClient is intentionally not enabled yet in your repo.
        # When you enable it, this becomes the place to add tool calling.
        return self.client.chat_complete(system=system, user=user, temperature=temperature)


def run_llm_agent(*, agent_key: str, context: Dict[str, Any]) -> Dict[str, Any]:
    """
    LLM seam (SAFE by design):
      - LLM will only ever PROPOSE output (summary/facts/recommendations/actions)
      - validate_agent_output() must pass before anything can be applied
      - apply_run_actions() remains the only place that mutates DB

    Not enabled yet.
    """
    _ = agent_key
    _ = context
    _ = LMStudioProvider()
    raise NotImplementedError("LLM agent routing not enabled yet.")
