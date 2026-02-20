# backend/app/domain/agents/llm_router.py
from __future__ import annotations

from typing import Any, Dict

from ...integrations.lm_studio_client import LMStudioClient


def run_llm_agent(*, agent_key: str, context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Placeholder seam:
    - later you’ll route certain agents here (llm_capable=True)
    - keep deterministic outputs as default until you flip the switch
    """
    _ = LMStudioClient()  # not used yet
    raise NotImplementedError("LLM agent routing not enabled yet.")