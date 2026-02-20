# backend/app/integrations/lm_studio_client.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class LMStudioConfig:
    """
    Placeholder config for LM Studio (future).
    Typical LM Studio exposes an OpenAI-compatible API at:
      http://localhost:1234/v1/chat/completions
    but you can wire this however you want later.
    """
    base_url: str = "http://localhost:1234"
    api_path: str = "/v1"
    model: str = "local-model"


class LMStudioClient:
    """
    Placeholder client. Intentionally no network calls yet.
    When you’re ready, implement:
      - chat_complete(prompt, system, tools?)
      - embeddings(texts)
    """
    def __init__(self, cfg: Optional[LMStudioConfig] = None):
        self.cfg = cfg or LMStudioConfig()

    def chat_complete(self, *, system: str, user: str, temperature: float = 0.2) -> Dict[str, Any]:
        raise NotImplementedError("LM Studio integration not enabled yet.")

    def embeddings(self, texts: list[str]) -> list[list[float]]:
        raise NotImplementedError("LM Studio embeddings not enabled yet.")