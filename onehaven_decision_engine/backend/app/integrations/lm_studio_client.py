from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

import httpx


@dataclass
class LMStudioConfig:
    base_url: str = os.getenv("LM_STUDIO_BASE_URL", "http://127.0.0.1:1234")
    api_path: str = os.getenv("LM_STUDIO_API_PATH", "/v1")
    model: str = os.getenv("LM_STUDIO_MODEL", "qwen3-coder-30b-a3b-instruct")
    vision_model: str = os.getenv("LM_STUDIO_VISION_MODEL", "qwen2.5-vl-7b-instruct")
    timeout_seconds: float = float(os.getenv("LM_STUDIO_TIMEOUT_SECONDS", "90"))
    enabled: bool = os.getenv("LM_STUDIO_ENABLED", "1").strip().lower() not in {"0", "false", "no"}


class LMStudioClient:
    def __init__(self, cfg: Optional[LMStudioConfig] = None):
        self.cfg = cfg or LMStudioConfig()

    @property
    def chat_url(self) -> str:
        return f"{self.cfg.base_url.rstrip('/')}{self.cfg.api_path}/chat/completions"

    def chat_complete(
        self,
        *,
        system: str,
        user: str,
        temperature: float = 0.2,
        response_format: dict[str, Any] | None = None,
        model: str | None = None,
    ) -> Dict[str, Any]:
        if not self.cfg.enabled:
            raise RuntimeError("LM Studio is disabled")

        payload: Dict[str, Any] = {
            "model": model or self.cfg.model,
            "temperature": float(temperature),
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        }
        if response_format:
            payload["response_format"] = response_format

        with httpx.Client(timeout=self.cfg.timeout_seconds) as client:
            res = client.post(self.chat_url, json=payload)
            res.raise_for_status()
            data = res.json()

        choice = ((data.get("choices") or [{}])[0] or {}).get("message") or {}
        content = choice.get("content")
        return {
            "raw": data,
            "message": choice,
            "content": content,
            "usage": data.get("usage") or {},
            "model": data.get("model") or payload["model"],
        }
