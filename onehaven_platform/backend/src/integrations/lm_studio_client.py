# backend/app/integrations/lm_studio_client.py
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional

import httpx

from onehaven_platform.backend.src.config import settings


@dataclass
class LMStudioConfig:
    base_url: str = settings.lm_studio_base_url
    model: str = settings.lm_studio_model
    vision_model: str = settings.lm_studio_vision_model
    timeout_seconds: float = float(settings.lm_studio_timeout_seconds)
    enabled: bool = bool(settings.lm_studio_enabled)
    temperature: float = float(settings.lm_studio_temperature)
    max_tokens: int | None = settings.lm_studio_max_tokens
    api_key: str = getattr(settings, "lm_studio_api_key", "lm-studio")
    json_mode: bool = bool(getattr(settings, "lm_studio_json_mode", True))

    @property
    def chat_url(self) -> str:
        return f"{self.base_url.rstrip('/')}/chat/completions"


class LMStudioClient:
    def __init__(self, cfg: Optional[LMStudioConfig] = None):
        self.cfg = cfg or LMStudioConfig()

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        api_key = str(self.cfg.api_key or "").strip()
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        return headers

    def _request(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not self.cfg.enabled:
            raise RuntimeError("LM Studio is disabled")

        try:
            with httpx.Client(timeout=float(self.cfg.timeout_seconds)) as client:
                res = client.post(self.cfg.chat_url, json=payload, headers=self._headers())
                res.raise_for_status()
                data = res.json()
                if not isinstance(data, dict):
                    raise RuntimeError("LM Studio returned a non-object response")
                return data
        except httpx.HTTPStatusError as exc:
            body = ""
            try:
                body = exc.response.text
            except Exception:
                body = ""
            raise RuntimeError(f"LM Studio HTTP {exc.response.status_code}: {body}") from exc
        except httpx.HTTPError as exc:
            raise RuntimeError(f"LM Studio connection error: {exc}") from exc
        except Exception as exc:
            raise RuntimeError(f"LM Studio request failed: {exc}") from exc

    @staticmethod
    def _normalize_content(content: Any) -> Any:
        if isinstance(content, dict):
            return content

        if isinstance(content, list):
            text_parts: list[str] = []
            for part in content:
                if not isinstance(part, dict):
                    continue
                if isinstance(part.get("text"), str):
                    text_parts.append(part["text"])
                elif isinstance(part.get("content"), str):
                    text_parts.append(part["content"])
            content = "".join(text_parts)

        if isinstance(content, str):
            raw = content.strip()
            if not raw:
                return {}
            try:
                return json.loads(raw)
            except Exception:
                return {"text": raw}

        return {}

    @staticmethod
    def _normalize_response(data: Dict[str, Any], fallback_model: str) -> Dict[str, Any]:
        choice = ((data.get("choices") or [{}])[0] or {}).get("message") or {}
        normalized = LMStudioClient._normalize_content(choice.get("content"))

        return {
            "raw": data,
            "message": choice,
            "content": normalized,
            "usage": data.get("usage") or {},
            "model": data.get("model") or fallback_model,
            "finish_reason": ((data.get("choices") or [{}])[0] or {}).get("finish_reason"),
        }

    def _build_response_format(
        self,
        *,
        response_format: dict[str, Any] | None = None,
        json_schema: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if response_format:
            return response_format

        if json_schema is not None:
            # OpenAI-compatible structured output style
            return {
                "type": "json_schema",
                "json_schema": {
                    "name": "agent_response",
                    "schema": json_schema,
                },
            }

        if self.cfg.json_mode:
            return {"type": "json_object"}

        return None

    def chat_complete(
        self,
        *,
        system: str,
        user: str,
        temperature: float | None = None,
        response_format: dict[str, Any] | None = None,
        json_schema: dict[str, Any] | None = None,
        tools: list[dict[str, Any]] | None = None,
        tool_choice: str | None = None,
        model: str | None = None,
    ) -> Dict[str, Any]:
        resolved_model = model or self.cfg.model

        payload: Dict[str, Any] = {
            "model": resolved_model,
            "temperature": float(self.cfg.temperature if temperature is None else temperature),
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        }

        if self.cfg.max_tokens is not None:
            payload["max_tokens"] = int(self.cfg.max_tokens)

        final_response_format = self._build_response_format(
            response_format=response_format,
            json_schema=json_schema,
        )
        if final_response_format is not None:
            payload["response_format"] = final_response_format

        if tools:
            payload["tools"] = tools
        if tool_choice:
            payload["tool_choice"] = tool_choice

        data = self._request(payload)
        return self._normalize_response(data, resolved_model)

    def vision_complete(
        self,
        *,
        system: str,
        user: str,
        image_urls: list[str],
        response_format: dict[str, Any] | None = None,
        json_schema: dict[str, Any] | None = None,
        model: str | None = None,
        temperature: float | None = None,
    ) -> Dict[str, Any]:
        resolved_model = model or self.cfg.vision_model

        content: list[dict[str, Any]] = [{"type": "text", "text": user}]
        for url in image_urls:
            clean = str(url or "").strip()
            if not clean:
                continue
            content.append({"type": "image_url", "image_url": {"url": clean}})

        payload: Dict[str, Any] = {
            "model": resolved_model,
            "temperature": float(self.cfg.temperature if temperature is None else temperature),
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": content},
            ],
        }

        if self.cfg.max_tokens is not None:
            payload["max_tokens"] = int(self.cfg.max_tokens)

        final_response_format = self._build_response_format(
            response_format=response_format,
            json_schema=json_schema,
        )
        if final_response_format is not None:
            payload["response_format"] = final_response_format

        data = self._request(payload)
        return self._normalize_response(data, resolved_model)
    