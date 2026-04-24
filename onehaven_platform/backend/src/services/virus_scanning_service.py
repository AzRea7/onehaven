from __future__ import annotations

import os
import socket
from pathlib import Path
from typing import Any


class VirusScanResult(dict):
    pass


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def virus_scanning_enabled() -> bool:
    return _bool_env("ACQUISITION_VIRUS_SCAN_ENABLED", False) or _bool_env("COMPLIANCE_VIRUS_SCAN_ENABLED", False)


def scan_file(path: str | Path) -> dict[str, Any]:
    """
    Default-safe implementation:
    - If scanning disabled => mark as skipped
    - If ClamAV TCP is configured => try scan
    - If scan service errors => mark as error, caller can decide fail-open/fail-closed
    """
    path = Path(path)
    if not virus_scanning_enabled():
        return {
            "scan_status": "skipped",
            "scan_result": "disabled",
            "infected": False,
            "engine": None,
        }

    host = os.getenv("CLAMAV_HOST", "clamav")
    port = int(os.getenv("CLAMAV_PORT", "3310"))

    try:
        with socket.create_connection((host, port), timeout=10) as sock:
            sock.sendall(b"zINSTREAM\0")
            with path.open("rb") as f:
                while True:
                    chunk = f.read(8192)
                    if not chunk:
                        break
                    sock.sendall(len(chunk).to_bytes(4, "big"))
                    sock.sendall(chunk)
            sock.sendall((0).to_bytes(4, "big"))

            response = sock.recv(4096).decode("utf-8", errors="replace").strip()

        lowered = response.lower()
        if "ok" in lowered:
            return {
                "scan_status": "clean",
                "scan_result": response,
                "infected": False,
                "engine": "clamav",
            }
        if "found" in lowered:
            return {
                "scan_status": "infected",
                "scan_result": response,
                "infected": True,
                "engine": "clamav",
            }
        return {
            "scan_status": "error",
            "scan_result": response,
            "infected": False,
            "engine": "clamav",
        }
    except Exception as exc:
        return {
            "scan_status": "error",
            "scan_result": f"scan_failed: {exc}",
            "infected": False,
            "engine": "clamav",
        }
