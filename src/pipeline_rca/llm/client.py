"""Tiny Ollama client - just urllib, so no extra dependency."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Callable

from pipeline_rca import config


class OllamaError(RuntimeError):
    pass


@dataclass
class ChatResult:
    text: str
    model: str
    seconds: float


@contextmanager
def _open(path: str, payload: dict | None, timeout: float):
    url = config.OLLAMA_HOST.rstrip("/") + path
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            yield resp
    except urllib.error.HTTPError as e:
        raise OllamaError(f"ollama returned {e.code}: {e.read().decode(errors='replace')[:200]}") from e
    except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
        raise OllamaError(f"can't reach ollama at {config.OLLAMA_HOST} ({e})") from e


def _request(path: str, payload: dict | None = None, timeout: float = 5) -> dict:
    with _open(path, payload, timeout) as resp:
        return json.loads(resp.read())


def available_models() -> list[str]:
    try:
        return [m["name"] for m in _request("/api/tags").get("models", [])]
    except OllamaError:
        return []


def is_available(model: str | None = None) -> bool:
    models = available_models()
    if not model:
        return bool(models)
    # "llama3.2" should match "llama3.2:latest"
    return any(m == model or m.split(":")[0] == model for m in models)


def chat(
    system: str,
    user: str,
    model: str | None = None,
    temperature: float = 0.2,
    on_token: Callable[[str], None] | None = None,
) -> ChatResult:
    """Send one chat request. Pass on_token to get the text as it's generated (for the UI)."""
    model = model or config.OLLAMA_MODEL
    start = time.perf_counter()
    payload = {
        "model": model,
        "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
        "stream": on_token is not None,
        "options": {"temperature": temperature, "num_ctx": 4096, "num_predict": 700},
    }

    if on_token is None:
        text = _request("/api/chat", payload, timeout=config.OLLAMA_TIMEOUT).get("message", {}).get("content", "")
    else:
        # streaming responses come back as one json object per line
        parts = []
        with _open("/api/chat", payload, config.OLLAMA_TIMEOUT) as resp:
            for line in resp:
                if not line.strip():
                    continue
                msg = json.loads(line)
                if "error" in msg:
                    raise OllamaError(msg["error"])
                chunk = msg.get("message", {}).get("content", "")
                if chunk:
                    parts.append(chunk)
                    on_token(chunk)
                if msg.get("done"):
                    break
        text = "".join(parts)

    text = text.strip()
    if not text:
        raise OllamaError("ollama returned an empty response")
    return ChatResult(text=text, model=model, seconds=round(time.perf_counter() - start, 1))
