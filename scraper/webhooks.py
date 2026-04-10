"""
Webhook dispatcher — fires HTTP POST to configured URLs on lead events.
Events: status_changed, lead_qualified, lead_replied, email_opened
"""
from __future__ import annotations
import json
import threading
from datetime import datetime
from typing import Any


def fire(event: str, payload: dict, db) -> None:
    """Fire webhooks for the given event in a background thread."""
    try:
        hooks = db.fetch_webhooks(event=event)
    except Exception:
        return
    if not hooks:
        return
    threading.Thread(target=_dispatch_all, args=(hooks, event, payload), daemon=True).start()


def _dispatch_all(hooks: list[dict], event: str, payload: dict) -> None:
    try:
        import httpx
    except ImportError:
        return

    body = json.dumps({
        "event":   event,
        "payload": payload,
        "fired_at": datetime.utcnow().isoformat(),
    })
    headers = {"Content-Type": "application/json", "User-Agent": "LeadManager-Webhook/1.0"}

    for hook in hooks:
        url = hook.get("url", "")
        if not url:
            continue
        try:
            httpx.post(url, content=body, headers=headers, timeout=10)
        except Exception:
            pass  # Webhooks are fire-and-forget
