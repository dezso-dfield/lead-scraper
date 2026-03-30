"""
App settings — persisted to ~/.scraper/settings.json, overridable by env vars.
"""
from __future__ import annotations
import json
import os
from pathlib import Path

SETTINGS_PATH = Path.home() / ".scraper" / "settings.json"

DEFAULTS: dict = {
    # SMTP
    "smtp_host":     "",
    "smtp_port":     587,
    "smtp_ssl":      False,
    "smtp_starttls": True,
    "smtp_user":     "",
    "smtp_password": "",
    "from_name":     "",
    "from_email":    "",
    # Sending behaviour
    "delay_min":     5,
    "delay_max":     15,
    "daily_limit":   500,
    "auto_contacted": True,   # mark as contacted after send
    "unsubscribe_footer": True,
}

# Environment variable names that map to settings keys
ENV_MAP: dict[str, str] = {
    "smtp_host":     "SMTP_HOST",
    "smtp_port":     "SMTP_PORT",
    "smtp_ssl":      "SMTP_SSL",
    "smtp_starttls": "SMTP_STARTTLS",
    "smtp_user":     "SMTP_USER",
    "smtp_password": "SMTP_PASSWORD",
    "from_name":     "FROM_NAME",
    "from_email":    "FROM_EMAIL",
}


def load() -> dict:
    s = dict(DEFAULTS)
    if SETTINGS_PATH.exists():
        try:
            saved = json.loads(SETTINGS_PATH.read_text())
            s.update({k: v for k, v in saved.items() if k in DEFAULTS})
        except Exception:
            pass
    for key, env_key in ENV_MAP.items():
        val = os.environ.get(env_key, "").strip()
        if val:
            if key == "smtp_port":
                s[key] = int(val)
            elif key in ("smtp_ssl", "smtp_starttls"):
                s[key] = val.lower() in ("1", "true", "yes")
            else:
                s[key] = val
    return s


def save(updates: dict) -> dict:
    current = load()
    locked = env_locked()
    for key, env_key in ENV_MAP.items():
        if key in locked:
            updates.pop(key, None)
    current.update({k: v for k, v in updates.items() if k in DEFAULTS})
    SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    SETTINGS_PATH.write_text(json.dumps(current, indent=2))
    return load()


def env_locked() -> set[str]:
    """Keys overridden by environment variables — cannot be edited in UI."""
    return {key for key, env_key in ENV_MAP.items() if os.environ.get(env_key, "").strip()}


def get_for_ui() -> dict:
    """Settings with password masked + which keys are env-locked."""
    s = load()
    if s.get("smtp_password"):
        s["smtp_password"] = "••••••••"
    s["_env_locked"] = list(env_locked())
    return s
