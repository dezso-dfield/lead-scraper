"""
App settings — two layers:
  1. Global:  ~/.scraper/settings.json  (+ ~/.scraper/.env)
  2. Project: ~/.scraper/projects/{id}/settings.json  (+ .env)
Project values override global ones.

Environment variables (SMTP_HOST etc.) override everything.
"""
from __future__ import annotations
import json
import os
from pathlib import Path

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
    "auto_contacted": True,
    "unsubscribe_footer": True,
}

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


def _parse_env_file(path: Path) -> dict[str, str]:
    """Parse a KEY=value .env file into a dict (ignores comments/blanks)."""
    result: dict[str, str] = {}
    if not path.exists():
        return result
    for line in path.read_text(errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, _, val = line.partition("=")
            result[key.strip()] = val.strip().strip('"').strip("'")
    return result


def _load_json(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            pass
    return {}


def load(project_id: str | None = None) -> dict:
    """
    Load merged settings: defaults → global json → global .env file →
    project json → project .env file → environment variables.
    """
    from scraper.projects import get_settings_path, get_env_path, SCRAPER_DIR

    s = dict(DEFAULTS)

    # 1. Global settings.json
    global_json = SCRAPER_DIR / "settings.json"
    s.update({k: v for k, v in _load_json(global_json).items() if k in DEFAULTS})

    # 2. Global .env file
    global_env = _parse_env_file(SCRAPER_DIR / ".env")
    _apply_env_vars(s, global_env)

    # 3. Project overrides
    if project_id and project_id != "default":
        proj_json = get_settings_path(project_id)
        s.update({k: v for k, v in _load_json(proj_json).items() if k in DEFAULTS})

        proj_env = _parse_env_file(get_env_path(project_id))
        _apply_env_vars(s, proj_env)

    # 4. Real OS environment variables (highest priority)
    _apply_env_vars(s, dict(os.environ))

    return s


def _apply_env_vars(s: dict, env: dict) -> None:
    for key, env_key in ENV_MAP.items():
        val = env.get(env_key, "").strip()
        if val:
            if key == "smtp_port":
                try:
                    s[key] = int(val)
                except ValueError:
                    pass
            elif key in ("smtp_ssl", "smtp_starttls"):
                s[key] = val.lower() in ("1", "true", "yes")
            else:
                s[key] = val


def save(updates: dict, project_id: str | None = None) -> dict:
    """
    Save settings to the appropriate json file.
    Env-var-locked keys are not overwritten.
    Returns the merged settings after save.
    """
    from scraper.projects import get_settings_path, SCRAPER_DIR

    locked = env_locked()
    clean = {k: v for k, v in updates.items() if k in DEFAULTS and k not in locked}

    if project_id and project_id != "default":
        path = get_settings_path(project_id)
        path.parent.mkdir(parents=True, exist_ok=True)
    else:
        path = SCRAPER_DIR / "settings.json"
        SCRAPER_DIR.mkdir(parents=True, exist_ok=True)

    current = _load_json(path)
    current.update(clean)
    path.write_text(json.dumps(current, indent=2))

    return load(project_id)


def env_locked() -> set[str]:
    """Keys overridden by real OS env vars — cannot be edited in UI."""
    return {key for key, env_key in ENV_MAP.items() if os.environ.get(env_key, "").strip()}


def get_for_ui(project_id: str | None = None) -> dict:
    """Settings with password masked + locked keys + project id."""
    s = load(project_id)
    if s.get("smtp_password"):
        s["smtp_password"] = "••••••••"
    s["_env_locked"] = list(env_locked())
    s["_project_id"] = project_id or "default"
    return s


# ─── .env file editing ───────────────────────────────────────────────────────

def read_env_file(project_id: str | None = None) -> str:
    """Return raw text content of the .env file for a project (or global)."""
    from scraper.projects import get_env_path, SCRAPER_DIR
    if project_id and project_id != "default":
        path = get_env_path(project_id)
    else:
        path = SCRAPER_DIR / ".env"
    return path.read_text(errors="replace") if path.exists() else ""


def write_env_file(content: str, project_id: str | None = None) -> None:
    """Write raw text content back to the .env file."""
    from scraper.projects import get_env_path, SCRAPER_DIR
    if project_id and project_id != "default":
        path = get_env_path(project_id)
        path.parent.mkdir(parents=True, exist_ok=True)
    else:
        path = SCRAPER_DIR / ".env"
        SCRAPER_DIR.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
