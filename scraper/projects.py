"""
Project management — each project has its own leads DB, settings, and .env.

Structure:
  ~/.scraper/
    projects.json          # [{id, name, color, created_at}]
    active_project.txt     # current project id
    settings.json          # global settings
    .env                   # global env vars
    leads.db               # default project DB (backward compat)
    projects/
      {id}/
        leads.db
        settings.json      # project overrides
        .env               # project env vars
"""
from __future__ import annotations
import json
import os
from datetime import datetime
from pathlib import Path

# On Vercel (read-only FS except /tmp) store data in /tmp
_base = Path("/tmp") if os.environ.get("VERCEL") else Path.home()
SCRAPER_DIR   = _base / ".scraper"
PROJECTS_DIR  = SCRAPER_DIR / "projects"
PROJECTS_FILE = SCRAPER_DIR / "projects.json"
ACTIVE_FILE   = SCRAPER_DIR / "active_project.txt"

COLOR_PALETTE = [
    "#6366f1", "#22c55e", "#f59e0b", "#ef4444",
    "#06b6d4", "#a855f7", "#ec4899", "#14b8a6",
]

DEFAULT_PROJECT: dict = {
    "id":         "default",
    "name":       "Default",
    "color":      "#6366f1",
    "created_at": "2024-01-01T00:00:00",
}


# ─── Persistence ─────────────────────────────────────────────────────────────

def _load() -> list[dict]:
    if PROJECTS_FILE.exists():
        try:
            data = json.loads(PROJECTS_FILE.read_text())
            if data and isinstance(data, list):
                return data
        except Exception:
            pass
    return [DEFAULT_PROJECT]


def _save(projects: list[dict]) -> None:
    SCRAPER_DIR.mkdir(parents=True, exist_ok=True)
    PROJECTS_FILE.write_text(json.dumps(projects, indent=2))


def _ensure_default(projects: list[dict]) -> list[dict]:
    if not any(p["id"] == "default" for p in projects):
        projects.insert(0, DEFAULT_PROJECT)
    return projects


# ─── Public API ──────────────────────────────────────────────────────────────

def list_projects() -> list[dict]:
    return _ensure_default(_load())


def get_project(project_id: str) -> dict | None:
    for p in _load():
        if p["id"] == project_id:
            return p
    if project_id == "default":
        return DEFAULT_PROJECT
    return None


def create_project(name: str, color: str = "") -> dict:
    projects = _ensure_default(_load())

    # Derive a slug id
    slug = name.lower().strip()
    slug = "".join(c if c.isalnum() or c == "-" else "-" for c in slug).strip("-") or "project"
    existing_ids = {p["id"] for p in projects}
    base, counter = slug, 2
    while slug in existing_ids:
        slug = f"{base}-{counter}"
        counter += 1

    color = color or COLOR_PALETTE[len(projects) % len(COLOR_PALETTE)]
    project = {
        "id":         slug,
        "name":       name,
        "color":      color,
        "created_at": datetime.utcnow().isoformat(),
    }
    projects.append(project)
    _save(projects)

    # Ensure directory exists
    proj_dir = PROJECTS_DIR / slug
    proj_dir.mkdir(parents=True, exist_ok=True)

    return project


def update_project(project_id: str, name: str | None = None, color: str | None = None) -> dict:
    projects = _ensure_default(_load())
    for p in projects:
        if p["id"] == project_id:
            if name is not None:
                p["name"] = name
            if color is not None:
                p["color"] = color
            break
    _save(projects)
    return get_project(project_id)


def delete_project(project_id: str) -> None:
    if project_id == "default":
        raise ValueError("Cannot delete the Default project")
    projects = [p for p in _load() if p["id"] != project_id]
    _save(projects)
    # Switch away if this was active
    if get_active_id() == project_id:
        set_active("default")


# ─── Active project ───────────────────────────────────────────────────────────

def get_active_id() -> str:
    if ACTIVE_FILE.exists():
        pid = ACTIVE_FILE.read_text().strip()
        if get_project(pid):
            return pid
    return "default"


def set_active(project_id: str) -> str:
    if not get_project(project_id):
        raise ValueError(f"Project {project_id!r} not found")
    SCRAPER_DIR.mkdir(parents=True, exist_ok=True)
    ACTIVE_FILE.write_text(project_id)
    return project_id


# ─── Path helpers ─────────────────────────────────────────────────────────────

def get_db_path(project_id: str) -> Path:
    """Return the SQLite DB path for a project."""
    if project_id == "default":
        return SCRAPER_DIR / "leads.db"   # backward compat — existing data
    path = PROJECTS_DIR / project_id / "leads.db"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def get_settings_path(project_id: str) -> Path:
    if project_id == "default":
        return SCRAPER_DIR / "settings.json"
    return PROJECTS_DIR / project_id / "settings.json"


def get_env_path(project_id: str) -> Path:
    if project_id == "default":
        return SCRAPER_DIR / ".env"
    return PROJECTS_DIR / project_id / ".env"
