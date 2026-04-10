"""
FastAPI web server — serves the Lead Manager UI on http://localhost:7337
"""
from __future__ import annotations
import asyncio
import io
import json
import os
import re
import threading
import uuid
from pathlib import Path
from queue import Empty, Queue
from typing import AsyncIterator

from fastapi import BackgroundTasks, FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel

import scraper.db as db
import scraper.settings as settings_mod
import scraper.projects as projects_mod

STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(title="Lead Manager", docs_url=None, redoc_url=None)

# ─── Active project + DB helper ──────────────────────────────────────────────

_active_project_id: str = projects_mod.get_active_id()


def _db() -> db.Database:
    """Return the Database instance for the currently active project."""
    return db.get_instance(projects_mod.get_db_path(_active_project_id))


# ─── Job tracking ─────────────────────────────────────────────────────────────

_jobs: dict[str, dict] = {}
_email_jobs: dict[str, dict] = {}


# ─── Models ───────────────────────────────────────────────────────────────────

class ScrapeRequest(BaseModel):
    niche: str
    location: str = "Budapest, Hungary"
    max_leads: int = 100
    use_ai: bool = False
    include_social: bool = False
    include_maps: bool = False


class ManualLeadRequest(BaseModel):
    company_name: str = ""
    website: str = ""
    email: str = ""
    phone: str = ""
    niche: str = ""
    city: str = ""
    country: str = ""
    status: str = "new"
    notes: str = ""


class SequenceRequest(BaseModel):
    name: str
    trigger: str = "manual"
    steps: list = []
    active: bool = True


class EnrollRequest(BaseModel):
    sequence_id: int


class UpdateLeadRequest(BaseModel):
    status: str | None = None
    notes: str | None = None
    tags: list[str] | None = None
    callback_at: str | None = None
    contact_name: str | None = None
    contact_title: str | None = None
    company_name: str | None = None
    niche: str | None = None


class BulkStatusRequest(BaseModel):
    lead_ids: list[int]
    status: str


class WebhookRequest(BaseModel):
    url: str
    event: str = "status_changed"
    active: bool = True


class IMAPRequest(BaseModel):
    action: str  # "start" | "stop"


class ValidateEmailsRequest(BaseModel):
    emails: list[str]


class EmailCampaignRequest(BaseModel):
    lead_ids: list[int]
    subject: str
    body: str
    auto_contacted: bool = True
    validate_emails: bool = True


class ProjectCreateRequest(BaseModel):
    name: str
    color: str = ""


class ProjectUpdateRequest(BaseModel):
    name: str | None = None
    color: str | None = None


class EnvFileRequest(BaseModel):
    content: str
    project_id: str | None = None


# ─── Projects ─────────────────────────────────────────────────────────────────

@app.get("/api/projects")
def list_projects():
    projects = projects_mod.list_projects()
    # Annotate each with lead count
    result = []
    for p in projects:
        d = db.get_instance(projects_mod.get_db_path(p["id"])).stats()
        p["lead_count"] = d.get("total", 0)
        p["active"] = p["id"] == _active_project_id
        result.append(p)
    return result


@app.post("/api/projects")
def create_project(body: ProjectCreateRequest):
    global _active_project_id
    project = projects_mod.create_project(body.name.strip(), body.color)
    # Auto-activate the new project
    _active_project_id = projects_mod.set_active(project["id"])
    return {**project, "active": True, "lead_count": 0}


@app.put("/api/projects/{project_id}")
def update_project(project_id: str, body: ProjectUpdateRequest):
    p = projects_mod.update_project(project_id, name=body.name, color=body.color)
    if not p:
        raise HTTPException(404, "Project not found")
    return p


@app.delete("/api/projects/{project_id}")
def delete_project(project_id: str):
    global _active_project_id
    try:
        projects_mod.delete_project(project_id)
    except ValueError as e:
        raise HTTPException(400, str(e))
    if _active_project_id == project_id:
        _active_project_id = "default"
    return {"ok": True}


@app.post("/api/projects/{project_id}/activate")
def activate_project(project_id: str):
    global _active_project_id
    try:
        _active_project_id = projects_mod.set_active(project_id)
    except ValueError as e:
        raise HTTPException(404, str(e))
    return {"active": _active_project_id}


# ─── Stats + Leads ────────────────────────────────────────────────────────────

@app.get("/api/stats")
def get_stats():
    return _db().stats()


@app.get("/api/leads")
def get_leads(
    search: str = "",
    status: str = "",
    has_email: bool = False,
    has_phone: bool = False,
    tag: str = "",
    callback_overdue: bool = False,
    order_by: str = "updated_at DESC",
):
    return _db().fetch_all(
        search=search, status=status,
        has_email=has_email, has_phone=has_phone,
        tag=tag, callback_overdue=callback_overdue,
        order_by=order_by,
    )


@app.get("/api/leads/{lead_id}")
def get_lead(lead_id: int):
    lead = _db().fetch_by_id(lead_id)
    if not lead:
        raise HTTPException(404, "Lead not found")
    return lead


@app.get("/api/leads/{lead_id}/duplicates")
def find_lead_duplicates(lead_id: int):
    """Find the same lead (by website/email) across all other projects."""
    lead = _db().fetch_by_id(lead_id)
    if not lead:
        raise HTTPException(404, "Lead not found")

    domain = None
    if lead.get("website"):
        import re
        m = re.search(r"(?:https?://)?(?:www\.)?([^/\s]+)", lead["website"])
        domain = m.group(1).lower() if m else None
    emails = set(e.lower() for e in (lead.get("emails") or []))

    matches = []
    for p in projects_mod.list_projects():
        if p["id"] == _active_project_id:
            continue
        try:
            pdb = db.get_instance(projects_mod.get_db_path(p["id"]))
            for other in pdb.fetch_all():
                hit = False
                if domain and other.get("website"):
                    m2 = re.search(r"(?:https?://)?(?:www\.)?([^/\s]+)", other["website"])
                    if m2 and m2.group(1).lower() == domain:
                        hit = True
                if not hit and emails:
                    other_emails = set(e.lower() for e in (other.get("emails") or []))
                    if emails & other_emails:
                        hit = True
                if hit:
                    matches.append({
                        "project_id":   p["id"],
                        "project_name": p["name"],
                        "project_color": p.get("color", "#6366f1"),
                        "lead_id":      other["id"],
                        "company_name": other.get("company_name") or other.get("website", ""),
                        "status":       other.get("status", "new"),
                    })
        except Exception:
            pass
    return matches


@app.put("/api/leads/{lead_id}")
def update_lead(lead_id: int, body: UpdateLeadRequest):
    from scraper import webhooks as wh_mod
    d = _db()
    old_lead = d.fetch_by_id(lead_id)
    if body.status is not None:
        d.update_status(lead_id, body.status)
    if body.notes is not None:
        d.update_notes(lead_id, body.notes)
    if body.tags is not None:
        d.update_tags(lead_id, body.tags)
    if body.callback_at is not None:
        d.update_callback_at(lead_id, body.callback_at)
    if getattr(body, 'contact_name', None) is not None or getattr(body, 'contact_title', None) is not None:
        d.update_contact(lead_id,
                         getattr(body, 'contact_name', None) or (old_lead or {}).get('contact_name', ''),
                         getattr(body, 'contact_title', None) or (old_lead or {}).get('contact_title', ''))
    if body.company_name is not None:
        d.update_company_name(lead_id, body.company_name.strip())
    if body.niche is not None:
        d.update_niche(lead_id, body.niche.strip())
    lead = d.fetch_by_id(lead_id)
    # Fire webhook if status changed
    if body.status and old_lead and old_lead.get("status") != body.status:
        wh_mod.fire("status_changed", {"lead_id": lead_id, "old_status": old_lead.get("status"), "new_status": body.status, "company": (lead or {}).get("company_name", "")}, d)
        if body.status == "qualified":
            wh_mod.fire("lead_qualified", {"lead_id": lead_id, "company": (lead or {}).get("company_name", ""), "email": ((lead or {}).get("emails") or [""])[0]}, d)
    return lead


@app.post("/api/leads/bulk-status")
def bulk_status(body: BulkStatusRequest):
    count = _db().bulk_update_status(body.lead_ids, body.status)
    return {"ok": True, "updated": count}


@app.delete("/api/leads/all")
def delete_all_leads():
    count = _db().delete_all_leads()
    return {"ok": True, "deleted": count}


@app.delete("/api/leads/{lead_id}")
def delete_lead(lead_id: int):
    _db().delete(lead_id)
    return {"ok": True}


@app.delete("/api/leads")
def delete_leads(ids: list[int] = Query(...)):
    _db().delete_many(ids)
    return {"ok": True, "deleted": len(ids)}


@app.post("/api/leads")
def create_lead(body: ManualLeadRequest):
    from scraper.models import Lead
    emails = [e.strip() for e in body.email.split(",") if "@" in e] if body.email else []
    phones = [p.strip() for p in body.phone.split(",") if p.strip()] if body.phone else []
    lead = Lead(
        company_name=body.company_name.strip(),
        website=body.website.strip(),
        emails=emails,
        phones=phones,
        niche=body.niche.strip(),
        city=body.city.strip(),
        country=body.country.strip(),
        sources=["manual"],
        confidence=0.8,
    )
    if not lead.canonical_key():
        raise HTTPException(400, "website or company_name required")
    was_new, lead_id = _db().upsert(lead)
    if body.status and body.status != "new":
        _db().update_status(lead_id, body.status)
    if body.notes:
        _db().update_notes(lead_id, body.notes)
    return _db().fetch_by_id(lead_id)


# ─── Scrape ───────────────────────────────────────────────────────────────────

@app.post("/api/scrape")
def start_scrape(body: ScrapeRequest, background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())
    q: Queue = Queue()
    _jobs[job_id] = {"queue": q, "done": False, "counts": {
        "discovered": 0, "saved_new": 0, "merged": 0,
    }, "include_maps": body.include_maps}
    project_db = _db()
    cfg = settings_mod.load(_active_project_id)
    background_tasks.add_task(
        _run_scrape, job_id, body.niche, body.location, body.max_leads,
        project_db, body.use_ai, body.include_social, cfg,
    )
    return {"job_id": job_id}


def _run_scrape(job_id: str, niche: str, location: str, max_leads: int,
                project_db: db.Database, use_ai: bool = False,
                include_social: bool = False, cfg: dict | None = None):
    job = _jobs[job_id]
    q: Queue = job["queue"]

    def emit(type: str, **kwargs):
        q.put({"type": type, **kwargs})

    try:
        from scraper.scrapers.deep_search import DeepSearcher
        from scraper.extractors.contact import ContactExtractor
        from scraper.http.session import fetch, get_client

        client = get_client()
        extractor = ContactExtractor(default_region="HU")

        emit("log", level="info", msg=f"Starting deep search: '{niche}' in '{location}'")
        emit("log", level="dim",  msg=f"Database: {project_db.path}")

        def on_progress(msg: str):
            emit("log", level="info", msg=msg)

        # AI query boost
        extra_ai_queries: list[str] = []
        if use_ai and cfg and cfg.get("anthropic_api_key"):
            emit("log", level="info", msg="Using AI to generate smart queries…")
            try:
                from scraper.scrapers.ai_queries import generate_ai_queries
                extra_ai_queries = generate_ai_queries(niche, location, cfg["anthropic_api_key"])
                emit("log", level="success", msg=f"AI generated {len(extra_ai_queries)} additional queries")
            except Exception as e:
                emit("log", level="error", msg=f"AI query generation failed: {e}")

        searcher = DeepSearcher(
            niche=niche, location=location,
            max_leads=max_leads, on_progress=on_progress,
            extra_queries=extra_ai_queries,
        )
        stubs = searcher.run()
        job["counts"]["discovered"] = len(stubs)

        # Social media search
        if include_social:
            emit("log", level="info", msg="Searching social media platforms…")
            try:
                from scraper.scrapers.social_scraper import search_social
                social_leads = search_social(niche, location, on_progress=on_progress)
                stubs.extend(social_leads)
                job["counts"]["discovered"] = len(stubs)
                emit("log", level="success", msg=f"+{len(social_leads)} social profiles found")
            except Exception as e:
                emit("log", level="error", msg=f"Social search error: {e}")

        # Google Maps scraping
        include_maps = job.get("include_maps", False)
        if include_maps:
            emit("log", level="info", msg="Searching Google Maps…")
            try:
                from scraper.scrapers.maps_scraper import search_maps
                maps_api_key = cfg.get("google_maps_api_key", "") if cfg else ""
                maps_leads = search_maps(niche, location, api_key=maps_api_key,
                                         max_results=60, on_progress=on_progress)
                stubs.extend(maps_leads)
                job["counts"]["discovered"] = len(stubs)
                emit("log", level="success", msg=f"Google Maps: +{len(maps_leads)} places")
            except Exception as e:
                emit("log", level="error", msg=f"Google Maps error: {e}")

        emit("log", level="success", msg=f"Discovery done: {len(stubs)} URLs found")
        emit("log", level="info", msg="Enriching websites for emails & phones…")
        emit("progress", discovered=len(stubs), saved_new=0, merged=0)

        for i, stub in enumerate(stubs):
            if not _jobs.get(job_id, {}).get("running", True) is False:
                pass
            if not stub.website:
                continue

            key = stub.canonical_key()
            if project_db.exists(key):
                project_db.upsert(stub)
                job["counts"]["merged"] += 1
            else:
                enriched = extractor.enrich_lead(stub, lambda url: fetch(url, client=client))
                was_new, _ = project_db.upsert(enriched)
                if was_new:
                    job["counts"]["saved_new"] += 1
                    if enriched.has_contacts():
                        email = enriched.emails[0] if enriched.emails else ""
                        phone = enriched.phones[0] if enriched.phones else ""
                        emit("log", level="success",
                             msg=f"+ {enriched.company_name or enriched.website[:45]:45} {email}")
                        emit("lead", data={
                            "company_name": enriched.company_name,
                            "email": email, "phone": phone,
                        })
                    else:
                        emit("log", level="dim", msg=f"  {stub.website[:60]}")
                else:
                    job["counts"]["merged"] += 1

            if (i + 1) % 5 == 0:
                c = job["counts"]
                emit("progress", discovered=c["discovered"],
                     saved_new=c["saved_new"], merged=c["merged"])

        c = job["counts"]
        s = project_db.stats()
        emit("log", level="success", msg="═══ Scrape Complete ═══")
        emit("log", level="info",    msg=f"  New leads: {c['saved_new']}")
        emit("log", level="info",    msg=f"  Merged:    {c['merged']}")
        emit("log", level="info",    msg=f"  DB Total:  {s.get('total', 0)} leads")
        emit("progress", discovered=c["discovered"], saved_new=c["saved_new"], merged=c["merged"])
        emit("done", counts=c, stats=s)

    except Exception as e:
        emit("log", level="error", msg=f"Error: {e}")
        emit("done", counts=job["counts"], stats=project_db.stats())
    finally:
        job["done"] = True


@app.get("/api/scrape/{job_id}/stream")
async def scrape_stream(job_id: str):
    if job_id not in _jobs:
        raise HTTPException(404, "Job not found")

    async def generate() -> AsyncIterator[str]:
        job = _jobs[job_id]
        q: Queue = job["queue"]
        while True:
            try:
                msg = q.get_nowait()
                yield f"data: {json.dumps(msg)}\n\n"
                if msg.get("type") == "done":
                    break
            except Empty:
                if job["done"] and q.empty():
                    break
                yield "data: {\"type\":\"ping\"}\n\n"
                await asyncio.sleep(0.15)

    return StreamingResponse(generate(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/api/scrape/{job_id}/stop")
def stop_scrape(job_id: str):
    if job_id in _jobs:
        _jobs[job_id]["running"] = False
    return {"ok": True}


# ─── Settings ─────────────────────────────────────────────────────────────────

@app.get("/api/settings")
def get_settings(project_id: str | None = None):
    pid = project_id or _active_project_id
    return settings_mod.get_for_ui(pid)


@app.put("/api/settings")
def update_settings(body: dict, project_id: str | None = None):
    pid = project_id or _active_project_id
    scope = body.pop("_scope", "global")  # "global" or "project"
    if scope == "project" and pid and pid != "default":
        return settings_mod.save(body, project_id=pid)
    return settings_mod.save(body, project_id=None)


@app.post("/api/settings/test-smtp")
def test_smtp(scope: str = "global"):
    from scraper.email.smtp import test_connection
    pid = _active_project_id if scope == "project" else None
    cfg = settings_mod.load(pid)
    return test_connection(cfg)


# ─── .env file editing ────────────────────────────────────────────────────────

@app.get("/api/env")
def get_env(project_id: str | None = None):
    pid = project_id if project_id is not None else None
    return {
        "global":  settings_mod.read_env_file(None),
        "project": settings_mod.read_env_file(pid) if pid and pid != "default" else "",
        "project_id": pid or "default",
    }


@app.put("/api/env")
def save_env(body: EnvFileRequest):
    pid = body.project_id if body.project_id and body.project_id != "default" else None
    settings_mod.write_env_file(body.content, project_id=pid)
    return {"ok": True}


# ─── Email campaign ───────────────────────────────────────────────────────────

@app.post("/api/email/send")
def start_email_campaign(body: EmailCampaignRequest, background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())
    q: Queue = Queue()
    _email_jobs[job_id] = {"queue": q, "done": False, "stop": [False],
                           "counts": {"sent": 0, "failed": 0, "skipped": 0}}
    project_db = _db()
    background_tasks.add_task(
        _run_email_campaign, job_id, body.lead_ids, body.subject,
        body.body, body.auto_contacted, project_db, body.validate_emails,
    )
    return {"job_id": job_id}


def _run_email_campaign(job_id: str, lead_ids: list[int], subject: str,
                        body: str, auto_contacted: bool, project_db: db.Database,
                        validate_emails: bool = True):
    job = _email_jobs[job_id]
    q: Queue = job["queue"]
    stop_flag: list[bool] = job["stop"]

    def emit(**kwargs):
        q.put(kwargs)

    try:
        from scraper.email.smtp import send_campaign

        leads = [project_db.fetch_by_id(lid) for lid in lead_ids]
        leads = [l for l in leads if l]

        # MX validation pass — skip leads with invalid email domains
        if validate_emails:
            try:
                from scraper.email.validator import validate_emails_bulk
                all_emails = [e for l in leads for e in (l.get("emails") or [])]
                if all_emails:
                    emit(type="log", level="info", msg=f"Validating {len(all_emails)} email addresses…")
                    results = {r["email"]: r["valid"] for r in validate_emails_bulk(all_emails)}
                    before = len(leads)
                    leads = [l for l in leads if any(results.get(e, True) for e in (l.get("emails") or []))]
                    skipped_mx = before - len(leads)
                    if skipped_mx:
                        job["counts"]["skipped"] += skipped_mx
                        emit(type="log", level="warn", msg=f"Skipped {skipped_mx} leads with invalid email domains")
            except Exception as _ve:
                emit(type="log", level="warn", msg=f"Email validation unavailable: {_ve}")

        emit(type="log", level="info",
             msg=f"Starting campaign: {len(leads)} leads, subject: '{subject[:60]}'")

        # Pre-generate tracking tokens per lead
        base_url = os.environ.get("BASE_URL", "http://localhost:7337")
        for _lead in leads:
            _tok = uuid.uuid4().hex
            project_db.create_open_token(_lead["id"], _tok)
            _lead["_pixel_token"] = _tok
        body_with_pixel = body + '\n<img src="' + base_url + '/api/t/{{_pixel_token}}.gif" width="1" height="1" style="display:none">'

        # Generate unsubscribe tokens per lead
        base_url_unsub = settings_mod.load(_active_project_id).get("base_url", "http://localhost:7337").rstrip("/")
        for _lead in leads:
            tok = project_db.get_or_create_unsub_token(_lead["id"])
            _lead["_unsub_token"] = tok
            _lead["_unsub_url"] = f"{base_url_unsub}/unsubscribe?token={tok}"

        def on_progress(ev: dict):
            t = ev.get("type", "")
            if t == "sent":
                job["counts"]["sent"] += 1
                emit(type="log", level="success",
                     msg=f"✓ [{ev['index']}/{ev['total']}] {ev.get('company','')[:40]} → {ev.get('to','')}")
                emit(type="progress", **job["counts"], total=len(leads))
                project_db.log_email(ev["lead_id"], subject, ev.get("to", ""), "sent")
                if auto_contacted:
                    project_db.update_status(ev["lead_id"], "contacted")
                project_db.update_last_emailed(ev["lead_id"])
            elif t == "failed":
                job["counts"]["failed"] += 1
                emit(type="log", level="error",
                     msg=f"✗ [{ev['index']}/{ev['total']}] {ev.get('company','')[:40]} — {ev.get('error','')}")
                emit(type="progress", **job["counts"], total=len(leads))
                project_db.log_email(ev["lead_id"], subject, ev.get("to", ""), "failed", ev.get("error", ""))
            elif t == "skipped":
                job["counts"]["skipped"] += 1
                emit(type="log", level="dim",
                     msg=f"  [{ev['index']}/{ev['total']}] {ev.get('company','')[:40]} — skipped ({ev.get('reason','')})")
            elif t == "delay":
                emit(type="log", level="dim",
                     msg=f"  Waiting {ev['seconds']}s before next email…")

        send_campaign(leads, subject, body_with_pixel, stop_flag=stop_flag, on_progress=on_progress)

        c = job["counts"]
        emit(type="log", level="success", msg="═══ Campaign Complete ═══")
        emit(type="log", level="info",    msg=f"  Sent:    {c['sent']}")
        emit(type="log", level="info",    msg=f"  Failed:  {c['failed']}")
        emit(type="log", level="info",    msg=f"  Skipped: {c['skipped']}")
        emit(type="done", counts=c)

    except Exception as e:
        emit(type="log", level="error", msg=f"Error: {e}")
        emit(type="done", counts=job["counts"])
    finally:
        job["done"] = True


@app.get("/api/email/jobs/{job_id}/stream")
async def email_stream(job_id: str):
    if job_id not in _email_jobs:
        raise HTTPException(404, "Job not found")

    async def generate() -> AsyncIterator[str]:
        job = _email_jobs[job_id]
        q: Queue = job["queue"]
        while True:
            try:
                msg = q.get_nowait()
                yield f"data: {json.dumps(msg)}\n\n"
                if msg.get("type") == "done":
                    break
            except Empty:
                if job["done"] and q.empty():
                    break
                yield "data: {\"type\":\"ping\"}\n\n"
                await asyncio.sleep(0.15)

    return StreamingResponse(generate(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/api/email/jobs/{job_id}/stop")
def stop_email_job(job_id: str):
    if job_id in _email_jobs:
        _email_jobs[job_id]["stop"][0] = True
    return {"ok": True}


@app.get("/api/email/logs")
def get_email_logs(lead_id: int | None = None):
    return _db().fetch_email_logs(lead_id=lead_id)


# ─── Unsubscribe ──────────────────────────────────────────────────────────────

@app.get("/unsubscribe")
def unsubscribe(token: str = ""):
    if not token:
        return JSONResponse({"error": "Invalid token"}, status_code=400)
    lead = _db().unsubscribe_by_token(token)
    if not lead:
        return JSONResponse({"error": "Token not found"}, status_code=404)
    html = """<!DOCTYPE html><html><body style="font-family:sans-serif;max-width:480px;margin:60px auto;text-align:center">
<h2 style="color:#22c55e">Unsubscribed</h2>
<p>You have been removed from our mailing list.</p>
<p style="color:#64748b;font-size:13px">Company: {company}</p>
</body></html>""".format(company=lead.get("company_name", ""))
    from fastapi.responses import HTMLResponse
    return HTMLResponse(html)


# ─── Webhooks ─────────────────────────────────────────────────────────────────

@app.get("/api/webhooks")
def get_webhooks():
    return _db().fetch_webhooks()


@app.post("/api/webhooks")
def create_webhook(body: WebhookRequest):
    if not body.url.startswith("http"):
        raise HTTPException(400, "URL must start with http(s)://")
    return _db().save_webhook(body.url, body.event, body.active)


@app.put("/api/webhooks/{webhook_id}")
def update_webhook(webhook_id: int, body: WebhookRequest):
    return _db().save_webhook(body.url, body.event, body.active, webhook_id)


@app.delete("/api/webhooks/{webhook_id}")
def delete_webhook(webhook_id: int):
    _db().delete_webhook(webhook_id)
    return {"ok": True}


# ─── IMAP poller ──────────────────────────────────────────────────────────────

@app.get("/api/imap/status")
def imap_status():
    from scraper.email.imap_poller import get_status
    return get_status()


@app.post("/api/imap/start")
def imap_start():
    from scraper.email.imap_poller import start_poller
    cfg = settings_mod.load(_active_project_id)
    ok = start_poller(cfg, _db())
    return {"ok": ok, "message": "Started" if ok else "Already running"}


@app.post("/api/imap/stop")
def imap_stop():
    from scraper.email.imap_poller import stop_poller
    stop_poller()
    return {"ok": True}


# ─── Email validation ─────────────────────────────────────────────────────────

@app.post("/api/email/validate")
def validate_emails(body: ValidateEmailsRequest):
    try:
        from scraper.email.validator import validate_emails_bulk
        results = validate_emails_bulk(body.emails)
        valid   = [r for r in results if r["valid"]]
        invalid = [r for r in results if not r["valid"]]
        return {"results": results, "valid_count": len(valid), "invalid_count": len(invalid)}
    except ImportError:
        raise HTTPException(500, "dnspython not installed: pip install dnspython")


# ─── Email open tracking ──────────────────────────────────────────────────────

@app.get("/api/t/{token}.gif")
def track_open(token: str):
    _db().record_open(token)
    # 1x1 transparent GIF
    gif = bytes([
        0x47,0x49,0x46,0x38,0x39,0x61,0x01,0x00,0x01,0x00,0x80,0x00,0x00,
        0xff,0xff,0xff,0x00,0x00,0x00,0x21,0xf9,0x04,0x00,0x00,0x00,0x00,0x00,
        0x2c,0x00,0x00,0x00,0x00,0x01,0x00,0x01,0x00,0x00,0x02,0x02,0x44,0x01,0x00,0x3b
    ])
    from fastapi.responses import Response
    return Response(content=gif, media_type="image/gif",
                    headers={"Cache-Control": "no-store, no-cache"})


@app.get("/api/dashboard")
def get_dashboard():
    return _db().dashboard_stats()


# ─── Activity log ─────────────────────────────────────────────────────────────

class ActivityRequest(BaseModel):
    activity_type: str          # "call" | "email" | "note"
    outcome: str = ""           # "answered" | "no_answer" | "voicemail" | "callback" | "not_interested" | "interested" | "sent" | "failed"
    subject: str = ""
    notes: str = ""
    update_status: str = ""     # optionally update lead status


@app.post("/api/leads/{lead_id}/activity")
def log_activity(lead_id: int, body: ActivityRequest):
    d = _db()
    lead = d.fetch_by_id(lead_id)
    if not lead:
        raise HTTPException(404, "Lead not found")
    activity_id = d.log_activity(lead_id, body.activity_type, body.outcome, body.subject, body.notes)
    if body.activity_type == "call":
        d.update_last_called(lead_id)
    elif body.activity_type == "email":
        d.update_last_emailed(lead_id)
    if body.update_status:
        d.update_status(lead_id, body.update_status)
    return {"id": activity_id, "ok": True}


@app.get("/api/leads/{lead_id}/activity")
def get_lead_activity(lead_id: int):
    return _db().fetch_activities(lead_id=lead_id)


@app.get("/api/activity")
def get_all_activity(activity_type: str = "", limit: int = 200):
    return _db().fetch_activities(activity_type=activity_type or None, limit=limit)


# ─── Export ───────────────────────────────────────────────────────────────────

@app.get("/api/export/csv")
def export_csv(search: str = "", status: str = "",
               has_email: bool = False, has_phone: bool = False):
    from scraper.export.csv_exporter import export_csv as _export_csv
    from datetime import datetime
    import tempfile
    rows = _db().fetch_all(search=search, status=status,
                           has_email=has_email, has_phone=has_phone)
    leads = [_row_to_lead(r) for r in rows]
    tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
    _export_csv(leads, tmp.name)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return FileResponse(tmp.name, media_type="text/csv", filename=f"leads_{ts}.csv")


@app.get("/api/export/excel")
def export_excel(search: str = "", status: str = "",
                 has_email: bool = False, has_phone: bool = False):
    from scraper.export.excel_exporter import export_excel as _export_excel
    from datetime import datetime
    import tempfile
    rows = _db().fetch_all(search=search, status=status,
                           has_email=has_email, has_phone=has_phone)
    leads = [_row_to_lead(r) for r in rows]
    tmp = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
    _export_excel(leads, tmp.name)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return FileResponse(
        tmp.name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=f"leads_{ts}.xlsx",
    )


@app.get("/api/export/json")
def export_json(search: str = "", status: str = "",
                has_email: bool = False, has_phone: bool = False):
    from datetime import datetime
    rows = _db().fetch_all(search=search, status=status,
                           has_email=has_email, has_phone=has_phone)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return JSONResponse(
        content=rows,
        headers={"Content-Disposition": f"attachment; filename=leads_{ts}.json"},
    )


def _row_to_lead(r: dict):
    from scraper.models import Lead
    return Lead(
        company_name=r["company_name"], website=r["website"],
        emails=r["emails"], phones=r["phones"], address=r["address"],
        city=r["city"], country=r["country"], niche=r["niche"],
        sources=r["sources"], confidence=r["confidence"],
    )


# ─── Import ───────────────────────────────────────────────────────────────────

def _import_rows(rows: list[dict]) -> dict:
    added = merged = skipped = 0
    from scraper.models import Lead
    d = _db()
    for row in rows:
        website = (row.get("website") or row.get("Website") or "").strip()
        if not website:
            skipped += 1
            continue

        def _get(*keys):
            for k in keys:
                v = row.get(k)
                if v is not None and str(v).strip():
                    return str(v).strip()
            return ""

        emails_raw = _get("emails", "Emails", "email", "Email")
        phones_raw = _get("phones", "Phones", "phone", "Phone")
        emails = [e.strip() for e in re.split(r"[;,\s]+", emails_raw) if "@" in e] if emails_raw else []
        phones = [p.strip() for p in re.split(r"[;,]+", phones_raw) if p.strip()] if phones_raw else []

        lead = Lead(
            company_name=_get("company_name", "Company Name", "company", "Company"),
            website=website, emails=emails, phones=phones,
            address=_get("address", "Address"),
            city=_get("city", "City"),
            country=_get("country", "Country"),
            niche=_get("niche", "Niche"),
            sources=["import"],
            confidence=float(row.get("confidence", row.get("Confidence", 0.5)) or 0.5),
        )
        was_new, _ = d.upsert(lead)
        if was_new:
            added += 1
        else:
            merged += 1
    return {"added": added, "merged": merged, "skipped": skipped}


# ─── Sequences ───────────────────────────────────────────────────────────────

SEQUENCE_TRIGGERS = [
    {"value": "manual",              "label": "Manual enrollment only"},
    {"value": "call:interested",     "label": "After call: Interested"},
    {"value": "call:no_answer",      "label": "After call: No Answer"},
    {"value": "call:voicemail",      "label": "After call: Voicemail"},
    {"value": "call:callback",       "label": "After call: Callback requested"},
    {"value": "call:not_interested", "label": "After call: Not Interested"},
    {"value": "email:sent",          "label": "After email is sent"},
]


@app.get("/api/sequences/triggers")
def list_triggers():
    return SEQUENCE_TRIGGERS


@app.get("/api/sequences")
def list_sequences():
    return _db().fetch_sequences()


@app.post("/api/sequences")
def create_sequence(body: SequenceRequest):
    return _db().save_sequence(body.name, body.trigger, body.steps, body.active)


@app.put("/api/sequences/{seq_id}")
def update_sequence(seq_id: int, body: SequenceRequest):
    return _db().save_sequence(body.name, body.trigger, body.steps, body.active, seq_id)


@app.delete("/api/sequences/{seq_id}")
def delete_sequence(seq_id: int):
    _db().delete_sequence(seq_id)
    return {"ok": True}


@app.post("/api/leads/{lead_id}/enroll")
def enroll_lead(lead_id: int, body: EnrollRequest):
    lead = _db().fetch_by_id(lead_id)
    if not lead:
        raise HTTPException(404, "Lead not found")
    result = _db().enroll_lead(lead_id, body.sequence_id)
    if result is None:
        raise HTTPException(409, "Lead already enrolled in this sequence")
    return result


@app.get("/api/leads/{lead_id}/enrollments")
def get_lead_enrollments(lead_id: int):
    return _db().fetch_enrollments(lead_id=lead_id)


@app.delete("/api/enrollments/{enrollment_id}")
def cancel_enrollment(enrollment_id: int):
    _db().cancel_enrollment(enrollment_id)
    return {"ok": True}


@app.post("/api/sequences/process")
def process_due_sequences(background_tasks: BackgroundTasks):
    """Process all due sequence steps — send emails to enrolled leads."""
    job_id = str(uuid.uuid4())
    q: Queue = Queue()
    _email_jobs[job_id] = {"queue": q, "done": False, "stop": [False],
                           "counts": {"sent": 0, "failed": 0, "skipped": 0}}
    project_db = _db()
    background_tasks.add_task(_run_sequence_processing, job_id, project_db)
    return {"job_id": job_id}


def _run_sequence_processing(job_id: str, project_db: db.Database):
    job = _email_jobs[job_id]
    q: Queue = job["queue"]
    stop_flag: list[bool] = job["stop"]

    def emit(**kwargs):
        q.put(kwargs)

    try:
        from scraper.email.smtp import send_one

        due = project_db.get_due_enrollments()
        emit(type="log", level="info", msg=f"Processing {len(due)} due sequence steps…")

        for item in due:
            if stop_flag[0]:
                break
            steps = item["steps_list"]
            step_idx = item["step_index"]
            if step_idx >= len(steps):
                project_db.advance_enrollment(item["id"])
                continue

            step = steps[step_idx]
            emails = item["emails_list"]
            if not emails:
                emit(type="log", level="dim",
                     msg=f"  Skip {item.get('company_name','?')} — no email")
                job["counts"]["skipped"] += 1
                project_db.advance_enrollment(item["id"])
                continue

            to_email = emails[0]
            subject = step.get("subject", "Follow-up")
            body_text = step.get("body", "")
            lead = project_db.fetch_by_id(item["lead_id"])

            try:
                cfg = settings_mod.load(_active_project_id)
                send_one(cfg, to_email, subject, body_text, lead=lead)
                project_db.log_email(item["lead_id"], subject, to_email, "sent")
                project_db.update_last_emailed(item["lead_id"])
                project_db.log_activity(item["lead_id"], "email", "sent", subject,
                                        f"Sequence step {step_idx+1}: {item.get('seq_name','?')}")
                project_db.advance_enrollment(item["id"])
                job["counts"]["sent"] += 1
                emit(type="log", level="success",
                     msg=f"✓ {item.get('company_name','?')[:40]} → {to_email} (step {step_idx+1})")
            except Exception as e:
                job["counts"]["failed"] += 1
                emit(type="log", level="error",
                     msg=f"✗ {item.get('company_name','?')[:40]} — {e}")

        c = job["counts"]
        emit(type="log", level="success", msg=f"Done — {c['sent']} sent, {c['failed']} failed, {c['skipped']} skipped")
        emit(type="done", counts=c)
    except Exception as e:
        emit(type="log", level="error", msg=f"Error: {e}")
        emit(type="done", counts=job["counts"])
    finally:
        job["done"] = True


# ─── AI Queries ───────────────────────────────────────────────────────────────

class AIQueryRequest(BaseModel):
    niche: str
    location: str = "Budapest, Hungary"


@app.post("/api/ai/queries")
def ai_queries(body: AIQueryRequest):
    cfg = settings_mod.load(_active_project_id)
    api_key = cfg.get("anthropic_api_key", "")
    if not api_key:
        raise HTTPException(400, "ANTHROPIC_API_KEY not configured. Add it in Settings → AI.")
    try:
        from scraper.scrapers.ai_queries import generate_ai_queries
        queries = generate_ai_queries(body.niche, body.location, api_key)
        return {"queries": queries}
    except Exception as e:
        raise HTTPException(500, str(e))


# ─── Scripts ─────────────────────────────────────────────────────────────────

class ScriptRequest(BaseModel):
    name: str
    subject: str = ""
    body: str = ""


@app.get("/api/scripts")
def list_scripts():
    return _db().fetch_scripts()


@app.post("/api/scripts")
def create_script(body: ScriptRequest):
    return _db().save_script(body.name, body.subject, body.body)


@app.put("/api/scripts/{script_id}")
def update_script(script_id: int, body: ScriptRequest):
    return _db().save_script(body.name, body.subject, body.body, script_id)


@app.delete("/api/scripts/{script_id}")
def delete_script(script_id: int):
    _db().delete_script(script_id)
    return {"ok": True}


@app.post("/api/import/csv")
async def import_csv(file: UploadFile = File(...)):
    import csv as _csv
    content = await file.read()
    text = content.decode("utf-8-sig", errors="replace")
    rows = list(_csv.DictReader(io.StringIO(text)))
    return _import_rows(rows)


@app.post("/api/import/excel")
async def import_excel(file: UploadFile = File(...)):
    try:
        import openpyxl
    except ImportError:
        raise HTTPException(500, "openpyxl not installed")
    content = await file.read()
    wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    ws = wb.active
    rows_iter = ws.iter_rows(values_only=True)
    headers = [str(h).strip() if h is not None else "" for h in next(rows_iter, [])]
    rows = [{headers[i]: (str(v).strip() if v is not None else "")
             for i, v in enumerate(row) if i < len(headers)} for row in rows_iter]
    return _import_rows(rows)


@app.post("/api/import/json")
async def import_json_file(file: UploadFile = File(...)):
    content = await file.read()
    rows = json.loads(content)
    if not isinstance(rows, list):
        raise HTTPException(400, "JSON must be an array of lead objects")
    return _import_rows(rows)


# ─── Static files + SPA fallback ─────────────────────────────────────────────

# ─── Update endpoints ────────────────────────────────────────────────────────

REPO_DIR = Path(__file__).parent.parent.parent  # project root


def _git(*args: str, cwd: Path = REPO_DIR) -> str:
    import subprocess
    result = subprocess.run(
        ["git", *args], cwd=str(cwd),
        capture_output=True, text=True, timeout=30
    )
    return result.stdout.strip()


@app.get("/api/update/check")
def update_check():
    """Compare local HEAD with origin/main."""
    try:
        _git("fetch", "origin", "main", "--quiet")
        local  = _git("rev-parse", "HEAD")
        remote = _git("rev-parse", "origin/main")
        if local == remote:
            return {"up_to_date": True, "commits_behind": 0, "changelog": []}
        behind = int(_git("rev-list", "--count", f"HEAD..origin/main") or "0")
        log = _git("log", "--oneline", "HEAD..origin/main")
        changelog = [ln.strip() for ln in log.splitlines() if ln.strip()]
        current_ver = _git("describe", "--tags", "--abbrev=0") or ""
        return {
            "up_to_date": False,
            "commits_behind": behind,
            "changelog": changelog,
            "current_version": current_ver,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/update/run")
async def update_run():
    """Pull latest code and reinstall deps if needed. Streams log lines as SSE."""
    import subprocess

    async def generate() -> AsyncIterator[str]:
        def send(msg: str) -> str:
            return f"data: {json.dumps({'line': msg})}\n\n"

        yield send("Pulling latest code from GitHub…")

        # git pull
        proc = await asyncio.create_subprocess_exec(
            "git", "pull", "origin", "main",
            cwd=str(REPO_DIR),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        async for raw in proc.stdout:
            line = raw.decode().rstrip()
            if line:
                yield send(line)
        await proc.wait()

        if proc.returncode != 0:
            yield send("ERROR: git pull failed.")
            yield f"data: {json.dumps({'done': True, 'success': False})}\n\n"
            return

        # Check if deps changed
        changed = _git("diff", "HEAD@{1}", "HEAD", "--name-only")
        needs_install = any(f in changed for f in ("requirements.txt", "pyproject.toml"))

        if needs_install:
            yield send("Dependencies changed — reinstalling…")
            pip = str(REPO_DIR / ".venv" / "bin" / "pip")
            import shutil
            pip_cmd = pip if Path(pip).exists() else shutil.which("pip3") or "pip"
            proc2 = await asyncio.create_subprocess_exec(
                pip_cmd, "install", "-r", "requirements.txt", "-q",
                cwd=str(REPO_DIR),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            async for raw in proc2.stdout:
                line = raw.decode().rstrip()
                if line:
                    yield send(line)
            await proc2.wait()
            yield send("Dependencies updated.")
        else:
            yield send("No dependency changes.")

        yield send("Done! Restart the server to apply all changes.")
        yield f"data: {json.dumps({'done': True, 'success': True})}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/static/{file_path:path}")
def serve_static(file_path: str):
    full = STATIC_DIR / file_path
    if not full.exists():
        raise HTTPException(404)
    return FileResponse(str(full), headers={"Cache-Control": "no-cache, no-store, must-revalidate"})


@app.get("/{full_path:path}")
def spa_fallback(full_path: str):
    html_path = STATIC_DIR / "index.html"
    html = html_path.read_text()
    # Inject cache-busting version based on file mtimes
    import hashlib, os as _os
    def _mtime(name):
        p = STATIC_DIR / name
        return str(int(_os.path.getmtime(p))) if p.exists() else "0"
    v_js  = _mtime("app.js")
    v_css = _mtime("style.css")
    html = html.replace('href="/static/style.css"', f'href="/static/style.css?v={v_css}"')
    html = html.replace('src="/static/app.js"',    f'src="/static/app.js?v={v_js}"')
    from fastapi.responses import HTMLResponse
    return HTMLResponse(html, headers={"Cache-Control": "no-cache"})


# ─── Run ──────────────────────────────────────────────────────────────────────

def run_server(host: str = "127.0.0.1", port: int = 7337, open_browser: bool = True):
    import warnings
    import urllib3
    warnings.filterwarnings("ignore")
    urllib3.disable_warnings()

    if open_browser:
        import time, webbrowser
        def _open():
            time.sleep(1.2)
            webbrowser.open(f"http://{host}:{port}")
        threading.Thread(target=_open, daemon=True).start()

    print(f"\n  Lead Manager Web UI")
    print(f"  → http://{host}:{port}\n")
    import uvicorn
    uvicorn.run(app, host=host, port=port, log_level="warning")
