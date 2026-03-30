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

import uvicorn
from fastapi import BackgroundTasks, FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
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


class UpdateLeadRequest(BaseModel):
    status: str | None = None
    notes: str | None = None


class EmailCampaignRequest(BaseModel):
    lead_ids: list[int]
    subject: str
    body: str
    auto_contacted: bool = True


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
    order_by: str = "updated_at DESC",
):
    return _db().fetch_all(
        search=search, status=status,
        has_email=has_email, has_phone=has_phone,
        order_by=order_by,
    )


@app.get("/api/leads/{lead_id}")
def get_lead(lead_id: int):
    lead = _db().fetch_by_id(lead_id)
    if not lead:
        raise HTTPException(404, "Lead not found")
    return lead


@app.put("/api/leads/{lead_id}")
def update_lead(lead_id: int, body: UpdateLeadRequest):
    d = _db()
    if body.status is not None:
        d.update_status(lead_id, body.status)
    if body.notes is not None:
        d.update_notes(lead_id, body.notes)
    return d.fetch_by_id(lead_id)


@app.delete("/api/leads/{lead_id}")
def delete_lead(lead_id: int):
    _db().delete(lead_id)
    return {"ok": True}


@app.delete("/api/leads")
def delete_leads(ids: list[int] = Query(...)):
    _db().delete_many(ids)
    return {"ok": True, "deleted": len(ids)}


# ─── Scrape ───────────────────────────────────────────────────────────────────

@app.post("/api/scrape")
def start_scrape(body: ScrapeRequest, background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())
    q: Queue = Queue()
    _jobs[job_id] = {"queue": q, "done": False, "counts": {
        "discovered": 0, "saved_new": 0, "merged": 0,
    }}
    # Capture current project DB at job creation time
    project_db = _db()
    background_tasks.add_task(_run_scrape, job_id, body.niche, body.location, body.max_leads, project_db)
    return {"job_id": job_id}


def _run_scrape(job_id: str, niche: str, location: str, max_leads: int, project_db: db.Database):
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

        searcher = DeepSearcher(
            niche=niche, location=location,
            max_leads=max_leads, on_progress=on_progress,
        )
        stubs = searcher.run()
        job["counts"]["discovered"] = len(stubs)

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
def test_smtp():
    from scraper.email.smtp import test_connection
    return test_connection()


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
        body.body, body.auto_contacted, project_db,
    )
    return {"job_id": job_id}


def _run_email_campaign(job_id: str, lead_ids: list[int], subject: str,
                        body: str, auto_contacted: bool, project_db: db.Database):
    job = _email_jobs[job_id]
    q: Queue = job["queue"]
    stop_flag: list[bool] = job["stop"]

    def emit(**kwargs):
        q.put(kwargs)

    try:
        from scraper.email.smtp import send_campaign

        leads = [project_db.fetch_by_id(lid) for lid in lead_ids]
        leads = [l for l in leads if l]

        emit(type="log", level="info",
             msg=f"Starting campaign: {len(leads)} leads, subject: '{subject[:60]}'")

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

        send_campaign(leads, subject, body, stop_flag=stop_flag, on_progress=on_progress)

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

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/{full_path:path}")
def spa_fallback(full_path: str):
    return FileResponse(str(STATIC_DIR / "index.html"))


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
    uvicorn.run(app, host=host, port=port, log_level="warning")
