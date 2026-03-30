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

STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(title="Lead Manager", docs_url=None, redoc_url=None)

# ─── Active scrape jobs ────────────────────────────────────────────────────

_jobs: dict[str, dict] = {}  # job_id -> {queue, done, counts}


# ─── Models ───────────────────────────────────────────────────────────────

class ScrapeRequest(BaseModel):
    niche: str
    location: str = "Budapest, Hungary"
    max_leads: int = 100


class UpdateLeadRequest(BaseModel):
    status: str | None = None
    notes: str | None = None


# ─── API Routes ───────────────────────────────────────────────────────────

@app.get("/api/stats")
def get_stats():
    return db.stats()


@app.get("/api/leads")
def get_leads(
    search: str = "",
    status: str = "",
    has_email: bool = False,
    has_phone: bool = False,
    order_by: str = "updated_at DESC",
):
    return db.fetch_all(
        search=search,
        status=status,
        has_email=has_email,
        has_phone=has_phone,
        order_by=order_by,
    )


@app.get("/api/leads/{lead_id}")
def get_lead(lead_id: int):
    lead = db.fetch_by_id(lead_id)
    if not lead:
        raise HTTPException(404, "Lead not found")
    return lead


@app.put("/api/leads/{lead_id}")
def update_lead(lead_id: int, body: UpdateLeadRequest):
    if body.status is not None:
        db.update_status(lead_id, body.status)
    if body.notes is not None:
        db.update_notes(lead_id, body.notes)
    return db.fetch_by_id(lead_id)


@app.delete("/api/leads/{lead_id}")
def delete_lead(lead_id: int):
    db.delete(lead_id)
    return {"ok": True}


@app.delete("/api/leads")
def delete_leads(ids: list[int] = Query(...)):
    db.delete_many(ids)
    return {"ok": True, "deleted": len(ids)}


# ─── Scrape ───────────────────────────────────────────────────────────────

@app.post("/api/scrape")
def start_scrape(body: ScrapeRequest, background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())
    q: Queue = Queue()
    _jobs[job_id] = {"queue": q, "done": False, "counts": {
        "discovered": 0, "saved_new": 0, "merged": 0,
    }}
    background_tasks.add_task(_run_scrape, job_id, body.niche, body.location, body.max_leads)
    return {"job_id": job_id}


def _run_scrape(job_id: str, niche: str, location: str, max_leads: int):
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
        emit("log", level="dim", msg=f"Database: {db.DB_PATH}")

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
                pass  # continue unless explicitly stopped
            if not stub.website:
                continue

            key = stub.canonical_key()
            if db.exists(key):
                db.upsert(stub)
                job["counts"]["merged"] += 1
            else:
                enriched = extractor.enrich_lead(stub, lambda url: fetch(url, client=client))
                was_new, _ = db.upsert(enriched)
                if was_new:
                    job["counts"]["saved_new"] += 1
                    if enriched.has_contacts():
                        email = enriched.emails[0] if enriched.emails else ""
                        phone = enriched.phones[0] if enriched.phones else ""
                        emit("log", level="success",
                             msg=f"+ {enriched.company_name or enriched.website[:45]:45} {email}")
                        emit("lead", data={
                            "company_name": enriched.company_name,
                            "email": email,
                            "phone": phone,
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
        s = db.stats()
        emit("log", level="success", msg="═══ Scrape Complete ═══")
        emit("log", level="info",    msg=f"  New leads: {c['saved_new']}")
        emit("log", level="info",    msg=f"  Merged:    {c['merged']}")
        emit("log", level="info",    msg=f"  DB Total:  {s.get('total', 0)} leads")
        emit("progress", discovered=c["discovered"], saved_new=c["saved_new"],
             merged=c["merged"])
        emit("done", counts=c, stats=s)

    except Exception as e:
        emit("log", level="error", msg=f"Error: {e}")
        emit("done", counts=job["counts"], stats=db.stats())
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

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/scrape/{job_id}/stop")
def stop_scrape(job_id: str):
    if job_id in _jobs:
        _jobs[job_id]["running"] = False
    return {"ok": True}


# ─── Export ───────────────────────────────────────────────────────────────

@app.get("/api/export/csv")
def export_csv(
    search: str = "",
    status: str = "",
    has_email: bool = False,
    has_phone: bool = False,
):
    from scraper.models import Lead
    from scraper.export.csv_exporter import export_csv as _export_csv
    from datetime import datetime
    import tempfile

    rows = db.fetch_all(search=search, status=status,
                        has_email=has_email, has_phone=has_phone)
    leads = [_row_to_lead(r) for r in rows]

    tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
    _export_csv(leads, tmp.name)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return FileResponse(
        tmp.name,
        media_type="text/csv",
        filename=f"leads_{ts}.csv",
    )


@app.get("/api/export/excel")
def export_excel(
    search: str = "",
    status: str = "",
    has_email: bool = False,
    has_phone: bool = False,
):
    from scraper.export.excel_exporter import export_excel as _export_excel
    from datetime import datetime
    import tempfile

    rows = db.fetch_all(search=search, status=status,
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


def _row_to_lead(r: dict):
    from scraper.models import Lead
    return Lead(
        company_name=r["company_name"], website=r["website"],
        emails=r["emails"], phones=r["phones"], address=r["address"],
        city=r["city"], country=r["country"], niche=r["niche"],
        sources=r["sources"], confidence=r["confidence"],
    )


@app.get("/api/export/json")
def export_json(
    search: str = "",
    status: str = "",
    has_email: bool = False,
    has_phone: bool = False,
):
    from datetime import datetime
    rows = db.fetch_all(search=search, status=status,
                        has_email=has_email, has_phone=has_phone)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return JSONResponse(
        content=rows,
        headers={"Content-Disposition": f"attachment; filename=leads_{ts}.json"},
    )


# ─── Import ───────────────────────────────────────────────────────────────

def _import_rows(rows: list[dict]) -> dict:
    """Upsert a list of dicts into the DB. Returns summary counts."""
    from scraper.models import Lead
    added = merged = skipped = 0
    for row in rows:
        website = (row.get("website") or row.get("Website") or "").strip()
        if not website:
            skipped += 1
            continue
        # Normalise field names (support both lowercase and Title Case headers)
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
            website=website,
            emails=emails,
            phones=phones,
            address=_get("address", "Address"),
            city=_get("city", "City"),
            country=_get("country", "Country"),
            niche=_get("niche", "Niche"),
            sources=["import"],
            confidence=float(row.get("confidence", row.get("Confidence", 0.5)) or 0.5),
        )
        was_new, _ = db.upsert(lead)
        if was_new:
            added += 1
        else:
            merged += 1
    return {"added": added, "merged": merged, "skipped": skipped}


@app.post("/api/import/csv")
async def import_csv(file: UploadFile = File(...)):
    import csv
    import io
    content = await file.read()
    text = content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)
    result = _import_rows(rows)
    return result


@app.post("/api/import/excel")
async def import_excel(file: UploadFile = File(...)):
    import io
    try:
        import openpyxl
    except ImportError:
        raise HTTPException(500, "openpyxl not installed")
    content = await file.read()
    wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    ws = wb.active
    rows_iter = ws.iter_rows(values_only=True)
    headers = [str(h).strip() if h is not None else "" for h in next(rows_iter, [])]
    rows = []
    for row in rows_iter:
        rows.append({headers[i]: (str(v).strip() if v is not None else "") for i, v in enumerate(row) if i < len(headers)})
    result = _import_rows(rows)
    return result


@app.post("/api/import/json")
async def import_json_file(file: UploadFile = File(...)):
    import json as _json
    content = await file.read()
    rows = _json.loads(content)
    if not isinstance(rows, list):
        raise HTTPException(400, "JSON must be an array of lead objects")
    result = _import_rows(rows)
    return result


# ─── Static files + SPA fallback ─────────────────────────────────────────

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/{full_path:path}")
def spa_fallback(full_path: str):
    return FileResponse(str(STATIC_DIR / "index.html"))


# ─── Run ──────────────────────────────────────────────────────────────────

def run_server(host: str = "127.0.0.1", port: int = 7337, open_browser: bool = True):
    import warnings
    import urllib3
    warnings.filterwarnings("ignore")
    urllib3.disable_warnings()

    if open_browser:
        import threading, time, webbrowser
        def _open():
            time.sleep(1.2)
            webbrowser.open(f"http://{host}:{port}")
        threading.Thread(target=_open, daemon=True).start()

    print(f"\n  Lead Manager Web UI")
    print(f"  → http://{host}:{port}\n")
    uvicorn.run(app, host=host, port=port, log_level="warning")
