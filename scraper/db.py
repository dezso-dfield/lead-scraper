"""
Central SQLite database for all scraped leads.
Single source of truth — all scrapers write here, TUI reads from here.
"""
from __future__ import annotations
import json
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Iterator

from scraper.models import Lead

DB_PATH = Path.home() / ".scraper" / "leads.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

SCHEMA = """
CREATE TABLE IF NOT EXISTS leads (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_key   TEXT    UNIQUE NOT NULL,
    company_name    TEXT    NOT NULL DEFAULT '',
    website         TEXT    NOT NULL DEFAULT '',
    emails          TEXT    NOT NULL DEFAULT '[]',
    phones          TEXT    NOT NULL DEFAULT '[]',
    address         TEXT    NOT NULL DEFAULT '',
    city            TEXT    NOT NULL DEFAULT '',
    country         TEXT    NOT NULL DEFAULT '',
    niche           TEXT    NOT NULL DEFAULT '',
    sources         TEXT    NOT NULL DEFAULT '[]',
    confidence      REAL    NOT NULL DEFAULT 0.0,
    status          TEXT    NOT NULL DEFAULT 'new',
    notes           TEXT    NOT NULL DEFAULT '',
    created_at      TEXT    NOT NULL,
    updated_at      TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_canonical ON leads(canonical_key);
CREATE INDEX IF NOT EXISTS idx_niche     ON leads(niche);
CREATE INDEX IF NOT EXISTS idx_status    ON leads(status);
CREATE INDEX IF NOT EXISTS idx_city      ON leads(city);
CREATE INDEX IF NOT EXISTS idx_updated   ON leads(updated_at);
"""

_local = threading.local()


def _conn() -> sqlite3.Connection:
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        _local.conn.row_factory = sqlite3.Row
        _local.conn.executescript(SCHEMA)
    return _local.conn


def _now() -> str:
    return datetime.utcnow().isoformat()


# ─── Write ──────────────────────────────────────────────────────────────────

def upsert(lead: Lead) -> tuple[bool, int]:
    """Insert or merge lead. Returns (was_new, id)."""
    key = lead.canonical_key()
    if not key:
        return False, -1

    conn = _conn()
    row = conn.execute(
        "SELECT id, emails, phones, sources, confidence, company_name, address FROM leads WHERE canonical_key = ?",
        (key,)
    ).fetchone()

    now = _now()

    if row is None:
        cur = conn.execute(
            """INSERT INTO leads
               (canonical_key, company_name, website, emails, phones, address,
                city, country, niche, sources, confidence, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                key,
                lead.company_name,
                lead.website,
                json.dumps(lead.emails),
                json.dumps(lead.phones),
                lead.address,
                lead.city,
                lead.country,
                lead.niche,
                json.dumps(lead.sources),
                lead.confidence,
                now,
                now,
            ),
        )
        conn.commit()
        return True, cur.lastrowid
    else:
        # Merge
        emails = _merge_list(json.loads(row["emails"]), lead.emails)
        phones = _merge_list(json.loads(row["phones"]), lead.phones)
        sources = _merge_list(json.loads(row["sources"]), lead.sources)
        conf = max(row["confidence"], lead.confidence)
        name = lead.company_name if len(lead.company_name) > len(row["company_name"]) else row["company_name"]
        address = lead.address if (lead.address and len(lead.address) > len(row["address"])) else row["address"]
        conn.execute(
            """UPDATE leads SET
               emails=?, phones=?, sources=?, confidence=?, company_name=?,
               address=?, updated_at=?
               WHERE id=?""",
            (json.dumps(emails), json.dumps(phones), json.dumps(sources),
             conf, name, address, now, row["id"]),
        )
        conn.commit()
        return False, row["id"]


def _merge_list(existing: list, new: list) -> list:
    seen = set(existing)
    result = list(existing)
    for item in new:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def update_status(lead_id: int, status: str) -> None:
    conn = _conn()
    conn.execute("UPDATE leads SET status=?, updated_at=? WHERE id=?", (status, _now(), lead_id))
    conn.commit()


def update_notes(lead_id: int, notes: str) -> None:
    conn = _conn()
    conn.execute("UPDATE leads SET notes=?, updated_at=? WHERE id=?", (notes, _now(), lead_id))
    conn.commit()


def delete(lead_id: int) -> None:
    conn = _conn()
    conn.execute("DELETE FROM leads WHERE id=?", (lead_id,))
    conn.commit()


def delete_many(ids: list[int]) -> None:
    conn = _conn()
    conn.executemany("DELETE FROM leads WHERE id=?", [(i,) for i in ids])
    conn.commit()


# ─── Read ───────────────────────────────────────────────────────────────────

def fetch_all(
    search: str = "",
    niche: str = "",
    city: str = "",
    status: str = "",
    has_email: bool = False,
    has_phone: bool = False,
    order_by: str = "updated_at DESC",
) -> list[dict]:
    conn = _conn()
    clauses = ["1=1"]
    params: list = []

    if search:
        clauses.append("(company_name LIKE ? OR emails LIKE ? OR website LIKE ?)")
        s = f"%{search}%"
        params += [s, s, s]
    if niche:
        clauses.append("niche LIKE ?")
        params.append(f"%{niche}%")
    if city:
        clauses.append("(city LIKE ? OR address LIKE ?)")
        params += [f"%{city}%", f"%{city}%"]
    if status:
        clauses.append("status = ?")
        params.append(status)
    if has_email:
        clauses.append("emails != '[]'")
    if has_phone:
        clauses.append("phones != '[]'")

    sql = f"SELECT * FROM leads WHERE {' AND '.join(clauses)} ORDER BY {order_by}"
    rows = conn.execute(sql, params).fetchall()
    return [_row_to_dict(r) for r in rows]


def fetch_by_id(lead_id: int) -> dict | None:
    conn = _conn()
    row = conn.execute("SELECT * FROM leads WHERE id=?", (lead_id,)).fetchone()
    return _row_to_dict(row) if row else None


def stats() -> dict:
    conn = _conn()
    row = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN emails != '[]' THEN 1 ELSE 0 END) as with_email,
            SUM(CASE WHEN phones != '[]' THEN 1 ELSE 0 END) as with_phone,
            SUM(CASE WHEN emails != '[]' AND phones != '[]' THEN 1 ELSE 0 END) as with_both,
            SUM(CASE WHEN status='new' THEN 1 ELSE 0 END) as status_new,
            SUM(CASE WHEN status='contacted' THEN 1 ELSE 0 END) as status_contacted,
            SUM(CASE WHEN status='qualified' THEN 1 ELSE 0 END) as status_qualified,
            SUM(CASE WHEN status='rejected' THEN 1 ELSE 0 END) as status_rejected
        FROM leads
    """).fetchone()
    return dict(row) if row else {}


def niches() -> list[str]:
    conn = _conn()
    rows = conn.execute("SELECT DISTINCT niche FROM leads WHERE niche != '' ORDER BY niche").fetchall()
    return [r["niche"] for r in rows]


def cities() -> list[str]:
    conn = _conn()
    rows = conn.execute("SELECT DISTINCT city FROM leads WHERE city != '' ORDER BY city").fetchall()
    return [r["city"] for r in rows]


def exists(canonical_key: str) -> bool:
    conn = _conn()
    row = conn.execute("SELECT 1 FROM leads WHERE canonical_key=?", (canonical_key,)).fetchone()
    return row is not None


def _row_to_dict(row: sqlite3.Row) -> dict:
    d = dict(row)
    d["emails"] = json.loads(d.get("emails", "[]"))
    d["phones"] = json.loads(d.get("phones", "[]"))
    d["sources"] = json.loads(d.get("sources", "[]"))
    return d
