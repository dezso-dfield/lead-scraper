"""
Central SQLite database for all scraped leads.
Supports multiple project databases via Database class + get_instance() factory.
Module-level functions delegate to the default (backward-compat) instance.
"""
from __future__ import annotations
import json
import sqlite3
import threading
from datetime import datetime
from pathlib import Path

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
    last_emailed_at TEXT    NOT NULL DEFAULT '',
    created_at      TEXT    NOT NULL,
    updated_at      TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_canonical ON leads(canonical_key);
CREATE INDEX IF NOT EXISTS idx_niche     ON leads(niche);
CREATE INDEX IF NOT EXISTS idx_status    ON leads(status);
CREATE INDEX IF NOT EXISTS idx_city      ON leads(city);
CREATE INDEX IF NOT EXISTS idx_updated   ON leads(updated_at);

CREATE TABLE IF NOT EXISTS email_logs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id     INTEGER NOT NULL,
    sent_at     TEXT    NOT NULL,
    subject     TEXT    NOT NULL DEFAULT '',
    to_email    TEXT    NOT NULL DEFAULT '',
    status      TEXT    NOT NULL DEFAULT 'sent',
    error       TEXT    NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_email_log_lead ON email_logs(lead_id);
CREATE INDEX IF NOT EXISTS idx_email_log_sent ON email_logs(sent_at);
"""


# ─── Database class ──────────────────────────────────────────────────────────

class Database:
    """Per-project database handle with thread-local connections."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()

    # ── Connection ──────────────────────────────────────────────────────────

    def _conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(str(self.path), check_same_thread=False)
            self._local.conn.row_factory = sqlite3.Row
            self._local.conn.executescript(SCHEMA)
            self._migrate(self._local.conn)
        return self._local.conn

    def _migrate(self, conn: sqlite3.Connection) -> None:
        existing = {row[1] for row in conn.execute("PRAGMA table_info(leads)").fetchall()}
        if "last_emailed_at" not in existing:
            conn.execute("ALTER TABLE leads ADD COLUMN last_emailed_at TEXT NOT NULL DEFAULT ''")
            conn.commit()

    @staticmethod
    def _now() -> str:
        return datetime.utcnow().isoformat()

    # ── Write ────────────────────────────────────────────────────────────────

    def upsert(self, lead: Lead) -> tuple[bool, int]:
        """Insert or merge lead. Returns (was_new, id)."""
        key = lead.canonical_key()
        if not key:
            return False, -1

        conn = self._conn()
        row = conn.execute(
            "SELECT id, emails, phones, sources, confidence, company_name, address FROM leads WHERE canonical_key = ?",
            (key,)
        ).fetchone()
        now = self._now()

        if row is None:
            cur = conn.execute(
                """INSERT INTO leads
                   (canonical_key, company_name, website, emails, phones, address,
                    city, country, niche, sources, confidence, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    key, lead.company_name, lead.website,
                    json.dumps(lead.emails), json.dumps(lead.phones),
                    lead.address, lead.city, lead.country, lead.niche,
                    json.dumps(lead.sources), lead.confidence, now, now,
                ),
            )
            conn.commit()
            return True, cur.lastrowid
        else:
            emails  = _merge_list(json.loads(row["emails"]),  lead.emails)
            phones  = _merge_list(json.loads(row["phones"]),  lead.phones)
            sources = _merge_list(json.loads(row["sources"]), lead.sources)
            conf    = max(row["confidence"], lead.confidence)
            name    = lead.company_name if len(lead.company_name) > len(row["company_name"]) else row["company_name"]
            address = lead.address if (lead.address and len(lead.address) > len(row["address"])) else row["address"]
            conn.execute(
                """UPDATE leads SET emails=?, phones=?, sources=?, confidence=?,
                   company_name=?, address=?, updated_at=? WHERE id=?""",
                (json.dumps(emails), json.dumps(phones), json.dumps(sources),
                 conf, name, address, now, row["id"]),
            )
            conn.commit()
            return False, row["id"]

    def update_status(self, lead_id: int, status: str) -> None:
        conn = self._conn()
        conn.execute("UPDATE leads SET status=?, updated_at=? WHERE id=?", (status, self._now(), lead_id))
        conn.commit()

    def update_notes(self, lead_id: int, notes: str) -> None:
        conn = self._conn()
        conn.execute("UPDATE leads SET notes=?, updated_at=? WHERE id=?", (notes, self._now(), lead_id))
        conn.commit()

    def delete(self, lead_id: int) -> None:
        conn = self._conn()
        conn.execute("DELETE FROM leads WHERE id=?", (lead_id,))
        conn.commit()

    def delete_many(self, ids: list[int]) -> None:
        conn = self._conn()
        conn.executemany("DELETE FROM leads WHERE id=?", [(i,) for i in ids])
        conn.commit()

    def update_last_emailed(self, lead_id: int) -> None:
        conn = self._conn()
        conn.execute("UPDATE leads SET last_emailed_at=?, updated_at=? WHERE id=?",
                     (self._now(), self._now(), lead_id))
        conn.commit()

    def log_email(self, lead_id: int, subject: str, to_email: str, status: str, error: str = "") -> None:
        conn = self._conn()
        conn.execute(
            "INSERT INTO email_logs (lead_id, sent_at, subject, to_email, status, error) VALUES (?,?,?,?,?,?)",
            (lead_id, self._now(), subject, to_email, status, error),
        )
        conn.commit()

    # ── Read ─────────────────────────────────────────────────────────────────

    def fetch_all(
        self,
        search: str = "",
        niche: str = "",
        city: str = "",
        status: str = "",
        has_email: bool = False,
        has_phone: bool = False,
        order_by: str = "updated_at DESC",
    ) -> list[dict]:
        conn = self._conn()
        clauses: list[str] = ["1=1"]
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

    def fetch_by_id(self, lead_id: int) -> dict | None:
        conn = self._conn()
        row = conn.execute("SELECT * FROM leads WHERE id=?", (lead_id,)).fetchone()
        return _row_to_dict(row) if row else None

    def stats(self) -> dict:
        conn = self._conn()
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

    def exists(self, canonical_key: str) -> bool:
        conn = self._conn()
        row = conn.execute("SELECT 1 FROM leads WHERE canonical_key=?", (canonical_key,)).fetchone()
        return row is not None

    def niches(self) -> list[str]:
        conn = self._conn()
        rows = conn.execute("SELECT DISTINCT niche FROM leads WHERE niche != '' ORDER BY niche").fetchall()
        return [r["niche"] for r in rows]

    def cities(self) -> list[str]:
        conn = self._conn()
        rows = conn.execute("SELECT DISTINCT city FROM leads WHERE city != '' ORDER BY city").fetchall()
        return [r["city"] for r in rows]

    def fetch_email_logs(self, lead_id: int | None = None, limit: int = 200) -> list[dict]:
        conn = self._conn()
        if lead_id:
            rows = conn.execute(
                "SELECT * FROM email_logs WHERE lead_id=? ORDER BY sent_at DESC LIMIT ?",
                (lead_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT el.*, l.company_name FROM email_logs el "
                "LEFT JOIN leads l ON l.id=el.lead_id ORDER BY el.sent_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]


# ─── Factory + caching ───────────────────────────────────────────────────────

_instances: dict[str, Database] = {}


def get_instance(path: Path) -> Database:
    key = str(path)
    if key not in _instances:
        _instances[key] = Database(path)
    return _instances[key]


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _merge_list(existing: list, new: list) -> list:
    seen = set(existing)
    result = list(existing)
    for item in new:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def _row_to_dict(row: sqlite3.Row) -> dict:
    d = dict(row)
    d["emails"]  = json.loads(d.get("emails",  "[]"))
    d["phones"]  = json.loads(d.get("phones",  "[]"))
    d["sources"] = json.loads(d.get("sources", "[]"))
    return d


# ─── Backward-compat module-level functions (default project) ─────────────────

_default = Database(DB_PATH)


def upsert(lead: Lead) -> tuple[bool, int]:                      return _default.upsert(lead)
def update_status(lead_id: int, status: str) -> None:            _default.update_status(lead_id, status)
def update_notes(lead_id: int, notes: str) -> None:              _default.update_notes(lead_id, notes)
def delete(lead_id: int) -> None:                                _default.delete(lead_id)
def delete_many(ids: list[int]) -> None:                         _default.delete_many(ids)
def update_last_emailed(lead_id: int) -> None:                   _default.update_last_emailed(lead_id)
def log_email(lead_id, subject, to_email, status, error=""):     _default.log_email(lead_id, subject, to_email, status, error)
def fetch_all(**kwargs) -> list[dict]:                           return _default.fetch_all(**kwargs)
def fetch_by_id(lead_id: int) -> dict | None:                    return _default.fetch_by_id(lead_id)
def stats() -> dict:                                             return _default.stats()
def exists(key: str) -> bool:                                    return _default.exists(key)
def niches() -> list[str]:                                       return _default.niches()
def cities() -> list[str]:                                       return _default.cities()
def fetch_email_logs(**kwargs) -> list[dict]:                    return _default.fetch_email_logs(**kwargs)
