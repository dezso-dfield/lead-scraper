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
    callback_at     TEXT    NOT NULL DEFAULT '',
    tags            TEXT    NOT NULL DEFAULT '[]',
    contact_name    TEXT    NOT NULL DEFAULT '',
    contact_title   TEXT    NOT NULL DEFAULT '',
    rating          TEXT    NOT NULL DEFAULT '',
    unsub_token     TEXT    NOT NULL DEFAULT '',
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

CREATE TABLE IF NOT EXISTS activity_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id       INTEGER NOT NULL,
    activity_type TEXT    NOT NULL DEFAULT 'note',
    outcome       TEXT    NOT NULL DEFAULT '',
    subject       TEXT    NOT NULL DEFAULT '',
    notes         TEXT    NOT NULL DEFAULT '',
    created_at    TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_activity_lead ON activity_log(lead_id);
CREATE INDEX IF NOT EXISTS idx_activity_time ON activity_log(created_at);

CREATE TABLE IF NOT EXISTS scripts (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT    NOT NULL DEFAULT '',
    subject    TEXT    NOT NULL DEFAULT '',
    body       TEXT    NOT NULL DEFAULT '',
    created_at TEXT    NOT NULL,
    updated_at TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS sequences (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT    NOT NULL DEFAULT '',
    trigger    TEXT    NOT NULL DEFAULT 'manual',
    steps      TEXT    NOT NULL DEFAULT '[]',
    active     INTEGER NOT NULL DEFAULT 1,
    created_at TEXT    NOT NULL,
    updated_at TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_seq_trigger ON sequences(trigger, active);

CREATE TABLE IF NOT EXISTS enrollments (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id     INTEGER NOT NULL,
    sequence_id INTEGER NOT NULL,
    step_index  INTEGER NOT NULL DEFAULT 0,
    next_run_at TEXT    NOT NULL DEFAULT '',
    status      TEXT    NOT NULL DEFAULT 'active',
    created_at  TEXT    NOT NULL,
    updated_at  TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_enroll_next ON enrollments(next_run_at, status);
CREATE INDEX IF NOT EXISTS idx_enroll_lead ON enrollments(lead_id);

CREATE TABLE IF NOT EXISTS email_opens (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id    INTEGER NOT NULL,
    token      TEXT    UNIQUE NOT NULL,
    opened_at  TEXT    NOT NULL DEFAULT '',
    created_at TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_opens_token ON email_opens(token);
CREATE INDEX IF NOT EXISTS idx_opens_lead  ON email_opens(lead_id);

CREATE TABLE IF NOT EXISTS webhooks (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    url        TEXT    NOT NULL DEFAULT '',
    event      TEXT    NOT NULL DEFAULT 'status_changed',
    active     INTEGER NOT NULL DEFAULT 1,
    created_at TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_webhooks_event ON webhooks(event, active);
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
        if "last_called_at" not in existing:
            conn.execute("ALTER TABLE leads ADD COLUMN last_called_at TEXT NOT NULL DEFAULT ''")
        if "callback_at" not in existing:
            conn.execute("ALTER TABLE leads ADD COLUMN callback_at TEXT NOT NULL DEFAULT ''")
        if "tags" not in existing:
            conn.execute("ALTER TABLE leads ADD COLUMN tags TEXT NOT NULL DEFAULT '[]'")
        if "contact_name" not in existing:
            conn.execute("ALTER TABLE leads ADD COLUMN contact_name TEXT NOT NULL DEFAULT ''")
        if "contact_title" not in existing:
            conn.execute("ALTER TABLE leads ADD COLUMN contact_title TEXT NOT NULL DEFAULT ''")
        if "rating" not in existing:
            conn.execute("ALTER TABLE leads ADD COLUMN rating TEXT NOT NULL DEFAULT ''")
        if "unsub_token" not in existing:
            conn.execute("ALTER TABLE leads ADD COLUMN unsub_token TEXT NOT NULL DEFAULT ''")
        # Ensure activity_log table exists (created after some installs)
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS activity_log (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id       INTEGER NOT NULL,
                activity_type TEXT    NOT NULL DEFAULT 'note',
                outcome       TEXT    NOT NULL DEFAULT '',
                subject       TEXT    NOT NULL DEFAULT '',
                notes         TEXT    NOT NULL DEFAULT '',
                created_at    TEXT    NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_activity_lead ON activity_log(lead_id);
            CREATE INDEX IF NOT EXISTS idx_activity_time ON activity_log(created_at);
            CREATE TABLE IF NOT EXISTS scripts (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                name       TEXT    NOT NULL DEFAULT '',
                subject    TEXT    NOT NULL DEFAULT '',
                body       TEXT    NOT NULL DEFAULT '',
                created_at TEXT    NOT NULL,
                updated_at TEXT    NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sequences (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                name       TEXT    NOT NULL DEFAULT '',
                trigger    TEXT    NOT NULL DEFAULT 'manual',
                steps      TEXT    NOT NULL DEFAULT '[]',
                active     INTEGER NOT NULL DEFAULT 1,
                created_at TEXT    NOT NULL,
                updated_at TEXT    NOT NULL
            );
            CREATE TABLE IF NOT EXISTS enrollments (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id     INTEGER NOT NULL,
                sequence_id INTEGER NOT NULL,
                step_index  INTEGER NOT NULL DEFAULT 0,
                next_run_at TEXT    NOT NULL DEFAULT '',
                status      TEXT    NOT NULL DEFAULT 'active',
                created_at  TEXT    NOT NULL,
                updated_at  TEXT    NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_seq_trigger ON sequences(trigger, active);
            CREATE INDEX IF NOT EXISTS idx_enroll_next ON enrollments(next_run_at, status);
            CREATE INDEX IF NOT EXISTS idx_enroll_lead ON enrollments(lead_id);
            CREATE TABLE IF NOT EXISTS email_opens (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id    INTEGER NOT NULL,
                token      TEXT    UNIQUE NOT NULL,
                opened_at  TEXT    NOT NULL DEFAULT '',
                created_at TEXT    NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_opens_token ON email_opens(token);
            CREATE INDEX IF NOT EXISTS idx_opens_lead  ON email_opens(lead_id);
            CREATE TABLE IF NOT EXISTS webhooks (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                url        TEXT    NOT NULL DEFAULT '',
                event      TEXT    NOT NULL DEFAULT 'status_changed',
                active     INTEGER NOT NULL DEFAULT 1,
                created_at TEXT    NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_webhooks_event ON webhooks(event, active);
        """)
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

    def update_last_called(self, lead_id: int) -> None:
        conn = self._conn()
        conn.execute("UPDATE leads SET last_called_at=?, updated_at=? WHERE id=?",
                     (self._now(), self._now(), lead_id))
        conn.commit()

    def log_activity(self, lead_id: int, activity_type: str, outcome: str = "",
                     subject: str = "", notes: str = "") -> int:
        conn = self._conn()
        cur = conn.execute(
            "INSERT INTO activity_log (lead_id, activity_type, outcome, subject, notes, created_at) VALUES (?,?,?,?,?,?)",
            (lead_id, activity_type, outcome, subject, notes, self._now()),
        )
        conn.commit()
        return cur.lastrowid

    def fetch_activities(self, lead_id: int | None = None, limit: int = 300,
                         activity_type: str = "") -> list[dict]:
        conn = self._conn()
        clauses = ["1=1"]
        params: list = []
        if lead_id:
            clauses.append("al.lead_id=?"); params.append(lead_id)
        if activity_type:
            clauses.append("al.activity_type=?"); params.append(activity_type)
        sql = (
            f"SELECT al.*, l.company_name, l.website FROM activity_log al "
            f"LEFT JOIN leads l ON l.id=al.lead_id "
            f"WHERE {' AND '.join(clauses)} ORDER BY al.created_at DESC LIMIT ?"
        )
        params.append(limit)
        return [dict(r) for r in conn.execute(sql, params).fetchall()]

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
        tag: str = "",
        callback_overdue: bool = False,
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
        if tag:
            clauses.append("tags LIKE ?")
            params.append(f"%{tag}%")
        if callback_overdue:
            clauses.append("callback_at != '' AND callback_at <= datetime('now')")

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
                SUM(CASE WHEN status='warm' THEN 1 ELSE 0 END) as status_warm,
                SUM(CASE WHEN status='qualified' THEN 1 ELSE 0 END) as status_qualified,
                SUM(CASE WHEN status='rejected' THEN 1 ELSE 0 END) as status_rejected,
                SUM(CASE WHEN callback_at != '' AND callback_at <= datetime('now') THEN 1 ELSE 0 END) as overdue_callbacks
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

    # ── Scripts ──────────────────────────────────────────────────────────────

    def fetch_scripts(self) -> list[dict]:
        conn = self._conn()
        rows = conn.execute("SELECT * FROM scripts ORDER BY name ASC").fetchall()
        return [dict(r) for r in rows]

    def save_script(self, name: str, subject: str, body: str,
                    script_id: int | None = None) -> dict:
        conn = self._conn()
        now  = self._now()
        if script_id:
            conn.execute(
                "UPDATE scripts SET name=?, subject=?, body=?, updated_at=? WHERE id=?",
                (name, subject, body, now, script_id),
            )
        else:
            conn.execute(
                "INSERT INTO scripts (name, subject, body, created_at, updated_at) VALUES (?,?,?,?,?)",
                (name, subject, body, now, now),
            )
        conn.commit()
        row = conn.execute(
            "SELECT * FROM scripts WHERE id = last_insert_rowid()" if not script_id
            else f"SELECT * FROM scripts WHERE id = {script_id}"
        ).fetchone()
        return dict(row)

    def delete_script(self, script_id: int) -> None:
        conn = self._conn()
        conn.execute("DELETE FROM scripts WHERE id=?", (script_id,))
        conn.commit()

    # ── Reset ────────────────────────────────────────────────────────────────

    def delete_all_leads(self) -> int:
        conn = self._conn()
        count = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
        conn.execute("DELETE FROM leads")
        conn.execute("DELETE FROM email_logs")
        conn.execute("DELETE FROM activity_log")
        conn.execute("DELETE FROM enrollments")
        conn.commit()
        return count

    def update_tags(self, lead_id: int, tags: list[str]) -> None:
        conn = self._conn()
        conn.execute("UPDATE leads SET tags=?, updated_at=? WHERE id=?",
                     (json.dumps(tags), self._now(), lead_id))
        conn.commit()

    def update_callback_at(self, lead_id: int, callback_at: str) -> None:
        conn = self._conn()
        conn.execute("UPDATE leads SET callback_at=?, updated_at=? WHERE id=?",
                     (callback_at, self._now(), lead_id))
        conn.commit()

    def bulk_update_status(self, lead_ids: list[int], status: str) -> int:
        if not lead_ids:
            return 0
        conn = self._conn()
        placeholders = ",".join("?" * len(lead_ids))
        conn.execute(
            f"UPDATE leads SET status=?, updated_at=? WHERE id IN ({placeholders})",
            [status, self._now()] + lead_ids,
        )
        conn.commit()
        return len(lead_ids)

    def create_open_token(self, lead_id: int, token: str) -> None:
        conn = self._conn()
        conn.execute(
            "INSERT OR IGNORE INTO email_opens (lead_id, token, created_at) VALUES (?,?,?)",
            (lead_id, token, self._now()),
        )
        conn.commit()

    def record_open(self, token: str) -> int | None:
        """Mark token as opened. Returns lead_id or None."""
        conn = self._conn()
        row = conn.execute("SELECT lead_id, opened_at FROM email_opens WHERE token=?", (token,)).fetchone()
        if not row:
            return None
        if not row["opened_at"]:
            conn.execute("UPDATE email_opens SET opened_at=? WHERE token=?", (self._now(), token))
            conn.execute("UPDATE leads SET status='warm', updated_at=? WHERE id=? AND status NOT IN ('qualified','rejected')",
                         (self._now(), row["lead_id"]))
            conn.commit()
        return row["lead_id"]

    def dashboard_stats(self) -> dict:
        conn = self._conn()
        funnel = conn.execute("""
            SELECT
                SUM(CASE WHEN status='new' THEN 1 ELSE 0 END) as new,
                SUM(CASE WHEN status='contacted' THEN 1 ELSE 0 END) as contacted,
                SUM(CASE WHEN status='warm' THEN 1 ELSE 0 END) as warm,
                SUM(CASE WHEN status='qualified' THEN 1 ELSE 0 END) as qualified,
                SUM(CASE WHEN status='rejected' THEN 1 ELSE 0 END) as rejected,
                COUNT(*) as total
            FROM leads
        """).fetchone()
        emails = conn.execute("""
            SELECT
                COUNT(*) as total_sent,
                SUM(CASE WHEN status='sent' THEN 1 ELSE 0 END) as sent,
                SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) as failed
            FROM email_logs
        """).fetchone()
        opens = conn.execute("SELECT COUNT(*) as opened FROM email_opens WHERE opened_at != ''").fetchone()
        callbacks = conn.execute(
            "SELECT COUNT(*) as due FROM leads WHERE callback_at != '' AND callback_at <= datetime('now')"
        ).fetchone()
        return {
            "funnel": dict(funnel) if funnel else {},
            "emails": {**(dict(emails) if emails else {}), "opened": opens["opened"] if opens else 0},
            "callbacks_due": callbacks["due"] if callbacks else 0,
        }

    def update_contact(self, lead_id: int, contact_name: str, contact_title: str) -> None:
        conn = self._conn()
        conn.execute("UPDATE leads SET contact_name=?, contact_title=?, updated_at=? WHERE id=?",
                     (contact_name, contact_title, self._now(), lead_id))
        conn.commit()

    def update_company_name(self, lead_id: int, company_name: str) -> None:
        conn = self._conn()
        conn.execute("UPDATE leads SET company_name=?, updated_at=? WHERE id=?",
                     (company_name, self._now(), lead_id))
        conn.commit()

    def update_niche(self, lead_id: int, niche: str) -> None:
        conn = self._conn()
        conn.execute("UPDATE leads SET niche=?, updated_at=? WHERE id=?",
                     (niche, self._now(), lead_id))
        conn.commit()

    def get_or_create_unsub_token(self, lead_id: int) -> str:
        import secrets
        conn = self._conn()
        row = conn.execute("SELECT unsub_token FROM leads WHERE id=?", (lead_id,)).fetchone()
        if not row:
            return ""
        token = row["unsub_token"]
        if not token:
            token = secrets.token_urlsafe(20)
            conn.execute("UPDATE leads SET unsub_token=? WHERE id=?", (token, lead_id))
            conn.commit()
        return token

    def unsubscribe_by_token(self, token: str) -> dict | None:
        """Mark lead as rejected via unsubscribe token. Returns lead dict or None."""
        conn = self._conn()
        row = conn.execute("SELECT id, status FROM leads WHERE unsub_token=?", (token,)).fetchone()
        if not row:
            return None
        lead_id = row["id"]
        if row["status"] != "rejected":
            conn.execute("UPDATE leads SET status='rejected', updated_at=? WHERE id=?",
                         (self._now(), lead_id))
            self.log_activity(lead_id, "unsubscribed", outcome="rejected", notes="Via unsubscribe link")
            conn.commit()
        return self.fetch_by_id(lead_id)

    # ── Webhooks ─────────────────────────────────────────────────────────────

    def fetch_webhooks(self, event: str = "") -> list[dict]:
        conn = self._conn()
        if event:
            rows = conn.execute(
                "SELECT * FROM webhooks WHERE (event=? OR event='*') AND active=1 ORDER BY id",
                (event,),
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM webhooks ORDER BY id").fetchall()
        return [dict(r) for r in rows]

    def save_webhook(self, url: str, event: str, active: bool = True, webhook_id: int | None = None) -> dict:
        conn = self._conn()
        now = self._now()
        if webhook_id:
            conn.execute("UPDATE webhooks SET url=?, event=?, active=?, created_at=? WHERE id=?",
                         (url, event, int(active), now, webhook_id))
        else:
            conn.execute("INSERT INTO webhooks (url, event, active, created_at) VALUES (?,?,?,?)",
                         (url, event, int(active), now))
        conn.commit()
        if webhook_id:
            row = conn.execute("SELECT * FROM webhooks WHERE id=?", (webhook_id,)).fetchone()
        else:
            row = conn.execute("SELECT * FROM webhooks WHERE id=last_insert_rowid()").fetchone()
        return dict(row)

    def delete_webhook(self, webhook_id: int) -> None:
        conn = self._conn()
        conn.execute("DELETE FROM webhooks WHERE id=?", (webhook_id,))
        conn.commit()

    # ── Sequences ────────────────────────────────────────────────────────────

    def fetch_sequences(self) -> list[dict]:
        conn = self._conn()
        rows = conn.execute("SELECT * FROM sequences ORDER BY name ASC").fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["steps"] = json.loads(d.get("steps", "[]"))
            result.append(d)
        return result

    def sequences_by_trigger(self, trigger: str) -> list[dict]:
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM sequences WHERE trigger=? AND active=1", (trigger,)
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["steps"] = json.loads(d.get("steps", "[]"))
            result.append(d)
        return result

    def save_sequence(self, name: str, trigger: str, steps: list,
                      active: bool = True, seq_id: int | None = None) -> dict:
        conn = self._conn()
        now = self._now()
        steps_json = json.dumps(steps)
        if seq_id:
            conn.execute(
                "UPDATE sequences SET name=?, trigger=?, steps=?, active=?, updated_at=? WHERE id=?",
                (name, trigger, steps_json, int(active), now, seq_id),
            )
            conn.commit()
            row = conn.execute("SELECT * FROM sequences WHERE id=?", (seq_id,)).fetchone()
        else:
            conn.execute(
                "INSERT INTO sequences (name, trigger, steps, active, created_at, updated_at) VALUES (?,?,?,?,?,?)",
                (name, trigger, steps_json, int(active), now, now),
            )
            conn.commit()
            row = conn.execute("SELECT * FROM sequences WHERE id = last_insert_rowid()").fetchone()
        d = dict(row)
        d["steps"] = json.loads(d.get("steps", "[]"))
        return d

    def delete_sequence(self, seq_id: int) -> None:
        conn = self._conn()
        conn.execute("DELETE FROM sequences WHERE id=?", (seq_id,))
        conn.execute("DELETE FROM enrollments WHERE sequence_id=?", (seq_id,))
        conn.commit()

    # ── Enrollments ──────────────────────────────────────────────────────────

    def enroll_lead(self, lead_id: int, sequence_id: int) -> dict | None:
        """Enroll a lead in a sequence. Returns enrollment dict, or None if already active."""
        conn = self._conn()
        existing = conn.execute(
            "SELECT id FROM enrollments WHERE lead_id=? AND sequence_id=? AND status='active'",
            (lead_id, sequence_id),
        ).fetchone()
        if existing:
            return None
        seq = conn.execute("SELECT * FROM sequences WHERE id=?", (sequence_id,)).fetchone()
        if not seq:
            return None
        steps = json.loads(seq["steps"])
        if not steps:
            return None
        from datetime import timedelta
        delay_days = float(steps[0].get("delay_days", 1))
        next_run = (datetime.utcnow() + timedelta(days=delay_days)).isoformat()
        now = self._now()
        cur = conn.execute(
            "INSERT INTO enrollments (lead_id, sequence_id, step_index, next_run_at, status, created_at, updated_at) VALUES (?,?,?,?,?,?,?)",
            (lead_id, sequence_id, 0, next_run, "active", now, now),
        )
        conn.commit()
        return dict(conn.execute("SELECT * FROM enrollments WHERE id=?", (cur.lastrowid,)).fetchone())

    def fetch_enrollments(self, lead_id: int | None = None) -> list[dict]:
        conn = self._conn()
        if lead_id:
            rows = conn.execute(
                """SELECT e.*, s.name as seq_name, s.steps as seq_steps
                   FROM enrollments e JOIN sequences s ON s.id=e.sequence_id
                   WHERE e.lead_id=? ORDER BY e.created_at DESC""",
                (lead_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT e.*, s.name as seq_name, l.company_name
                   FROM enrollments e
                   JOIN sequences s ON s.id=e.sequence_id
                   LEFT JOIN leads l ON l.id=e.lead_id
                   ORDER BY e.next_run_at ASC"""
            ).fetchall()
        return [dict(r) for r in rows]

    def get_due_enrollments(self) -> list[dict]:
        conn = self._conn()
        now = self._now()
        rows = conn.execute(
            """SELECT e.*, s.steps, s.name as seq_name, l.company_name, l.emails
               FROM enrollments e
               JOIN sequences s ON s.id=e.sequence_id
               JOIN leads l ON l.id=e.lead_id
               WHERE e.status='active' AND e.next_run_at <= ?
               ORDER BY e.next_run_at ASC""",
            (now,),
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["steps_list"] = json.loads(d.get("steps", "[]"))
            d["emails_list"] = json.loads(d.get("emails", "[]"))
            result.append(d)
        return result

    def advance_enrollment(self, enrollment_id: int) -> None:
        conn = self._conn()
        row = conn.execute(
            "SELECT e.*, s.steps FROM enrollments e JOIN sequences s ON s.id=e.sequence_id WHERE e.id=?",
            (enrollment_id,),
        ).fetchone()
        if not row:
            return
        steps = json.loads(row["steps"])
        next_idx = row["step_index"] + 1
        now = self._now()
        if next_idx >= len(steps):
            conn.execute("UPDATE enrollments SET status='completed', updated_at=? WHERE id=?", (now, enrollment_id))
        else:
            from datetime import timedelta
            delay_days = float(steps[next_idx].get("delay_days", 1))
            next_run = (datetime.utcnow() + timedelta(days=delay_days)).isoformat()
            conn.execute(
                "UPDATE enrollments SET step_index=?, next_run_at=?, updated_at=? WHERE id=?",
                (next_idx, next_run, now, enrollment_id),
            )
        conn.commit()

    def cancel_enrollment(self, enrollment_id: int) -> None:
        conn = self._conn()
        conn.execute("UPDATE enrollments SET status='cancelled', updated_at=? WHERE id=?",
                     (self._now(), enrollment_id))
        conn.commit()


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
    d["tags"]    = json.loads(d.get("tags",    "[]"))
    # Compute score
    score = 0
    if d["emails"]:  score += 30
    if d["phones"]:  score += 20
    if d.get("website"): score += 10
    if d.get("city"):    score += 5
    st = d.get("status", "new")
    score += {"new": 0, "contacted": 10, "warm": 20, "qualified": 35, "rejected": -10}.get(st, 0)
    d["score"] = min(100, score)
    return d


# ─── Backward-compat module-level functions (default project) ─────────────────

_default = Database(DB_PATH)


def upsert(lead: Lead) -> tuple[bool, int]:                      return _default.upsert(lead)
def update_status(lead_id: int, status: str) -> None:            _default.update_status(lead_id, status)
def update_notes(lead_id: int, notes: str) -> None:              _default.update_notes(lead_id, notes)
def delete(lead_id: int) -> None:                                _default.delete(lead_id)
def delete_many(ids: list[int]) -> None:                         _default.delete_many(ids)
def update_last_emailed(lead_id: int) -> None:                   _default.update_last_emailed(lead_id)
def update_last_called(lead_id: int) -> None:                    _default.update_last_called(lead_id)
def log_email(lead_id, subject, to_email, status, error=""):     _default.log_email(lead_id, subject, to_email, status, error)
def log_activity(lead_id, atype, outcome="", subject="", notes=""): return _default.log_activity(lead_id, atype, outcome, subject, notes)
def fetch_activities(**kwargs) -> list[dict]:                    return _default.fetch_activities(**kwargs)
def fetch_all(**kwargs) -> list[dict]:                           return _default.fetch_all(**kwargs)
def fetch_by_id(lead_id: int) -> dict | None:                    return _default.fetch_by_id(lead_id)
def stats() -> dict:                                             return _default.stats()
def exists(key: str) -> bool:                                    return _default.exists(key)
def niches() -> list[str]:                                       return _default.niches()
def cities() -> list[str]:                                       return _default.cities()
def fetch_email_logs(**kwargs) -> list[dict]:                    return _default.fetch_email_logs(**kwargs)
def update_tags(lead_id: int, tags: list[str]) -> None:             _default.update_tags(lead_id, tags)
def update_callback_at(lead_id: int, callback_at: str) -> None:     _default.update_callback_at(lead_id, callback_at)
def bulk_update_status(ids: list[int], status: str) -> int:         return _default.bulk_update_status(ids, status)
def create_open_token(lead_id: int, token: str) -> None:            _default.create_open_token(lead_id, token)
def record_open(token: str) -> int | None:                          return _default.record_open(token)
def dashboard_stats() -> dict:                                       return _default.dashboard_stats()
def update_contact(lead_id: int, contact_name: str, contact_title: str) -> None: _default.update_contact(lead_id, contact_name, contact_title)
def update_company_name(lead_id: int, company_name: str) -> None:                _default.update_company_name(lead_id, company_name)
def update_niche(lead_id: int, niche: str) -> None:                              _default.update_niche(lead_id, niche)
def get_or_create_unsub_token(lead_id: int) -> str:                               return _default.get_or_create_unsub_token(lead_id)
def unsubscribe_by_token(token: str) -> dict | None:                              return _default.unsubscribe_by_token(token)
def fetch_webhooks(event: str = "") -> list[dict]:                                return _default.fetch_webhooks(event)
def save_webhook(url: str, event: str, active: bool = True, webhook_id: int | None = None) -> dict: return _default.save_webhook(url, event, active, webhook_id)
def delete_webhook(webhook_id: int) -> None:                                      _default.delete_webhook(webhook_id)
