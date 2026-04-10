"""
IMAP inbox poller — detects replies to sent campaigns.
When a reply is found matching a lead's email, marks the lead as 'warm'
and logs the reply as an activity.
"""
from __future__ import annotations
import email
import imaplib
import threading
import time
from datetime import datetime, timedelta
from email.header import decode_header
from typing import Callable


# Global poller state
_poller_thread: threading.Thread | None = None
_stop_event = threading.Event()
_status: dict = {"running": False, "last_check": "", "replies_found": 0, "error": ""}


def get_status() -> dict:
    return dict(_status)


def start_poller(
    cfg: dict,
    project_db,
    on_reply: Callable[[dict], None] | None = None,
) -> bool:
    """Start the background IMAP polling thread. Returns True if started."""
    global _poller_thread, _stop_event

    if _status["running"]:
        return False

    _stop_event.clear()
    _poller_thread = threading.Thread(
        target=_poll_loop,
        args=(cfg, project_db, on_reply),
        daemon=True,
        name="imap-poller",
    )
    _poller_thread.start()
    _status["running"] = True
    _status["error"] = ""
    return True


def stop_poller() -> None:
    """Signal the poller to stop."""
    global _stop_event
    _stop_event.set()
    _status["running"] = False


def _poll_loop(cfg: dict, project_db, on_reply: Callable | None) -> None:
    interval_mins = int(cfg.get("imap_interval", 10))
    while not _stop_event.is_set():
        try:
            replies = _check_inbox(cfg, project_db)
            _status["last_check"] = datetime.utcnow().isoformat()
            _status["replies_found"] += len(replies)
            for r in replies:
                if on_reply:
                    on_reply(r)
        except Exception as e:
            _status["error"] = str(e)

        # Sleep in small increments so we can stop quickly
        for _ in range(interval_mins * 60 // 5):
            if _stop_event.is_set():
                break
            time.sleep(5)

    _status["running"] = False


def _check_inbox(cfg: dict, project_db) -> list[dict]:
    """Connect to IMAP and scan for replies from known lead emails."""
    host     = cfg.get("imap_host", "")
    port     = int(cfg.get("imap_port", 993))
    user     = cfg.get("imap_user", "") or cfg.get("smtp_user", "")
    password = cfg.get("imap_password", "") or cfg.get("smtp_password", "")
    folder   = cfg.get("imap_folder", "INBOX")
    use_ssl  = cfg.get("imap_ssl", True)

    if not host or not user or not password:
        raise ValueError("IMAP not configured (host/user/password missing)")

    # Connect
    if use_ssl:
        conn = imaplib.IMAP4_SSL(host, port)
    else:
        conn = imaplib.IMAP4(host, port)
    conn.login(user, password)
    conn.select(folder)

    # Search for emails in the last 24 hours
    since = (datetime.utcnow() - timedelta(hours=24)).strftime("%d-%b-%Y")
    _, msg_nums = conn.search(None, f"SINCE {since}")

    found_replies: list[dict] = []
    all_leads = project_db.fetch_all()
    lead_email_map: dict[str, dict] = {}
    for lead in all_leads:
        for em in (lead.get("emails") or []):
            lead_email_map[em.lower().strip()] = lead

    for num in (msg_nums[0].split() if msg_nums[0] else []):
        try:
            _, data = conn.fetch(num, "(RFC822)")
            raw = data[0][1] if data and data[0] else None
            if not raw:
                continue

            msg = email.message_from_bytes(raw)
            from_addr = _decode_header_str(msg.get("From", ""))
            subject   = _decode_header_str(msg.get("Subject", ""))

            # Extract email address from "Name <email>" format
            from_email = _extract_email(from_addr)
            if not from_email:
                continue

            # Check if it's from a known lead
            lead = lead_email_map.get(from_email.lower())
            if not lead:
                continue

            # Don't re-log if already logged recently
            body = _get_body(msg)
            lead_id = lead["id"]

            # Update lead status to warm (if not already qualified/rejected)
            if lead.get("status") not in ("qualified", "rejected"):
                project_db.update_status(lead_id, "warm")

            # Log the reply as an activity
            project_db.log_activity(
                lead_id, "email_reply",
                outcome="replied",
                subject=subject[:200],
                notes=body[:500],
            )

            found_replies.append({
                "lead_id":    lead_id,
                "company":    lead.get("company_name", ""),
                "from_email": from_email,
                "subject":    subject,
            })

        except Exception:
            continue

    conn.logout()
    return found_replies


def _decode_header_str(value: str) -> str:
    parts = decode_header(value)
    result = []
    for part, enc in parts:
        if isinstance(part, bytes):
            result.append(part.decode(enc or "utf-8", errors="replace"))
        else:
            result.append(str(part))
    return " ".join(result)


def _extract_email(header_val: str) -> str:
    import re
    m = re.search(r"<([^>]+)>", header_val)
    if m:
        return m.group(1).strip()
    m = re.search(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", header_val)
    return m.group(0).strip() if m else ""


def _get_body(msg) -> str:
    """Extract plain text body from email message."""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                charset = part.get_content_charset() or "utf-8"
                try:
                    return part.get_payload(decode=True).decode(charset, errors="replace")[:1000]
                except Exception:
                    pass
    else:
        if msg.get_content_type() == "text/plain":
            charset = msg.get_content_charset() or "utf-8"
            try:
                return msg.get_payload(decode=True).decode(charset, errors="replace")[:1000]
            except Exception:
                pass
    return ""
