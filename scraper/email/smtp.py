"""
SMTP email sender — merge tags, random delay, per-lead logging.
"""
from __future__ import annotations
import random
import smtplib
import ssl
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Callable

from scraper.settings import load as load_settings

MERGE_TAGS = [
    "{{company_name}}", "{{first_name}}", "{{website}}",
    "{{email}}", "{{city}}", "{{niche}}",
]

UNSUBSCRIBE_FOOTER = "\n\n---\nTo unsubscribe reply with 'unsubscribe'."


def render(text: str, lead: dict) -> str:
    company = lead.get("company_name") or ""
    replacements = {
        "{{company_name}}": company,
        "{{first_name}}":   company.split()[0] if company else "",
        "{{website}}":      lead.get("website") or "",
        "{{email}}":        (lead.get("emails") or [""])[0],
        "{{city}}":         lead.get("city") or "",
        "{{niche}}":        lead.get("niche") or "",
    }
    for tag, val in replacements.items():
        text = text.replace(tag, val)
    return text


def _build_conn(host: str, port: int, use_ssl: bool, use_starttls: bool) -> smtplib.SMTP:
    ctx = ssl.create_default_context()
    if use_ssl:
        return smtplib.SMTP_SSL(host, port, context=ctx, timeout=20)
    s = smtplib.SMTP(host, port, timeout=20)
    if use_starttls:
        s.starttls(context=ctx)
    return s


def test_connection() -> dict:
    """Attempt to connect and authenticate. Returns {ok, error}."""
    cfg = load_settings()
    host = cfg.get("smtp_host", "")
    if not host:
        return {"ok": False, "error": "SMTP host not configured"}
    try:
        conn = _build_conn(
            host, int(cfg.get("smtp_port", 587)),
            cfg.get("smtp_ssl", False), cfg.get("smtp_starttls", True),
        )
        user = cfg.get("smtp_user", "")
        pwd  = cfg.get("smtp_password", "")
        if user and pwd:
            conn.login(user, pwd)
        conn.quit()
        return {"ok": True, "error": ""}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def send_campaign(
    leads: list[dict],
    subject: str,
    body: str,
    stop_flag: list[bool],
    on_progress: Callable[[dict], None] | None = None,
) -> list[dict]:
    """
    Send `subject`/`body` to the primary email of each lead.
    Returns list of result dicts: {lead_id, status, error, to_email}.
    """
    cfg = load_settings()
    host      = cfg.get("smtp_host", "")
    port      = int(cfg.get("smtp_port", 587))
    use_ssl   = cfg.get("smtp_ssl", False)
    use_tls   = cfg.get("smtp_starttls", True)
    user      = cfg.get("smtp_user", "")
    password  = cfg.get("smtp_password", "")
    from_email = cfg.get("from_email", "") or user
    from_name  = cfg.get("from_name", "")
    delay_min  = float(cfg.get("delay_min", 5))
    delay_max  = float(cfg.get("delay_max", 15))
    add_unsub  = cfg.get("unsubscribe_footer", True)

    if not host or not from_email:
        raise ValueError("SMTP not configured — open Settings and fill in SMTP details.")

    results: list[dict] = []

    def _connect():
        conn = _build_conn(host, port, use_ssl, use_tls)
        if user and password:
            conn.login(user, password)
        return conn

    conn = _connect()

    try:
        for i, lead in enumerate(leads):
            if stop_flag[0]:
                break

            to_email = (lead.get("emails") or [None])[0]
            if not to_email:
                results.append({"lead_id": lead["id"], "status": "skipped",
                                 "error": "no email", "to_email": ""})
                if on_progress:
                    on_progress({"type": "skipped", "lead_id": lead["id"],
                                 "company": lead.get("company_name", ""),
                                 "reason": "no email", "index": i + 1, "total": len(leads)})
                continue

            subj_r = render(subject, lead)
            body_r = render(body, lead)
            if add_unsub:
                body_r += UNSUBSCRIBE_FOOTER

            msg = MIMEMultipart("alternative")
            msg["Subject"] = subj_r
            msg["From"]    = f"{from_name} <{from_email}>" if from_name else from_email
            msg["To"]      = to_email
            # HTML version: wrap plain text in minimal HTML
            html_body = body_r.replace("\n", "<br>")
            msg.attach(MIMEText(body_r, "plain", "utf-8"))
            msg.attach(MIMEText(f"<html><body style='font-family:sans-serif'>{html_body}</body></html>",
                                "html", "utf-8"))

            try:
                conn.sendmail(from_email, [to_email], msg.as_string())
                results.append({"lead_id": lead["id"], "status": "sent",
                                 "error": "", "to_email": to_email})
                if on_progress:
                    on_progress({"type": "sent", "lead_id": lead["id"],
                                 "company": lead.get("company_name", ""),
                                 "to": to_email, "subject": subj_r,
                                 "index": i + 1, "total": len(leads)})
            except smtplib.SMTPException as exc:
                err = str(exc)
                results.append({"lead_id": lead["id"], "status": "failed",
                                 "error": err, "to_email": to_email})
                if on_progress:
                    on_progress({"type": "failed", "lead_id": lead["id"],
                                 "company": lead.get("company_name", ""),
                                 "to": to_email, "error": err,
                                 "index": i + 1, "total": len(leads)})
                # Reconnect
                try:
                    conn.quit()
                except Exception:
                    pass
                try:
                    conn = _connect()
                except Exception:
                    pass

            # Random delay (skip after last email)
            if i < len(leads) - 1 and not stop_flag[0]:
                delay = random.uniform(delay_min, delay_max)
                if on_progress:
                    on_progress({"type": "delay", "seconds": round(delay, 1),
                                 "index": i + 1, "total": len(leads)})
                time.sleep(delay)
    finally:
        try:
            conn.quit()
        except Exception:
            pass

    return results
