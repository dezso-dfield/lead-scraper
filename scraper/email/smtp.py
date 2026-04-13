"""
SMTP email sender — merge tags, random delay, per-lead logging.
"""
from __future__ import annotations
import random
import re
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


def render(text: str, lead: dict) -> str:
    company = lead.get("company_name") or ""
    replacements = {
        "{{company_name}}": company,
        "{{first_name}}":   company.split()[0] if company else "",
        "{{website}}":      lead.get("website") or "",
        "{{email}}":        (lead.get("emails") or [""])[0],
        "{{city}}":         lead.get("city") or "",
        "{{niche}}":        lead.get("niche") or "",
        "{{_pixel_token}}": lead.get("_pixel_token") or "",
        "{{_unsub_url}}":   lead.get("_unsub_url") or "",
        "{{_unsub_token}}": lead.get("_unsub_token") or "",
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


def _build_message(cfg: dict, to_email: str, subj_r: str, body_r: str, lead: dict) -> MIMEMultipart:
    """Build a MIMEMultipart message with proper plain-text and HTML parts.

    Rules:
    - Plain text: raw rendered text, no HTML tags, plain-text unsubscribe line
    - HTML: newlines → <br>, pixel tracking img (HTML only), linked unsubscribe footer
    """
    from_email = cfg.get("from_email", "") or cfg.get("smtp_user", "")
    from_name  = cfg.get("from_name", "")
    add_unsub  = cfg.get("unsubscribe_footer", True)
    base_url   = cfg.get("base_url", "http://localhost:7337").rstrip("/")

    # ── Plain text ──────────────────────────────────────────────────────────
    # Strip any HTML tags the template body may contain (e.g. pixel img)
    plain_body = re.sub(r"<[^>]+>", "", body_r).strip()
    if add_unsub and lead.get("_unsub_url"):
        plain_body += f"\n\n---\nTo unsubscribe: {lead['_unsub_url']}"

    # ── HTML ────────────────────────────────────────────────────────────────
    html_body = body_r.replace("\n", "<br>")
    # Append pixel tracking image (HTML only)
    if lead.get("_pixel_token"):
        html_body += (
            f'<img src="{base_url}/api/t/{lead["_pixel_token"]}.gif"'
            ' width="1" height="1" style="display:none;max-height:0;max-width:0;overflow:hidden">'
        )
    # Append proper unsubscribe link (HTML only)
    if add_unsub and lead.get("_unsub_url"):
        url = lead["_unsub_url"]
        html_body += (
            '<br><br><p style="font-size:11px;color:#94a3b8;margin-top:12px;border-top:1px solid #e2e8f0;padding-top:8px">'
            f'Don\'t want these emails? <a href="{url}" style="color:#6366f1;text-decoration:underline">Unsubscribe here</a>.</p>'
        )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subj_r
    msg["From"]    = f"{from_name} <{from_email}>" if from_name else from_email
    msg["To"]      = to_email
    msg.attach(MIMEText(plain_body, "plain", "utf-8"))
    msg.attach(MIMEText(f"<html><body style='font-family:sans-serif;max-width:600px'>{html_body}</body></html>",
                        "html", "utf-8"))
    return msg


def test_connection(cfg: dict | None = None) -> dict:
    """Attempt to connect and authenticate. Returns {ok, error}."""
    if cfg is None:
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


def send_one(cfg: dict, to_email: str, subject: str, body: str,
             lead: dict | None = None) -> None:
    """Send a single email using the provided cfg dict."""
    host      = cfg.get("smtp_host", "")
    port      = int(cfg.get("smtp_port", 587))
    use_ssl   = cfg.get("smtp_ssl", False)
    use_tls   = cfg.get("smtp_starttls", True)
    user      = cfg.get("smtp_user", "")
    password  = cfg.get("smtp_password", "")
    from_email = cfg.get("from_email", "") or user

    if not host or not from_email:
        raise ValueError("SMTP not configured")

    _lead = lead or {}
    subj_r = render(subject, _lead)
    body_r = render(body, _lead)
    msg = _build_message(cfg, to_email, subj_r, body_r, _lead)

    conn = _build_conn(host, port, use_ssl, use_tls)
    if user and password:
        conn.login(user, password)
    from_addr = cfg.get("from_email", "") or user
    conn.sendmail(from_addr, [to_email], msg.as_string())
    conn.quit()


def send_campaign(
    leads: list[dict],
    subject: str,
    body: str,
    stop_flag: list[bool],
    on_progress: Callable[[dict], None] | None = None,
    cfg: dict | None = None,
) -> list[dict]:
    """
    Send `subject`/`body` to the primary email of each lead.
    `cfg` should be loaded with the active project_id for correct from_name/SMTP.
    Returns list of result dicts: {lead_id, status, error, to_email}.
    """
    if cfg is None:
        cfg = load_settings()

    host       = cfg.get("smtp_host", "")
    port       = int(cfg.get("smtp_port", 587))
    use_ssl    = cfg.get("smtp_ssl", False)
    use_tls    = cfg.get("smtp_starttls", True)
    user       = cfg.get("smtp_user", "")
    password   = cfg.get("smtp_password", "")
    from_email = cfg.get("from_email", "") or user
    delay_min  = float(cfg.get("delay_min", 5))
    delay_max  = float(cfg.get("delay_max", 15))

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
            msg = _build_message(cfg, to_email, subj_r, body_r, lead)

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
                # Reconnect on error
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
