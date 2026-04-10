"""
Email validation via MX record check.
No external API needed — uses DNS lookup to verify the domain can receive email.
"""
from __future__ import annotations
import re
import socket
from functools import lru_cache


EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")


def is_valid_format(email: str) -> bool:
    return bool(EMAIL_RE.match(email.strip()))


@lru_cache(maxsize=512)
def has_mx_record(domain: str) -> bool:
    """Return True if domain has an MX record (can receive email)."""
    try:
        import dns.resolver
        answers = dns.resolver.resolve(domain, "MX", lifetime=5)
        return len(answers) > 0
    except Exception:
        pass
    # Fallback: try A record
    try:
        socket.getaddrinfo(domain, None)
        return True
    except socket.gaierror:
        return False


def validate_email(email: str) -> dict:
    """
    Validate a single email address.
    Returns: {email, valid, reason}
    """
    email = email.strip().lower()
    if not is_valid_format(email):
        return {"email": email, "valid": False, "reason": "invalid_format"}

    domain = email.split("@")[1]

    # Known disposable/spam domains
    DISPOSABLE = {"mailinator.com", "guerrillamail.com", "tempmail.com",
                  "throwaway.email", "yopmail.com", "sharklasers.com",
                  "guerrillamailblock.com", "grr.la", "trashmail.com"}
    if domain in DISPOSABLE:
        return {"email": email, "valid": False, "reason": "disposable"}

    if not has_mx_record(domain):
        return {"email": email, "valid": False, "reason": "no_mx_record"}

    return {"email": email, "valid": True, "reason": "ok"}


def validate_emails_bulk(emails: list[str]) -> list[dict]:
    """Validate a list of email addresses."""
    return [validate_email(e) for e in emails if e.strip()]
