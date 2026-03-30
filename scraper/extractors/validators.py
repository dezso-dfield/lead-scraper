from __future__ import annotations
import re
import phonenumbers
from phonenumbers import PhoneNumberFormat

EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", re.IGNORECASE)

# Emails that are clearly not real leads
JUNK_EMAIL_PREFIXES = frozenset({
    "noreply", "no-reply", "donotreply", "mailer-daemon",
    "bounce", "postmaster", "webmaster", "admin@localhost",
    "support@sentry", "error@", "test@",
})

# TLDs that are actually file extensions (false positives from src="img@2x.gif" etc.)
FAKE_TLDS = frozenset({
    "gif", "png", "jpg", "jpeg", "svg", "webp", "ico", "bmp",
    "css", "js", "ts", "jsx", "tsx", "json", "xml", "csv",
    "pdf", "doc", "docx", "xls", "xlsx", "zip", "tar", "gz",
    "woff", "woff2", "ttf", "eot", "otf", "mp4", "mp3", "avi",
})


# Known real TLDs (keep this minimal — just block obviously fake ones)
VALID_TLD_PATTERN = re.compile(r"^[a-z]{2,}$")

# Common real TLDs for quick acceptance
KNOWN_TLDS = frozenset({
    "com", "net", "org", "hu", "de", "fr", "it", "es", "pl", "cz", "sk", "ro",
    "at", "ch", "nl", "be", "se", "dk", "no", "fi", "pt", "ie", "gr", "hr",
    "eu", "io", "co", "biz", "info", "online", "email", "agency", "studio",
    "events", "solutions", "tech", "digital", "media", "group", "pro",
    "uk", "us", "ca", "au", "nz",
})


def is_valid_email(email: str) -> bool:
    email = email.lower().strip()
    if not EMAIL_REGEX.fullmatch(email):
        return False
    if "@" not in email:
        return False
    local, domain = email.rsplit("@", 1)
    if "." not in domain:
        return False
    tld = domain.rsplit(".", 1)[-1]
    # Reject obvious file extensions
    if tld in FAKE_TLDS:
        return False
    # TLD must look like a real TLD (2-12 alpha chars)
    if not re.match(r"^[a-z]{2,12}$", tld):
        return False
    # For unknown TLDs, be stricter — reject if they look like English words > 6 chars
    # that aren't in known TLDs (catches things like .the, .and, .request)
    if tld not in KNOWN_TLDS and len(tld) > 6:
        return False
    # Short nonsense TLDs that slip through
    if tld in {"the", "and", "for", "with", "this", "that", "from", "your", "have"}:
        return False
    if any(email.startswith(p) for p in JUNK_EMAIL_PREFIXES):
        return False
    if len(local) < 2 or len(domain) < 4:
        return False
    if "/" in local or "\\" in local:
        return False
    if re.match(r"^\d+x?$", local):
        return False
    return True


def normalize_email(email: str) -> str:
    return email.lower().strip().split("?")[0]


def parse_phone(raw: str, default_region: str = "HU") -> str | None:
    """Return E.164 formatted phone or None if invalid."""
    try:
        p = phonenumbers.parse(raw, default_region)
        if phonenumbers.is_valid_number(p):
            return phonenumbers.format_number(p, PhoneNumberFormat.E164)
    except Exception:
        pass
    return None


def format_phone_display(e164: str) -> str:
    """Format E.164 number for display."""
    try:
        p = phonenumbers.parse(e164, None)
        return phonenumbers.format_number(p, PhoneNumberFormat.INTERNATIONAL)
    except Exception:
        return e164
