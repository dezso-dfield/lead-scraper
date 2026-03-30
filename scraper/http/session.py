from __future__ import annotations
import random
import httpx
from urllib.parse import urlparse
from scraper.config import USER_AGENTS

# Brotli (`br`) omitted intentionally — not installed, causes garbled responses
BROWSER_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "hu-HU,hu;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# Google Maps consent cookie (bypasses the consent wall)
GOOGLE_COOKIES = {
    "CONSENT": "YES+cb",
    "SOCS": "CAESEwgDEgk0ODA3Nzk3MjkaAmh1IAEaBgiA_LyoBg",
}

# One UA per domain-session
_domain_ua: dict[str, str] = {}


def get_ua(domain: str) -> str:
    if domain not in _domain_ua:
        _domain_ua[domain] = random.choice(USER_AGENTS)
    return _domain_ua[domain]


def make_client(timeout: int = 20, cookies: dict | None = None) -> httpx.Client:
    return httpx.Client(
        timeout=timeout,
        follow_redirects=True,
        headers=BROWSER_HEADERS,
        cookies=cookies or {},
        verify=False,  # some .hu sites have cert issues
    )


_client: httpx.Client | None = None


def get_client() -> httpx.Client:
    global _client
    if _client is None or _client.is_closed:
        _client = make_client()
    return _client


def fetch(url: str, client: httpx.Client | None = None, extra_headers: dict | None = None) -> httpx.Response | None:
    from scraper.http.rate_limiter import throttle
    throttle(url)
    c = client or get_client()
    domain = urlparse(url).netloc.lower()
    headers = {"User-Agent": get_ua(domain)}
    if extra_headers:
        headers.update(extra_headers)
    try:
        resp = c.get(url, headers=headers)
        if resp.status_code in (403, 429, 503):
            return None
        resp.raise_for_status()
        return resp
    except Exception:
        return None
