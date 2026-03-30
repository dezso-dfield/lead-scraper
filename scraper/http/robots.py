from __future__ import annotations
import time
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

_cache: dict[str, tuple[RobotFileParser, float]] = {}
TTL = 3600.0


def can_fetch(url: str, user_agent: str = "*") -> bool:
    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    robots_url = f"{base}/robots.txt"
    now = time.monotonic()

    if robots_url in _cache:
        rp, ts = _cache[robots_url]
        if now - ts < TTL:
            return rp.can_fetch(user_agent, url)

    rp = RobotFileParser()
    rp.set_url(robots_url)
    try:
        rp.read()
    except Exception:
        # If robots.txt is unreachable, allow crawling
        rp = RobotFileParser()
    _cache[robots_url] = (rp, now)
    return rp.can_fetch(user_agent, url)
