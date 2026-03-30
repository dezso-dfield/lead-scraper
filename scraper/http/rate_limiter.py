from __future__ import annotations
import time
import threading
from urllib.parse import urlparse


class TokenBucketRateLimiter:
    def __init__(self, rate: float, capacity: float):
        self._rate = rate
        self._capacity = capacity
        self._tokens = capacity
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> float:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
            self._last = now
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return 0.0
            wait = (1.0 - self._tokens) / self._rate
            self._tokens = 0.0
            return wait


# Per-domain rate configs: (rate req/s, burst capacity)
DOMAIN_RATES: dict[str, tuple[float, float]] = {
    "google.com": (1 / 15, 1),
    "www.google.com": (1 / 15, 1),
    "bing.com": (0.2, 2),
    "www.bing.com": (0.2, 2),
    "serpapi.com": (1.0, 5),
    "maps.google.com": (1 / 10, 1),
    "europages.com": (0.25, 3),
    "www.europages.com": (0.25, 3),
    "firmania.hu": (0.33, 3),
    "goldenpages.hu": (0.33, 3),
    "arany.hu": (0.33, 3),
}

DEFAULT_RATE = (0.5, 4)

_limiters: dict[str, TokenBucketRateLimiter] = {}
_lock = threading.Lock()


def get_limiter(url: str) -> TokenBucketRateLimiter:
    domain = urlparse(url).netloc.lower()
    with _lock:
        if domain not in _limiters:
            rate, capacity = DOMAIN_RATES.get(domain, DEFAULT_RATE)
            _limiters[domain] = TokenBucketRateLimiter(rate, capacity)
        return _limiters[domain]


def throttle(url: str, extra_jitter: float = 0.3) -> None:
    import random
    limiter = get_limiter(url)
    wait = limiter.acquire()
    if wait > 0:
        time.sleep(wait + random.uniform(0, extra_jitter))
    elif extra_jitter > 0:
        time.sleep(random.uniform(0.1, extra_jitter))
