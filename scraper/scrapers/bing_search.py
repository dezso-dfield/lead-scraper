from __future__ import annotations
import re
from urllib.parse import urlparse, urlencode, unquote
from bs4 import BeautifulSoup
from scraper.scrapers.base import BaseScraper
from scraper.models import Lead
from scraper.config import SKIP_DOMAINS, SKIP_EXTENSIONS


def _clean_bing_url(raw: str) -> str:
    """Bing wraps URLs in redirects — extract the actual href."""
    # Bing sometimes uses /ck/a?!&&p=... URLs
    m = re.search(r"url=([^&]+)", raw)
    if m:
        return unquote(m.group(1))
    return raw


def _is_valid(url: str) -> bool:
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return False
    domain = parsed.netloc.lower().removeprefix("www.")
    if any(sd in domain for sd in SKIP_DOMAINS):
        return False
    if any(url.lower().endswith(ext) for ext in SKIP_EXTENSIONS):
        return False
    return True


class BingSearchScraper(BaseScraper):
    name = "bing_search"

    def search(self, query: str, location: str, max_results: int = 50) -> list[Lead]:
        full_query = f"{query} {location}"
        leads: list[Lead] = []
        seen: set[str] = set()

        for page in range(0, min(max_results, 150), 50):
            params = urlencode({"q": full_query, "count": 50, "first": page + 1})
            url = f"https://www.bing.com/search?{params}"
            html = self.fetch(url)
            if not html:
                break

            soup = BeautifulSoup(html, "lxml")
            for a in soup.select("li.b_algo h2 > a[href]"):
                href = _clean_bing_url(a["href"])
                if not _is_valid(href):
                    continue
                domain = urlparse(href).netloc.lower().removeprefix("www.")
                if domain in seen:
                    continue
                seen.add(domain)
                leads.append(Lead(
                    company_name=a.get_text(strip=True).split(" - ")[0],
                    website=href,
                    niche=query,
                    sources=[self.name],
                ))
                if len(leads) >= max_results:
                    return leads

        return leads
