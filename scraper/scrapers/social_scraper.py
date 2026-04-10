"""
Social media lead scraper.
Finds business Facebook pages, LinkedIn company pages, and Instagram business
profiles using DuckDuckGo site: searches (avoids direct scraping restrictions).
"""
from __future__ import annotations
import re
import time
import random
from typing import Callable
from urllib.parse import urlparse

from scraper.models import Lead


# Social platforms and their URL patterns
SOCIAL_PLATFORMS = [
    {
        "name": "facebook",
        "queries": [
            "site:facebook.com/pages {niche} {city}",
            "site:facebook.com \"{niche}\" {city} contact",
        ],
        "url_pattern": r"facebook\.com/(pages/[^/]+/\d+|[^/]+)",
        "source_tag": "facebook",
    },
    {
        "name": "linkedin",
        "queries": [
            "site:linkedin.com/company {niche} {city}",
            "site:linkedin.com/company \"{niche}\" {country}",
        ],
        "url_pattern": r"linkedin\.com/company/[^/?]+",
        "source_tag": "linkedin",
    },
    {
        "name": "instagram",
        "queries": [
            "site:instagram.com {niche} {city} contact",
        ],
        "url_pattern": r"instagram\.com/[^/?]+",
        "source_tag": "instagram",
    },
]


def _clean_social_name(title: str, url: str) -> str:
    """Extract a clean business name from a social profile title."""
    # Remove common suffixes
    for suffix in [" | Facebook", " - Facebook", " (@", " on Instagram", " | LinkedIn", " | Company"]:
        title = title.split(suffix)[0]
    # Remove trailing pipes and dashes
    title = re.sub(r"[\|–—-]\s*$", "", title).strip()
    # Fallback to domain slug
    if not title or len(title) < 2:
        path = urlparse(url).path.strip("/").split("/")[-1]
        title = path.replace("-", " ").replace("_", " ").title()
    return title[:80]


def _is_profile_url(url: str, platform: dict) -> bool:
    """Check if a URL looks like a real social profile (not a search/tag page)."""
    parsed = urlparse(url)
    path = parsed.path.strip("/")
    # Reject feed/search/tag/hashtag pages
    skip_paths = {"search", "hashtag", "explore", "reel", "stories", "groups",
                  "events", "marketplace", "watch", "pages/search", "home", "login"}
    parts = set(path.lower().split("/"))
    if parts & skip_paths:
        return False
    if re.search(platform["url_pattern"], url):
        return True
    return False


def search_social(
    niche: str,
    location: str,
    on_progress: Callable[[str], None] | None = None,
) -> list[Lead]:
    """
    Search social media platforms for business leads.
    Returns Lead stubs with social profile URLs as websites.
    """
    def emit(msg: str) -> None:
        if on_progress:
            on_progress(msg)

    city = location.split(",")[0].strip()
    country = location.split(",")[-1].strip() if "," in location else location

    leads: list[Lead] = []
    seen_domains: set[str] = set()

    try:
        from ddgs import DDGS
    except ImportError:
        emit("  [social] ddgs not installed, skipping social search")
        return []

    with DDGS() as ddgs:
        for platform in SOCIAL_PLATFORMS:
            emit(f"  Searching {platform['name']} for {niche} in {city}…")
            found = 0

            for query_tpl in platform["queries"]:
                q = query_tpl.format(niche=niche, city=city, country=country)
                try:
                    results = list(ddgs.text(q, max_results=15))
                    time.sleep(random.uniform(0.5, 1.2))
                except Exception:
                    continue

                for r in results:
                    url = r.get("href", "")
                    title = r.get("title", "")
                    if not url or not _is_profile_url(url, platform):
                        continue
                    domain = urlparse(url).netloc + urlparse(url).path
                    if domain in seen_domains:
                        continue
                    seen_domains.add(domain)

                    name = _clean_social_name(title, url)
                    body = r.get("body", "")

                    # Try to extract email from snippet
                    emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", body)
                    phones = re.findall(r"(?:\+36|06)?[\s\-]?\d{1,2}[\s\-]?\d{3,4}[\s\-]?\d{4}", body)

                    lead = Lead(
                        company_name=name,
                        website=url.split("?")[0].rstrip("/"),
                        niche=niche,
                        emails=list(set(emails)),
                        phones=[p.strip() for p in phones],
                        sources=[platform["source_tag"]],
                        city=city,
                        country=country,
                    )
                    leads.append(lead)
                    found += 1

            if found:
                emit(f"  → +{found} {platform['name']} profiles found")

    emit(f"Social search complete: {len(leads)} profiles found")
    return leads
