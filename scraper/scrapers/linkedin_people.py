"""
LinkedIn people scraper — finds decision-maker names + titles via DDG.
Attaches contact_name / contact_title to existing leads by domain match.
"""
from __future__ import annotations
import re
import time
import random
from typing import Callable
from urllib.parse import urlparse


def search_linkedin_people(
    niche: str,
    location: str,
    on_progress: Callable[[str], None] | None = None,
    max_results: int = 40,
) -> list[dict]:
    """
    Returns list of dicts: {domain, contact_name, contact_title, linkedin_url}
    These are matched against existing leads by domain in server.py.
    """
    def emit(msg: str) -> None:
        if on_progress:
            on_progress(msg)

    try:
        from ddgs import DDGS
    except ImportError:
        emit("  [linkedin] ddgs not installed")
        return []

    city    = location.split(",")[0].strip()
    country = location.split(",")[-1].strip() if "," in location else location

    queries = [
        f'site:linkedin.com/in "{niche}" {city} owner OR founder OR CEO OR director',
        f'site:linkedin.com/in "{niche}" {country} managing director OR tulajdonos OR CEO',
        f'site:linkedin.com/company "{niche}" {city}',
    ]

    results_out: list[dict] = []
    seen_urls: set[str] = set()

    with DDGS() as ddgs:
        for q in queries:
            if len(results_out) >= max_results:
                break
            emit(f"  LinkedIn: {q[:60]}…")
            try:
                results = list(ddgs.text(q, max_results=15))
                time.sleep(random.uniform(0.6, 1.2))
            except Exception:
                continue

            for r in results:
                url   = r.get("href", "")
                title = r.get("title", "")
                body  = r.get("body", "")

                if not url or url in seen_urls:
                    continue
                if "linkedin.com" not in url:
                    continue
                seen_urls.add(url)

                # Parse name and title from LinkedIn snippet
                name, job_title = _parse_linkedin_snippet(title, body)
                if not name:
                    continue

                # Try to find the company domain from the snippet
                domain = _extract_company_domain(body)

                results_out.append({
                    "contact_name":  name,
                    "contact_title": job_title,
                    "linkedin_url":  url,
                    "domain":        domain,
                })

    emit(f"  → LinkedIn: {len(results_out)} people found")
    return results_out


def _parse_linkedin_snippet(title: str, body: str) -> tuple[str, str]:
    """Extract person name and job title from LinkedIn search result."""
    # Title format: "Name - Title at Company | LinkedIn"
    name = ""
    job_title = ""

    title_clean = re.sub(r"\s*\|\s*LinkedIn.*$", "", title).strip()
    if " - " in title_clean:
        parts = title_clean.split(" - ", 1)
        name      = parts[0].strip()
        job_title = parts[1].split(" at ")[0].strip()
    elif title_clean:
        name = title_clean.split(" – ")[0].strip()

    # Validate: name should look like a real name (2 words, mostly alpha)
    if name and not re.match(r"^[A-Za-záéíóöőúüűÁÉÍÓÖŐÚÜŰ\s\-\.]{3,50}$", name):
        name = ""

    return name[:60], job_title[:80]


def _extract_company_domain(body: str) -> str:
    """Try to find a company website domain mentioned in the snippet."""
    urls = re.findall(r"(?:https?://)?(?:www\.)?([a-zA-Z0-9\-]+\.[a-zA-Z]{2,})", body)
    for u in urls:
        if "linkedin" not in u and "google" not in u and "facebook" not in u:
            return u.lower()
    return ""
