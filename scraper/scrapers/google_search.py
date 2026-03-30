from __future__ import annotations
import time
import random
from urllib.parse import urlparse
from scraper.scrapers.base import BaseScraper
from scraper.models import Lead
from scraper.config import SKIP_DOMAINS, SKIP_EXTENSIONS, NICHE_TRANSLATIONS, HUNGARIAN_INDICATORS


def _is_valid_url(url: str) -> bool:
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return False
    domain = parsed.netloc.lower().removeprefix("www.")
    if any(sd in domain for sd in SKIP_DOMAINS):
        return False
    if any(url.lower().endswith(ext) for ext in SKIP_EXTENSIONS):
        return False
    return True


def _expand_queries(niche: str, location: str) -> list[str]:
    queries = [
        f"{niche} {location}",
        f"{niche} {location} contact email",
        f'"{niche}" {location}',
    ]

    # Add TLD filter for Hungarian queries
    loc_lower = location.lower()
    if any(ind in loc_lower for ind in HUNGARIAN_INDICATORS):
        queries.append(f"{niche} {location} site:.hu")

    # Add native language variants
    niche_lower = niche.lower()
    for key, translations in NICHE_TRANSLATIONS.items():
        if key in niche_lower:
            for t in translations[:2]:
                queries.append(f"{t} {location}")
            break

    return queries


class GoogleSearchScraper(BaseScraper):
    name = "google_search"

    def __init__(self, serp_api_key: str | None = None):
        super().__init__()
        self.serp_api_key = serp_api_key

    def search(self, query: str, location: str, max_results: int = 50) -> list[Lead]:
        leads: list[Lead] = []
        seen_domains: set[str] = set()
        queries = _expand_queries(query, location)

        for q in queries:
            if len(leads) >= max_results:
                break
            if self.serp_api_key:
                new = self._search_serpapi(q, max_results - len(leads))
            else:
                new = self._search_free(q, max_results - len(leads))

            for lead in new:
                domain = urlparse(lead.website).netloc.lower().removeprefix("www.")
                if domain and domain not in seen_domains:
                    seen_domains.add(domain)
                    leads.append(lead)

        return leads

    def _search_free(self, query: str, max_results: int) -> list[Lead]:
        try:
            from googlesearch import search
        except ImportError:
            return []

        leads = []
        try:
            for url in search(query, num_results=min(max_results, 30), sleep_interval=random.uniform(8, 15)):
                if _is_valid_url(url):
                    leads.append(Lead(
                        website=url,
                        niche=query,
                        sources=[self.name],
                    ))
        except Exception:
            pass
        return leads

    def _search_serpapi(self, query: str, max_results: int) -> list[Lead]:
        leads = []
        params = {
            "q": query,
            "num": min(max_results, 100),
            "api_key": self.serp_api_key,
        }
        try:
            import httpx
            resp = httpx.get("https://serpapi.com/search.json", params=params, timeout=30)
            data = resp.json()
            for r in data.get("organic_results", []):
                url = r.get("link", "")
                if _is_valid_url(url):
                    leads.append(Lead(
                        company_name=r.get("title", "").split(" - ")[0].strip(),
                        website=url,
                        niche=query,
                        sources=[self.name],
                    ))
            # Also grab local pack results
            for r in data.get("local_results", {}).get("places", []):
                website = r.get("website", "")
                phone = r.get("phone", "")
                lead = Lead(
                    company_name=r.get("title", ""),
                    website=website,
                    address=r.get("address", ""),
                    niche=query,
                    sources=["google_maps_local"],
                )
                if phone:
                    from scraper.extractors.validators import parse_phone
                    p = parse_phone(phone)
                    if p:
                        lead.phones.append(p)
                if website and _is_valid_url(website):
                    leads.append(lead)
        except Exception:
            pass
        return leads
