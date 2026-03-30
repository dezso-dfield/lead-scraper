from __future__ import annotations
import time
import random
import re
from urllib.parse import urlparse
from scraper.models import Lead
from scraper.config import SKIP_DOMAINS, SKIP_EXTENSIONS, NICHE_TRANSLATIONS, HUNGARIAN_INDICATORS

# Additional skip patterns for DDG (news, sports, jobs, random articles)
SKIP_PATH_PATTERNS = re.compile(
    r"/\d{4}/\d{2}/\d{2}/"   # date-based news URLs
    r"|/news/|/hirek?/|/cikk/"
    r"|/jobs?/|/allas/|/allashirdetes/"
    r"|/privacy|/terms|/terms-of-use|/adatvedelem"
    r"|/cookie|/impressum$",
    re.IGNORECASE,
)

SKIP_TITLE_PATTERNS = re.compile(
    r"^\d|"           # starts with number (sports scores, dates)
    r"állás|job offer|job listing|privacy policy|terms of|"
    r"cookie policy|adatvédelm|gdpr|404|not found|"
    r"temporary email|free email|freemail|disposable|"
    r"fesztivál|festival|concert|előadás|jegy|ticket|"
    r"félév|tanév|döntő|bajnokság|verseny|liga|kupa\b",
    re.IGNORECASE,
)

# Generic page titles that should trigger domain-based name fallback
GENERIC_TITLES = frozenset({
    "kapcsolat", "contact", "contact us", "elérhetőség", "főoldal", "home",
    "main page", "welcome", "kezdőlap", "about", "rólunk", "impressum",
    "mel", "oldal", "page", "untitled",
})


def _domain_as_name(url: str) -> str:
    domain = urlparse(url).netloc.lower().removeprefix("www.")
    return domain.split(".")[0].replace("-", " ").title()


def _clean_company_name(title: str, url: str) -> str:
    """Extract a clean company name from page title and URL."""
    # Try splitting on common separators — shortest non-generic part wins
    for sep in [" - ", " | ", " – ", " :: ", " • "]:
        parts = title.split(sep)
        candidates = [p.strip() for p in parts if 3 < len(p.strip()) < 60]
        if candidates:
            name = candidates[0]
            if name.lower() in GENERIC_TITLES or len(name) < 3:
                # Try later parts or fall back
                for alt in candidates[1:]:
                    if alt.lower() not in GENERIC_TITLES and len(alt) >= 3:
                        return alt
                return _domain_as_name(url)
            return name

    # No separator — check if the whole title is generic
    clean = title.strip()
    if clean.lower() in GENERIC_TITLES or len(clean) < 3:
        return _domain_as_name(url)

    # Long titles without separators are usually page descriptions, not names
    if len(clean) > 60:
        return _domain_as_name(url)

    return clean


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


def _is_relevant_result(url: str, title: str) -> bool:
    """Filter out news articles, job listings, etc."""
    if SKIP_PATH_PATTERNS.search(url):
        return False
    if SKIP_TITLE_PATTERNS.search(title):
        return False
    return True


def _is_list_page(url: str, title: str) -> bool:
    """Detect 'top 10 companies' style pages that link to many competitors."""
    url_l = url.lower()
    title_l = title.lower()
    list_signals = ["top", "legjobb", "best", "list", "lista", "cégek", "cegek", "companies", "directory"]
    return any(s in url_l or s in title_l for s in list_signals)


def _expand_queries(niche: str, location: str) -> list[str]:
    queries = [
        f"{niche} {location} email",
        f"{niche} {location} kapcsolat",
        f"{niche} {location}",
    ]

    loc_lower = location.lower()
    is_hungarian = any(ind in loc_lower for ind in HUNGARIAN_INDICATORS)

    if is_hungarian:
        queries.append(f"{niche} {location} site:.hu")
        queries.append(f"{niche} Budapest Kft elérhetőség")

    # Add native language variants
    niche_lower = niche.lower()
    for key, translations in NICHE_TRANSLATIONS.items():
        if key in niche_lower:
            for t in translations[:2]:
                queries.insert(0, f"{t} {location} email elérhetőség")
            break

    return queries


class DDGSearchScraper:
    name = "ddg_search"

    def search(self, query: str, location: str, max_results: int = 60) -> list[Lead]:
        try:
            from ddgs import DDGS
        except ImportError:
            return []

        leads: list[Lead] = []
        list_page_urls: list[tuple[str, str]] = []  # (url, title) for list pages
        seen_domains: set[str] = set()
        queries = _expand_queries(query, location)

        with DDGS() as ddgs:
            for q in queries:
                if len(leads) >= max_results:
                    break
                try:
                    results = list(ddgs.text(q, max_results=min(20, max_results - len(leads))))
                    time.sleep(random.uniform(0.5, 1.5))
                except Exception:
                    continue

                for r in results:
                    url = r.get("href", "")
                    title = r.get("title", "")

                    if not _is_valid_url(url):
                        continue
                    if not _is_relevant_result(url, title):
                        continue

                    domain = urlparse(url).netloc.lower().removeprefix("www.")
                    if domain in seen_domains:
                        continue

                    # Track list pages separately — we'll mine them for more companies
                    if _is_list_page(url, title):
                        if url not in [lp[0] for lp in list_page_urls]:
                            list_page_urls.append((url, title))
                        # Add the list page's own domain too (it may be a company)
                        seen_domains.add(domain)
                        continue

                    seen_domains.add(domain)
                    leads.append(Lead(
                        company_name=_clean_company_name(title, url),
                        website=url.split("?")[0].rstrip("/"),
                        niche=query,
                        sources=[self.name],
                    ))

        # Mine list pages for more company links
        if list_page_urls:
            list_leads = self._mine_list_pages(list_page_urls, seen_domains, query)
            leads.extend(list_leads[:max(0, max_results - len(leads))])

        return leads[:max_results]

    def _mine_list_pages(self, pages: list[tuple[str, str]], seen: set[str], query: str) -> list[Lead]:
        from scraper.http.session import fetch, get_client
        from bs4 import BeautifulSoup

        leads = []
        client = get_client()

        for url, _ in pages[:4]:  # limit to 4 list pages
            resp = fetch(url, client=client)
            if not resp:
                continue
            soup = BeautifulSoup(resp.text, "lxml")

            # Extract all external links
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if not href.startswith("http"):
                    continue
                if not _is_valid_url(href):
                    continue
                domain = urlparse(href).netloc.lower().removeprefix("www.")
                if domain in seen:
                    continue
                seen.add(domain)
                link_text = a.get_text(strip=True)
                leads.append(Lead(
                    company_name=link_text[:60] if link_text else "",
                    website=href.split("?")[0].rstrip("/"),
                    niche=query,
                    sources=[self.name + "_list"],
                ))

        return leads
