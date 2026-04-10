"""
Deep multi-round searcher.

Round 1: Direct queries (DDG + Bing)
Round 2: Autocomplete-expanded queries (real search patterns from DDG suggest)
Round 3: Synonym/translation variants
Round 4: Hungarian-specific + site:.hu
Round 5: List-page mining (top-10 style articles)

Each round feeds URLs into the enrichment queue and also seeds new queries
for subsequent rounds.
"""
from __future__ import annotations
import re
import time
import random
import threading
from urllib.parse import urlparse, urlencode
from typing import Callable

from scraper.models import Lead
from scraper.config import SKIP_DOMAINS, SKIP_EXTENSIONS, NICHE_TRANSLATIONS, HUNGARIAN_INDICATORS
from scraper.scrapers.ddg_search import DDGSearchScraper, _is_valid_url, _is_relevant_result, _is_list_page, _clean_company_name, _domain_as_name


# ─── Autocomplete / suggest ──────────────────────────────────────────────────

def _fetch_autocomplete(seed: str) -> list[str]:
    """
    Fetch DuckDuckGo autocomplete suggestions for `seed`.
    Returns up to 8 suggestion strings that real users actually search for.
    """
    try:
        import httpx
        url = "https://duckduckgo.com/ac/?" + urlencode({"q": seed, "type": "list", "kl": "wt-wt"})
        resp = httpx.get(url, timeout=5, headers={"User-Agent": "Mozilla/5.0"})
        data = resp.json()
        # Format: ["seed", ["sug1", "sug2", ...], ...]
        if isinstance(data, list) and len(data) >= 2 and isinstance(data[1], list):
            return [s for s in data[1] if isinstance(s, str) and s != seed][:8]
    except Exception:
        pass
    return []


# ─── Query generation ────────────────────────────────────────────────────────

def generate_queries(niche: str, location: str, use_autocomplete: bool = True) -> list[str]:
    """Return queries ordered from most specific to broadest."""
    city = location.split(",")[0].strip()
    country = location.split(",")[-1].strip() if "," in location else location

    is_hungarian = any(ind in location.lower() for ind in HUNGARIAN_INDICATORS)

    queries: list[str] = []

    # Round 1: Direct + contact signals
    queries += [
        f"{niche} {city} email elérhetőség",
        f"{niche} {city} kapcsolat",
        f"{niche} {city}",
        f"{niche} {city} Kft",
    ]

    # Round 2: Autocomplete — what real people search for
    if use_autocomplete:
        seeds = [
            f"{niche} {city}",
            f"{niche} {country}",
        ]
        for seed in seeds:
            for suggestion in _fetch_autocomplete(seed):
                # Only keep suggestions that look like business searches
                low = suggestion.lower()
                if any(skip in low for skip in ["wiki", "youtube", "facebook", "instagram", "reddit", "news"]):
                    continue
                queries.append(suggestion)
            time.sleep(0.2)

    # Round 3: Translations / synonyms
    niche_lower = niche.lower()
    translated: list[str] = []
    for key, variants in NICHE_TRANSLATIONS.items():
        if key in niche_lower:
            translated = variants
            break
    # Also check if niche IS a Hungarian word → add English equivalents
    if not translated:
        for key, variants in NICHE_TRANSLATIONS.items():
            if niche_lower in key or niche_lower in " ".join(v.lower() for v in variants):
                translated = variants
                break

    for t in translated[:3]:
        queries += [
            f"{t} {city} email",
            f"{t} {city}",
        ]

    # Round 4: TLD/country specific
    if is_hungarian:
        queries += [
            f"{niche} Budapest site:.hu",
            f"{niche} Magyarország elérhetőség",
            f'rendezvényszervező cégek {city} lista',
        ]

    # Round 5: Broader country search
    queries += [
        f"{niche} {country}",
        f'"{niche}" {city} cégek',
    ]

    # Deduplicate preserving order
    seen: set[str] = set()
    result = []
    for q in queries:
        if q not in seen:
            seen.add(q)
            result.append(q)
    return result


# ─── Deep DDG search ─────────────────────────────────────────────────────────

class DeepSearcher:
    """
    Runs multiple rounds of search, mines list pages, and does company-name
    targeted searches on discovered leads to find more contact info.
    """

    def __init__(
        self,
        niche: str,
        location: str,
        max_leads: int = 200,
        on_progress: Callable[[str], None] | None = None,
        extra_queries: list[str] | None = None,
    ):
        self.niche = niche
        self.location = location
        self.max_leads = max_leads
        self.on_progress = on_progress
        self._extra_queries = extra_queries or []
        self._seen_domains: set[str] = set()
        self._seen_queries: set[str] = set()
        self._list_pages: list[str] = []
        self._leads: list[Lead] = []
        self._lock = threading.Lock()

    def _emit(self, msg: str) -> None:
        if self.on_progress:
            self.on_progress(msg)

    def run(self) -> list[Lead]:
        self._emit("Fetching search suggestions from autocomplete…")
        queries = generate_queries(self.niche, self.location, use_autocomplete=True)
        # Merge AI-generated extra queries (deduplicated)
        seen_q: set[str] = set(queries)
        for q in self._extra_queries:
            if q not in seen_q:
                queries.append(q)
                seen_q.add(q)
        self._emit(f"Generated {len(queries)} search queries (incl. autocomplete suggestions)")

        # Rounds 1-4: DDG text search
        self._ddg_rounds(queries)

        # Round 5: Mine list pages collected during DDG rounds
        if self._list_pages:
            self._emit(f"Mining {len(self._list_pages)} list pages…")
            self._mine_list_pages()

        # Round 6: Bing fallback for whatever DDG missed
        if len(self._leads) < self.max_leads // 2:
            self._emit("Running Bing supplemental search…")
            self._bing_supplemental(queries[:3])

        return self._leads[:self.max_leads]

    def _ddg_rounds(self, queries: list[str]) -> None:
        try:
            from ddgs import DDGS
        except ImportError:
            return

        with DDGS() as ddgs:
            for i, q in enumerate(queries):
                if len(self._leads) >= self.max_leads:
                    break
                if q in self._seen_queries:
                    continue
                self._seen_queries.add(q)

                self._emit(f"[{i+1}/{len(queries)}] Searching: {q[:55]}…")

                # Paginate: DDG allows max_results but we split into batches
                # to get more diverse results
                batch_sizes = [15, 15, 10]  # 3 pages × ~15 = 40 per query
                for batch_n, batch_size in enumerate(batch_sizes):
                    if len(self._leads) >= self.max_leads:
                        break
                    try:
                        results = list(ddgs.text(
                            q,
                            max_results=batch_size,
                        ))
                        time.sleep(random.uniform(0.4, 1.0))
                    except Exception:
                        break

                    if not results:
                        break

                    new_count = 0
                    for r in results:
                        url = r.get("href", "")
                        title = r.get("title", "")

                        if not _is_valid_url(url) or not _is_relevant_result(url, title):
                            continue

                        domain = urlparse(url).netloc.lower().removeprefix("www.")
                        if domain in self._seen_domains:
                            continue

                        if _is_list_page(url, title):
                            with self._lock:
                                if url not in self._list_pages:
                                    self._list_pages.append(url)
                            self._seen_domains.add(domain)
                            continue

                        self._seen_domains.add(domain)
                        lead = Lead(
                            company_name=_clean_company_name(title, url),
                            website=url.split("?")[0].rstrip("/"),
                            niche=self.niche,
                            sources=["ddg_search"],
                        )
                        with self._lock:
                            self._leads.append(lead)
                        new_count += 1

                    # Stop paginating this query if we got no new results
                    if new_count == 0:
                        break

    def _mine_list_pages(self) -> None:
        from scraper.http.session import fetch, get_client
        from bs4 import BeautifulSoup

        client = get_client()
        for url in self._list_pages[:8]:
            resp = fetch(url, client=client)
            if not resp:
                continue
            soup = BeautifulSoup(resp.text, "lxml")

            self._emit(f"  Mining: {urlparse(url).netloc}")
            found = 0
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if not href.startswith("http"):
                    continue
                if not _is_valid_url(href):
                    continue
                domain = urlparse(href).netloc.lower().removeprefix("www.")
                if domain in self._seen_domains:
                    continue
                self._seen_domains.add(domain)

                link_text = a.get_text(strip=True)
                name = link_text if (3 < len(link_text) < 60) else _domain_as_name(href)
                lead = Lead(
                    company_name=name,
                    website=href.split("?")[0].rstrip("/"),
                    niche=self.niche,
                    sources=["list_page"],
                )
                with self._lock:
                    self._leads.append(lead)
                found += 1
                if len(self._leads) >= self.max_leads:
                    return

            if found:
                self._emit(f"  → +{found} leads from list page")

    def _bing_supplemental(self, queries: list[str]) -> None:
        from scraper.http.session import fetch, get_client
        from bs4 import BeautifulSoup
        from urllib.parse import urlencode, unquote
        import re

        client = get_client()
        for q in queries:
            if len(self._leads) >= self.max_leads:
                break
            params = urlencode({"q": q, "count": 50})
            url = f"https://www.bing.com/search?{params}"
            resp = fetch(url, client=client)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, "lxml")
            for a in soup.select("li.b_algo h2 > a[href], .b_algo h2 a"):
                href = a.get("href", "")
                # Unwrap Bing redirects
                m = re.search(r"url=([^&]+)", href)
                if m:
                    href = unquote(m.group(1))
                if not href.startswith("http") or not _is_valid_url(href):
                    continue
                domain = urlparse(href).netloc.lower().removeprefix("www.")
                if domain in self._seen_domains:
                    continue
                self._seen_domains.add(domain)
                lead = Lead(
                    company_name=a.get_text(strip=True).split(" - ")[0][:60],
                    website=href.split("?")[0].rstrip("/"),
                    niche=self.niche,
                    sources=["bing_search"],
                )
                with self._lock:
                    self._leads.append(lead)
