from __future__ import annotations
import threading
from typing import Callable
from scraper.models import Lead
from scraper.pipeline.deduplicator import Deduplicator
from scraper.extractors.contact import ContactExtractor
from scraper.http.session import fetch, get_client


def _fetch_fn(url: str):
    return fetch(url, client=get_client())


class Orchestrator:
    def __init__(
        self,
        query: str,
        location: str,
        max_leads: int = 100,
        use_maps: bool = True,
        serp_api_key: str | None = None,
        default_region: str = "HU",
        on_discovery: Callable[[Lead], None] | None = None,
        on_enriched: Callable[[Lead], None] | None = None,
        enrich_workers: int = 5,
        verbose: bool = False,
    ):
        self.query = query
        self.location = location
        self.max_leads = max_leads
        self.use_maps = use_maps
        self.serp_api_key = serp_api_key
        self.default_region = default_region
        self.on_discovery = on_discovery
        self.on_enriched = on_enriched
        self.enrich_workers = enrich_workers
        self.verbose = verbose

        self.dedup = Deduplicator()
        self.extractor = ContactExtractor(default_region=default_region)
        self._scraper_errors: dict[str, str] = {}

    def _get_scrapers(self):
        scrapers = []

        # Primary: DuckDuckGo (most reliable, no API key needed)
        try:
            from scraper.scrapers.ddg_search import DDGSearchScraper
            scrapers.append(DDGSearchScraper())
        except ImportError:
            pass

        # Secondary: Bing
        try:
            from scraper.scrapers.bing_search import BingSearchScraper
            scrapers.append(BingSearchScraper())
        except ImportError:
            pass

        # Google Search (SerpAPI or free fallback)
        try:
            from scraper.scrapers.google_search import GoogleSearchScraper
            scrapers.append(GoogleSearchScraper(serp_api_key=self.serp_api_key))
        except ImportError:
            pass

        # Hungarian directories
        try:
            from scraper.scrapers.hungarian_dirs import FirmaniaScraper, GoldenPagesScraper, CegjezetekScraper
            scrapers.extend([FirmaniaScraper(), GoldenPagesScraper(), CegjezetekScraper()])
        except ImportError:
            pass

        # Europages
        try:
            from scraper.scrapers.europages import EuropagesScraper
            scrapers.append(EuropagesScraper())
        except ImportError:
            pass

        # Google Maps
        if self.use_maps:
            try:
                from scraper.scrapers.google_maps import GoogleMapsScraper
                scrapers.insert(0, GoogleMapsScraper())
            except ImportError:
                pass

        return scrapers

    def _discover(self, scraper, results_per_scraper: int) -> list[Lead]:
        try:
            leads = scraper.search(self.query, self.location, max_results=results_per_scraper)
            return leads
        except Exception as e:
            self._scraper_errors[scraper.name] = str(e)
            return []

    def run(self, progress_callback: Callable[[str, int, int], None] | None = None) -> list[Lead]:
        scrapers = self._get_scrapers()
        if not scrapers:
            return []

        per_scraper = max(20, self.max_leads // max(len(scrapers), 1))

        # Stage 1: Discovery (parallel threads)
        stub_map: dict[str, list[Lead]] = {s.name: [] for s in scrapers}

        def discover_worker(scraper):
            leads = self._discover(scraper, per_scraper)
            stub_map[scraper.name] = leads
            for lead in leads:
                if self.on_discovery:
                    self.on_discovery(lead)

        threads = [threading.Thread(target=discover_worker, args=(s,), daemon=True) for s in scrapers]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Collect all stubs, deduplicate early to avoid redundant enrichment
        seen_domains: set[str] = set()
        all_stubs: list[Lead] = []
        for leads in stub_map.values():
            for lead in leads:
                key = lead.canonical_key()
                if key and key not in seen_domains:
                    seen_domains.add(key)
                    all_stubs.append(lead)

        # Stage 2: Enrichment (parallel workers)
        total = len(all_stubs)
        enriched_count = [0]
        lock = threading.Lock()

        def enrich_worker(stubs_chunk: list[Lead]):
            for lead in stubs_chunk:
                enriched = self.extractor.enrich_lead(lead, _fetch_fn)
                with lock:
                    self.dedup.add(enriched)
                    enriched_count[0] += 1
                    if self.on_enriched:
                        self.on_enriched(enriched)
                    if progress_callback:
                        progress_callback("enriching", enriched_count[0], total)

        chunk_size = max(1, (len(all_stubs) + self.enrich_workers - 1) // self.enrich_workers)
        chunks = [all_stubs[i:i + chunk_size] for i in range(0, len(all_stubs), chunk_size)]

        workers = [threading.Thread(target=enrich_worker, args=(chunk,), daemon=True) for chunk in chunks]
        for w in workers:
            w.start()
        for w in workers:
            w.join()

        return self.dedup.get_all()
