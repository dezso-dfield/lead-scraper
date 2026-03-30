from __future__ import annotations
import re
import json
import time
import random
from urllib.parse import quote_plus
from scraper.models import Lead
from scraper.extractors.validators import parse_phone


def _check_playwright() -> bool:
    try:
        from playwright.sync_api import sync_playwright  # noqa: F401
        return True
    except ImportError:
        return False


class GoogleMapsScraper:
    name = "google_maps"

    def search(self, query: str, location: str, max_results: int = 40) -> list[Lead]:
        full_query = f"{query} {location}"
        # Try HTML extraction first (no browser needed)
        leads = self._html_search(full_query, max_results)
        if leads:
            return leads
        # Fall back to Playwright if available
        if _check_playwright():
            return self._playwright_search(full_query, max_results)
        return []

    def _html_search(self, query: str, max_results: int) -> list[Lead]:
        """Parse Google Maps HTML response for embedded business data."""
        from scraper.http.session import make_client, GOOGLE_COOKIES, get_ua
        import httpx

        url = f"https://www.google.com/maps/search/{quote_plus(query)}"
        client = make_client(cookies=GOOGLE_COOKIES)
        domain = "www.google.com"
        headers = {
            "User-Agent": get_ua(domain),
            "Accept-Language": "hu-HU,hu;q=0.9,en;q=0.8",
        }

        try:
            resp = client.get(url, headers=headers)
        except Exception:
            return []

        if resp.status_code != 200 or len(resp.text) < 10000:
            return []

        return self._parse_maps_html(resp.text, max_results)

    def _parse_maps_html(self, html: str, max_results: int) -> list[Lead]:
        """Extract business listings from Google Maps HTML source."""
        leads = []

        # Google Maps embeds data as large JSON-like arrays.
        # Business names often appear near phone/website data in the JS blobs.
        # Strategy: find all .hu websites and phone numbers in proximity.

        # Extract website URLs
        websites = re.findall(r'https?://(?:www\.)?[\w\-]+\.(?:hu|com|eu|org|net)(?:/[\w\-/%.]*)?', html)
        websites = [w for w in websites if not any(skip in w for skip in
            ["google", "gstatic", "googleapis", "youtube", "facebook", "instagram",
             "maps.google", "schema.org", "w3.org"])]
        websites = list(dict.fromkeys(websites))  # deduplicate preserving order

        # Extract phone numbers
        phones = re.findall(r'\+36[\s\-]?\d{1,2}[\s\-]?\d{3}[\s\-]?\d{4}', html)
        phones += re.findall(r'\+36[\s\-]?\d{8,9}', html)
        phones_valid = []
        for p in phones:
            parsed = parse_phone(p, "HU")
            if parsed and parsed not in phones_valid:
                phones_valid.append(parsed)

        # Extract business names from Maps JS data
        # Pattern: "Business Name" followed shortly by a Maps place URL
        name_candidates = re.findall(r'"([A-ZÁÉÍÓÖŐÚÜŰ][^"]{4,60}(?:Kft|Bt|Zrt|Nyrt|szervező|iroda|csoport|team|event|events|rendezvény)[^"]{0,30})"', html)
        name_candidates = list(dict.fromkeys(name_candidates))

        # Pair websites with names/phones heuristically
        # (Maps HTML doesn't have clean per-listing structure without JS)
        for i, website in enumerate(websites[:max_results]):
            lead = Lead(
                website=website,
                sources=[self.name],
            )
            if i < len(name_candidates):
                lead.company_name = name_candidates[i]
            if i < len(phones_valid):
                lead.phones.append(phones_valid[i])
            leads.append(lead)

        return leads[:max_results]

    def _playwright_search(self, query: str, max_results: int) -> list[Lead]:
        from playwright.sync_api import sync_playwright

        leads: list[Lead] = []
        search_url = f"https://www.google.com/maps/search/{quote_plus(query)}"

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            ctx = browser.new_context(
                viewport={"width": 1366, "height": 768},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            )
            # Set consent cookie
            ctx.add_cookies([
                {"name": "CONSENT", "value": "YES+cb", "domain": ".google.com", "path": "/"},
                {"name": "SOCS", "value": "CAESEwgDEgk0ODA3Nzk3MjkaAmh1IAEaBgiA_LyoBg", "domain": ".google.com", "path": "/"},
            ])
            page = ctx.new_page()

            try:
                page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
                time.sleep(2)

                # Scroll the results feed
                feed = page.query_selector("div[role='feed']")
                if feed:
                    prev_count = 0
                    for _ in range(12):
                        items = page.query_selector_all("div[role='feed'] > div > div[jsaction]")
                        current_count = len(items)
                        if current_count >= max_results or current_count == prev_count:
                            break
                        prev_count = current_count
                        feed.evaluate("el => el.scrollBy(0, 800)")
                        time.sleep(random.uniform(1.0, 2.0))

                items = page.query_selector_all("div[role='feed'] > div > div[jsaction]")
                for item in items[:max_results]:
                    try:
                        item.click()
                        time.sleep(random.uniform(1.0, 2.0))
                        lead = Lead(sources=[self.name])

                        name_el = page.query_selector("h1.DUwDvf, h1[class*='fontHeadline']")
                        if name_el:
                            lead.company_name = name_el.inner_text().strip()

                        addr_el = page.query_selector("button[data-item-id='address']")
                        if addr_el:
                            lead.address = addr_el.inner_text().strip()

                        phone_el = page.query_selector("button[data-item-id*='phone']")
                        if phone_el:
                            p = parse_phone(phone_el.inner_text().strip(), "HU")
                            if p:
                                lead.phones.append(p)

                        web_el = page.query_selector("a[data-item-id='authority']")
                        if web_el:
                            href = web_el.get_attribute("href") or ""
                            m = re.search(r"url=([^&]+)", href)
                            if m:
                                from urllib.parse import unquote
                                href = unquote(m.group(1))
                            if href.startswith("http"):
                                lead.website = href.split("?")[0].rstrip("/")

                        if lead.company_name or lead.website:
                            leads.append(lead)
                    except Exception:
                        continue
            except Exception:
                pass
            finally:
                browser.close()

        return leads
