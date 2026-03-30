from __future__ import annotations
import re
import unicodedata
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from scraper.scrapers.base import BaseScraper
from scraper.models import Lead
from scraper.extractors.validators import parse_phone


def _slugify(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^\w\s-]", "", text.lower())
    return re.sub(r"[\s_-]+", "-", text).strip("-")


class EuropagesScraper(BaseScraper):
    name = "europages"
    BASE = "https://www.europages.com"

    def search(self, query: str, location: str, max_results: int = 50) -> list[Lead]:
        niche_slug = _slugify(query)
        loc_slug = _slugify(location)

        # Try both URL patterns
        url_patterns = [
            f"{self.BASE}/companies/{loc_slug}/{niche_slug}.html",
            f"{self.BASE}/en/companies/{loc_slug}/{niche_slug}.html",
            f"{self.BASE}/companies/{niche_slug}.html?countryCode={self._country_code(location)}",
        ]

        leads: list[Lead] = []
        seen: set[str] = set()

        for pattern in url_patterns:
            page_leads = self._scrape_pages(pattern, max_results - len(leads), seen)
            leads.extend(page_leads)
            if leads:
                break  # found results with first working pattern

        return leads[:max_results]

    def _country_code(self, location: str) -> str:
        loc = location.lower()
        if any(x in loc for x in ("hungary", "magyarország", "budapest", "hun")):
            return "HU"
        return ""

    def _scrape_pages(self, base_url: str, max_results: int, seen: set[str]) -> list[Lead]:
        leads = []
        url = base_url

        for page_num in range(1, 11):  # max 10 pages
            html = self.fetch(url)
            if not html:
                break

            soup = BeautifulSoup(html, "lxml")
            page_leads = self._parse_page(soup, seen)
            leads.extend(page_leads)

            if len(leads) >= max_results:
                break

            # Find next page
            next_a = soup.select_one("a[aria-label='next page'], a.next, li.next > a")
            if not next_a:
                break
            href = next_a.get("href", "")
            if not href:
                break
            url = urljoin(self.BASE, href)

        return leads

    def _parse_page(self, soup: BeautifulSoup, seen: set[str]) -> list[Lead]:
        leads = []
        for card in soup.select("article.company-card, div[data-cy='company-card'], div.company"):
            name_el = card.select_one("h2, h3, .company-name, [data-cy='company-name']")
            name = name_el.get_text(strip=True) if name_el else ""

            phone_el = card.select_one("[data-cy='phone'], .phone, .tel")
            phone = phone_el.get_text(strip=True) if phone_el else ""

            web_el = card.select_one("a[data-cy='website'], a[href*='://']")
            website = ""
            if web_el:
                href = web_el.get("href", "")
                if href.startswith("http"):
                    website = href.split("?")[0].rstrip("/")

            if not name and not website:
                continue

            key = website or name.lower()
            if key in seen:
                continue
            seen.add(key)

            lead = Lead(
                company_name=name,
                website=website,
                sources=[self.name],
            )
            if phone:
                p = parse_phone(phone, "HU")
                if p:
                    lead.phones.append(p)

            addr_el = card.select_one(".address, [data-cy='address']")
            if addr_el:
                lead.address = addr_el.get_text(strip=True)

            leads.append(lead)

        return leads
