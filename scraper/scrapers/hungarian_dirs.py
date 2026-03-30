from __future__ import annotations
import re
from urllib.parse import urlencode, urljoin
from bs4 import BeautifulSoup
from scraper.scrapers.base import BaseScraper
from scraper.models import Lead
from scraper.extractors.validators import parse_phone, is_valid_email, normalize_email


class FirmaniaScraper(BaseScraper):
    """firmania.hu — Hungarian business directory."""
    name = "firmania_hu"
    BASE = "https://hu.firmania.net"

    def search(self, query: str, location: str, max_results: int = 50) -> list[Lead]:
        leads = []
        seen: set[str] = set()
        city = location.split(",")[0].strip()

        for page in range(1, 6):
            params = urlencode({"q": f"{query} {city}", "page": page})
            url = f"{self.BASE}/results?{params}"
            html = self.fetch(url)
            if not html:
                break

            soup = BeautifulSoup(html, "lxml")
            items = soup.select(".company, .listing, article, .result-item, li[itemtype]")
            if not items:
                break

            for item in items:
                lead = self._parse_item(item)
                if not lead:
                    continue
                key = lead.website or lead.company_name.lower()
                if key in seen:
                    continue
                seen.add(key)
                leads.append(lead)
                if len(leads) >= max_results:
                    return leads

        return leads

    def _parse_item(self, item) -> Lead | None:
        name_el = item.select_one("h2, h3, .name, [itemprop='name']")
        name = name_el.get_text(strip=True) if name_el else ""

        phone_el = item.select_one(".phone, [itemprop='telephone'], .tel")
        phone = phone_el.get_text(strip=True) if phone_el else ""

        web_el = item.select_one("a[href*='://'], [itemprop='url']")
        website = ""
        if web_el:
            href = web_el.get("href", "") or web_el.get("content", "")
            if href.startswith("http"):
                website = href.split("?")[0].rstrip("/")

        addr_el = item.select_one("[itemprop='address'], .address")
        address = addr_el.get_text(strip=True) if addr_el else ""

        if not name:
            return None

        lead = Lead(
            company_name=name,
            website=website,
            address=address,
            sources=[self.name],
        )
        if phone:
            p = parse_phone(phone, "HU")
            if p:
                lead.phones.append(p)

        return lead


class GoldenPagesScraper(BaseScraper):
    """arany.hu / Golden Pages Hungary."""
    name = "arany_hu"
    BASE = "https://www.arany.hu"

    def search(self, query: str, location: str, max_results: int = 50) -> list[Lead]:
        leads = []
        seen: set[str] = set()
        city = location.split(",")[0].strip()

        for page in range(1, 6):
            params = {"mit": query, "hol": city, "oldal": page}
            url = f"{self.BASE}/cegkereso?" + urlencode(params)
            html = self.fetch(url)
            if not html:
                break

            soup = BeautifulSoup(html, "lxml")
            items = soup.select(".ceg-lista-elem, .company-item, article")
            if not items:
                break

            for item in items:
                lead = self._parse_item(item)
                if not lead:
                    continue
                key = lead.website or lead.company_name.lower()
                if key in seen:
                    continue
                seen.add(key)
                leads.append(lead)
                if len(leads) >= max_results:
                    return leads

        return leads

    def _parse_item(self, item) -> Lead | None:
        name_el = item.select_one("h2, h3, .ceg-nev, .nev")
        name = name_el.get_text(strip=True) if name_el else ""
        if not name:
            return None

        phone_el = item.select_one(".telefon, .phone, .tel")
        phone = phone_el.get_text(strip=True) if phone_el else ""

        web_el = item.select_one("a[href*='://']")
        website = ""
        if web_el:
            href = web_el.get("href", "")
            if href.startswith("http") and "arany.hu" not in href:
                website = href.split("?")[0].rstrip("/")

        addr_el = item.select_one(".cim, .address")
        address = addr_el.get_text(strip=True) if addr_el else ""

        lead = Lead(
            company_name=name,
            website=website,
            address=address,
            sources=[self.name],
        )
        if phone:
            p = parse_phone(phone, "HU")
            if p:
                lead.phones.append(p)

        return lead


class CegjezetekScraper(BaseScraper):
    """cegtalalo.hu — Hungarian company finder with contact details."""
    name = "cegtalalo_hu"
    BASE = "https://www.cegtalalo.hu"

    def search(self, query: str, location: str, max_results: int = 50) -> list[Lead]:
        leads = []
        seen: set[str] = set()
        city = location.split(",")[0].strip()

        for page in range(1, 6):
            params = {"q": f"{query} {city}", "page": page}
            url = f"{self.BASE}/search?" + urlencode(params)
            html = self.fetch(url)
            if not html:
                break

            soup = BeautifulSoup(html, "lxml")
            items = soup.select(".company, .result, article, .listing-item")
            if not items:
                # Try alternative selectors
                items = soup.find_all("div", class_=re.compile(r"company|result|listing", re.I))
            if not items:
                break

            for item in items:
                lead = self._parse_item(item)
                if not lead:
                    continue
                key = lead.website or lead.company_name.lower()
                if key in seen:
                    continue
                seen.add(key)
                leads.append(lead)
                if len(leads) >= max_results:
                    return leads

        return leads

    def _parse_item(self, item) -> Lead | None:
        name_el = item.select_one("h2, h3, .name, .company-name")
        name = name_el.get_text(strip=True) if name_el else ""
        if not name:
            return None

        emails_found = []
        for a in item.find_all("a", href=True):
            if a["href"].lower().startswith("mailto:"):
                email = normalize_email(a["href"][7:].split("?")[0])
                if is_valid_email(email):
                    emails_found.append(email)

        phone_el = item.select_one(".phone, .tel, [class*='phone']")
        phone = phone_el.get_text(strip=True) if phone_el else ""

        web_el = item.select_one("a[href*='://']")
        website = ""
        if web_el:
            href = web_el.get("href", "")
            if href.startswith("http") and self.BASE.split("//")[1] not in href:
                website = href.split("?")[0].rstrip("/")

        lead = Lead(
            company_name=name,
            website=website,
            emails=emails_found,
            sources=[self.name],
        )
        if phone:
            p = parse_phone(phone, "HU")
            if p:
                lead.phones.append(p)

        return lead
