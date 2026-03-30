from __future__ import annotations
import re
import html
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

from scraper.config import EMAIL_REGEX, OBFUS_EMAIL_REGEX, CONTACT_PATHS, CONTACT_KEYWORDS
from scraper.extractors.validators import is_valid_email, normalize_email, parse_phone
from scraper.extractors.schema_org import extract_schema_org
from scraper.models import Lead


class ContactExtractor:
    def __init__(self, default_region: str = "HU"):
        self.default_region = default_region

    def extract_from_html(self, html_content: str, base_url: str = "") -> dict:
        """Extract all contact info from HTML string."""
        result: dict = {
            "emails": [],
            "phones": [],
            "address": "",
            "name": "",
            "city": "",
            "country": "",
        }

        # Limit to 300KB to avoid huge pages
        html_content = html_content[:300_000]

        soup = BeautifulSoup(html_content, "lxml")

        # Layer 1: mailto: links
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.lower().startswith("mailto:"):
                email = href[7:].split("?")[0].strip()
                if is_valid_email(email):
                    e = normalize_email(email)
                    if e not in result["emails"]:
                        result["emails"].append(e)

        # Layer 2: Schema.org JSON-LD
        schema = extract_schema_org(soup)
        if schema.get("email") and is_valid_email(schema["email"]):
            e = normalize_email(schema["email"])
            if e not in result["emails"]:
                result["emails"].append(e)
        if schema.get("phone"):
            p = parse_phone(schema["phone"], self.default_region)
            if p and p not in result["phones"]:
                result["phones"].append(p)
        if schema.get("name"):
            result["name"] = schema["name"]
        if schema.get("address"):
            result["address"] = schema["address"]
        if schema.get("city"):
            result["city"] = schema["city"]
        if schema.get("country"):
            result["country"] = schema["country"]

        # Layer 3: itemprop microdata
        for el in soup.find_all(itemprop="email"):
            val = el.get("content") or el.get_text(strip=True)
            if val and is_valid_email(val):
                e = normalize_email(val)
                if e not in result["emails"]:
                    result["emails"].append(e)
        for el in soup.find_all(itemprop="telephone"):
            val = el.get("content") or el.get_text(strip=True)
            if val:
                p = parse_phone(val, self.default_region)
                if p and p not in result["phones"]:
                    result["phones"].append(p)

        # Layer 4: tel: links
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.lower().startswith("tel:"):
                raw = href[4:].strip()
                p = parse_phone(raw, self.default_region)
                if p and p not in result["phones"]:
                    result["phones"].append(p)

        # Layer 5: regex fallback on full text
        decoded = html.unescape(html_content)

        # Find [at]/[dot] obfuscated emails
        for m in OBFUS_EMAIL_REGEX.finditer(decoded):
            reconstructed = f"{m.group(1)}@{m.group(2)}.{m.group(3)}"
            if is_valid_email(reconstructed):
                e = normalize_email(reconstructed)
                if e not in result["emails"]:
                    result["emails"].append(e)

        # Plain email regex on full text
        text = soup.get_text(separator=" ")
        for m in EMAIL_REGEX.finditer(text):
            e = normalize_email(m.group(0))
            if is_valid_email(e) and e not in result["emails"]:
                result["emails"].append(e)

        # Phone regex fallback
        import phonenumbers
        for m in re.finditer(r"[\+\(]?[0-9][0-9 \-\(\)\.]{7,}[0-9]", text):
            raw = m.group(0)
            p = parse_phone(raw, self.default_region)
            if p and p not in result["phones"]:
                result["phones"].append(p)

        return result

    def find_contact_page_urls(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        """Find likely contact page URLs from page links."""
        found = []
        base_domain = urlparse(base_url).netloc

        for a in soup.find_all("a", href=True):
            href = a["href"].lower()
            text = a.get_text(strip=True).lower()
            if any(kw in href or kw in text for kw in CONTACT_KEYWORDS):
                full = urljoin(base_url, a["href"])
                if urlparse(full).netloc == base_domain and full not in found:
                    found.append(full)
                if len(found) >= 3:
                    break

        # Also probe standard paths if we found nothing
        if not found:
            for path in CONTACT_PATHS[:5]:
                found.append(urljoin(base_url, path))

        return found[:4]

    def enrich_lead(self, lead: Lead, fetch_fn) -> Lead:
        """Fetch website and contact pages, fill in emails/phones."""
        if not lead.website:
            return lead

        resp = fetch_fn(lead.website)
        if not resp:
            return lead

        html_content = resp.text
        base_url = str(resp.url)
        data = self.extract_from_html(html_content, base_url)

        if not lead.company_name and data.get("name"):
            lead.company_name = data["name"]
        if not lead.address and data.get("address"):
            lead.address = data["address"]
        if not lead.city and data.get("city"):
            lead.city = data["city"]
        if not lead.country and data.get("country"):
            lead.country = data["country"]

        for e in data["emails"]:
            if e not in lead.emails:
                lead.emails.append(e)
        for p in data["phones"]:
            if p not in lead.phones:
                lead.phones.append(p)

        # If we found no contact info, probe contact pages
        if not lead.has_contacts():
            soup = BeautifulSoup(html_content, "lxml")
            contact_urls = self.find_contact_page_urls(soup, base_url)
            for url in contact_urls:
                cr = fetch_fn(url)
                if not cr:
                    continue
                cdata = self.extract_from_html(cr.text, url)
                for e in cdata["emails"]:
                    if e not in lead.emails:
                        lead.emails.append(e)
                for p in cdata["phones"]:
                    if p not in lead.phones:
                        lead.phones.append(p)
                if lead.has_contacts():
                    break

        # Score confidence
        lead.confidence = min(1.0,
            0.3 * bool(lead.company_name) +
            0.35 * bool(lead.emails) +
            0.25 * bool(lead.phones) +
            0.1 * bool(lead.address)
        )
        return lead
