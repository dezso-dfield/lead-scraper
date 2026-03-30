from __future__ import annotations
import json
from bs4 import BeautifulSoup


CONTACT_TYPES = frozenset({
    "localbusiness", "organization", "contactpage", "person",
    "corporation", "professionalperson", "event",
})


def extract_schema_org(soup: BeautifulSoup) -> dict:
    """Extract contact info from Schema.org JSON-LD blocks."""
    result: dict = {}
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "")
        except Exception:
            continue
        if isinstance(data, list):
            for item in data:
                _process_schema_node(item, result)
        elif isinstance(data, dict):
            _process_schema_node(data, result)
    return result


def _process_schema_node(node: dict, result: dict) -> None:
    if not isinstance(node, dict):
        return
    t = node.get("@type", "")
    if isinstance(t, list):
        t = " ".join(t)
    t = t.lower()
    if not any(ct in t for ct in CONTACT_TYPES):
        # Still try nested @graph
        for item in node.get("@graph", []):
            _process_schema_node(item, result)
        return

    if "telephone" in node and "phone" not in result:
        result["phone"] = node["telephone"]
    if "email" in node and "email" not in result:
        result["email"] = node["email"]
    if "name" in node and "name" not in result:
        result["name"] = node["name"]

    addr = node.get("address", {})
    if isinstance(addr, dict):
        parts = []
        for key in ("streetAddress", "postalCode", "addressLocality", "addressCountry"):
            val = addr.get(key, "")
            if val:
                parts.append(str(val))
        if parts and "address" not in result:
            result["address"] = ", ".join(parts)
        if "addressLocality" in addr and "city" not in result:
            result["city"] = addr["addressLocality"]
        if "addressCountry" in addr and "country" not in result:
            result["country"] = addr["addressCountry"]
    elif isinstance(addr, str) and addr and "address" not in result:
        result["address"] = addr

    # Recurse into @graph
    for item in node.get("@graph", []):
        _process_schema_node(item, result)
