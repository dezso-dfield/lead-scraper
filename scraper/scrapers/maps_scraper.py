"""
Google Maps lead scraper.
Mode 1: Google Places API (if google_maps_api_key is set in settings)
Mode 2: DDG site:google.com/maps fallback (no API key needed)
"""
from __future__ import annotations
import re
import time
import random
from typing import Callable
from urllib.parse import urlparse, urlencode

from scraper.models import Lead


def search_maps(
    niche: str,
    location: str,
    api_key: str = "",
    max_results: int = 60,
    on_progress: Callable[[str], None] | None = None,
) -> list[Lead]:
    """Search Google Maps for businesses. Returns Lead stubs."""
    def emit(msg: str) -> None:
        if on_progress:
            on_progress(msg)

    if api_key:
        return _search_places_api(niche, location, api_key, max_results, emit)
    else:
        return _search_maps_ddg(niche, location, max_results, emit)


def _search_places_api(
    niche: str, location: str, api_key: str,
    max_results: int, emit: Callable,
) -> list[Lead]:
    """Use Google Places API (Text Search + Place Details)."""
    try:
        import httpx
    except ImportError:
        emit("  [maps] httpx not available")
        return []

    city    = location.split(",")[0].strip()
    country = location.split(",")[-1].strip() if "," in location else location
    query   = f"{niche} in {city}, {country}"
    emit(f"  Google Maps API: searching for {query!r}…")

    leads: list[Lead] = []
    seen: set[str] = set()
    page_token = None

    with httpx.Client(timeout=15) as client:
        for _page in range(3):  # max 3 pages × 20 = 60 results
            params: dict = {
                "query":  query,
                "key":    api_key,
                "fields": "place_id,name,formatted_address,website,rating,user_ratings_total",
            }
            if page_token:
                params["pagetoken"] = page_token
                time.sleep(2)  # required by Google

            resp = client.get("https://maps.googleapis.com/maps/api/place/textsearch/json", params=params)
            data = resp.json()

            if data.get("status") not in ("OK", "ZERO_RESULTS"):
                emit(f"  [maps] Places API error: {data.get('status')} — {data.get('error_message','')}")
                break

            for place in data.get("results", []):
                place_id = place.get("place_id", "")
                if place_id in seen:
                    continue
                seen.add(place_id)

                # Fetch details to get phone + website
                det_resp = client.get(
                    "https://maps.googleapis.com/maps/api/place/details/json",
                    params={"place_id": place_id, "key": api_key,
                            "fields": "name,formatted_address,formatted_phone_number,website,rating"},
                )
                det = det_resp.json().get("result", {})

                name    = det.get("name") or place.get("name", "")
                address = det.get("formatted_address") or place.get("formatted_address", "")
                phone   = det.get("formatted_phone_number", "")
                website = det.get("website", "")
                rating  = str(det.get("rating") or place.get("rating") or "")

                if not name:
                    continue

                city_guess = _extract_city(address) or city
                lead = Lead(
                    company_name=name,
                    website=website or f"https://www.google.com/maps/place/?q=place_id:{place_id}",
                    phones=[phone] if phone else [],
                    address=address,
                    city=city_guess,
                    country=country,
                    niche=niche,
                    sources=["google_maps"],
                    confidence=0.85,
                )
                leads.append(lead)

                if len(leads) >= max_results:
                    break

            page_token = data.get("next_page_token")
            if not page_token or len(leads) >= max_results:
                break

    emit(f"  → Google Maps API: {len(leads)} places found")
    return leads


def _search_maps_ddg(
    niche: str, location: str, max_results: int, emit: Callable,
) -> list[Lead]:
    """Fallback: DDG text search for Google Maps listings."""
    try:
        from ddgs import DDGS
    except ImportError:
        emit("  [maps] ddgs not installed")
        return []

    city    = location.split(",")[0].strip()
    country = location.split(",")[-1].strip() if "," in location else location

    queries = [
        f"{niche} {city} site:maps.google.com",
        f"{niche} {city} google maps contact phone",
        f'"{niche}" {city} cím telefon',
    ]

    leads: list[Lead] = []
    seen_domains: set[str] = set()

    with DDGS() as ddgs:
        for q in queries:
            if len(leads) >= max_results:
                break
            emit(f"  Maps DDG: {q[:55]}…")
            try:
                results = list(ddgs.text(q, max_results=20))
                time.sleep(random.uniform(0.5, 1.0))
            except Exception:
                continue

            for r in results:
                url   = r.get("href", "")
                title = r.get("title", "")
                body  = r.get("body", "")

                if not url or not title:
                    continue

                domain = urlparse(url).netloc.lower().removeprefix("www.")
                if domain in seen_domains:
                    continue
                seen_domains.add(domain)

                phones = re.findall(r"(?:\+\d{1,3}[\s\-]?)?\(?\d{1,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{3,4}", body)
                rating_m = re.search(r"(\d\.\d)\s*(?:/5|★|stars?|csillag)", body, re.I)
                rating = rating_m.group(1) if rating_m else ""

                lead = Lead(
                    company_name=title.split(" - ")[0][:80],
                    website=url.split("?")[0].rstrip("/"),
                    phones=[phones[0].strip()] if phones else [],
                    city=city,
                    country=country,
                    niche=niche,
                    sources=["google_maps_ddg"],
                    confidence=0.6,
                )
                leads.append(lead)
                if len(leads) >= max_results:
                    break

    emit(f"  → Maps DDG fallback: {len(leads)} results")
    return leads


def _extract_city(address: str) -> str:
    """Best-effort city extraction from a formatted address."""
    parts = [p.strip() for p in address.split(",")]
    if len(parts) >= 2:
        return parts[-2]
    return ""
