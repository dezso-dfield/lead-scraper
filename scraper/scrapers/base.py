from __future__ import annotations
from abc import ABC, abstractmethod
from scraper.models import Lead
from scraper.http.session import fetch, get_client


class BaseScraper(ABC):
    name: str = "base"

    def __init__(self):
        self.client = get_client()

    def fetch(self, url: str) -> str | None:
        resp = fetch(url, client=self.client)
        return resp.text if resp else None

    @abstractmethod
    def search(self, query: str, location: str, max_results: int = 50) -> list[Lead]:
        """Return a list of Lead stubs (website + company_name at minimum)."""
        ...
