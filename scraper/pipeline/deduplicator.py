from __future__ import annotations
from scraper.models import Lead


class Deduplicator:
    def __init__(self):
        self._leads: dict[str, Lead] = {}

    def add(self, lead: Lead) -> None:
        key = lead.canonical_key()
        if not key:
            return
        if key in self._leads:
            self._leads[key].merge(lead)
        else:
            self._leads[key] = lead

    def get_all(self) -> list[Lead]:
        return list(self._leads.values())

    def __len__(self) -> int:
        return len(self._leads)
