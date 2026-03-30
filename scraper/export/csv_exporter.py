from __future__ import annotations
import csv
from pathlib import Path
from scraper.models import Lead


def export_csv(leads: list[Lead], path: str | Path) -> Path:
    path = Path(path)
    fieldnames = ["company_name", "website", "emails", "phones", "address", "city", "country", "niche", "sources", "confidence"]

    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for lead in sorted(leads, key=lambda l: l.confidence, reverse=True):
            writer.writerow(lead.to_dict())

    return path
