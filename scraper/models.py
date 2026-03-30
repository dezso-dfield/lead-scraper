from __future__ import annotations
from dataclasses import dataclass, field
import hashlib


@dataclass(slots=True)
class Lead:
    company_name: str = ""
    website: str = ""
    emails: list[str] = field(default_factory=list)
    phones: list[str] = field(default_factory=list)
    address: str = ""
    city: str = ""
    country: str = ""
    niche: str = ""
    sources: list[str] = field(default_factory=list)
    confidence: float = 0.0

    def canonical_key(self) -> str:
        if self.website:
            domain = (
                self.website.lower()
                .removeprefix("https://")
                .removeprefix("http://")
                .removeprefix("www.")
                .rstrip("/")
                .split("/")[0]
                .split("?")[0]
            )
            return domain
        if self.emails:
            return self.emails[0].split("@")[-1].lower()
        key = f"{self.company_name.lower().strip()}::{self.phones[0] if self.phones else ''}"
        return hashlib.md5(key.encode()).hexdigest()

    def has_contacts(self) -> bool:
        return bool(self.emails or self.phones)

    def merge(self, other: "Lead") -> None:
        if len(other.company_name) > len(self.company_name):
            self.company_name = other.company_name
        for e in other.emails:
            if e not in self.emails:
                self.emails.append(e)
        for p in other.phones:
            if p not in self.phones:
                self.phones.append(p)
        if not self.address and other.address:
            self.address = other.address
        elif other.address and len(other.address) > len(self.address):
            self.address = other.address
        if not self.website and other.website:
            self.website = other.website
        for s in other.sources:
            if s not in self.sources:
                self.sources.append(s)
        self.confidence = max(self.confidence, other.confidence)
        # Sort emails: info@/contact@ first
        self.emails.sort(key=lambda e: (0 if any(e.startswith(p) for p in ("info", "contact", "hello")) else 1, e))

    def to_dict(self) -> dict:
        return {
            "company_name": self.company_name,
            "website": self.website,
            "emails": ", ".join(self.emails),
            "phones": ", ".join(self.phones),
            "address": self.address,
            "city": self.city,
            "country": self.country,
            "niche": self.niche,
            "sources": ", ".join(self.sources),
            "confidence": round(self.confidence, 2),
        }
