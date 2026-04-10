"""
AI-powered query generation using Claude API.
"""
from __future__ import annotations
import json
import re


def generate_ai_queries(niche: str, location: str, api_key: str) -> list[str]:
    """
    Use Claude Haiku to generate targeted search queries for finding business leads.
    Returns list of query strings, or [] if API call fails.
    """
    try:
        import anthropic
    except ImportError:
        return []

    city = location.split(",")[0].strip()
    country = location.split(",")[-1].strip() if "," in location else location

    client = anthropic.Anthropic(api_key=api_key)
    prompt = (
        f"Generate 12 targeted search queries to find {niche} businesses in {city}, {country}.\n\n"
        "Requirements:\n"
        "- Queries should find business contact pages with emails/phones\n"
        "- Mix English and local language variants\n"
        "- Include queries for directories, listings, and associations\n"
        "- Include contact-intent signals like 'email', 'contact', 'phone'\n"
        "- Some queries should use site: operators for relevant local directories\n"
        "- Focus on finding decision-makers and business owners\n\n"
        "Return ONLY a JSON array of strings. No explanation. Example:\n"
        '[\"event planners Budapest contact\", \"rendezvényszervező cégek Budapest email\"]'
    )

    try:
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}],
        )
        text = msg.content[0].text.strip()
        m = re.search(r"\[.*?\]", text, re.DOTALL)
        if m:
            queries = json.loads(m.group())
            return [q for q in queries if isinstance(q, str) and q.strip()]
    except Exception:
        pass
    return []
