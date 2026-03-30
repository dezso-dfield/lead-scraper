from __future__ import annotations
import re

# User agents pool - realistic Chrome/Firefox UAs
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
]

BROWSER_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,hu;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}

# Domains to skip when extracting leads
SKIP_DOMAINS = frozenset({
    "linkedin.com", "facebook.com", "instagram.com", "twitter.com", "x.com",
    "youtube.com", "wikipedia.org", "tiktok.com", "pinterest.com",
    "google.com", "google.hu", "bing.com", "yahoo.com",
    "tripadvisor.com", "booking.com", "airbnb.com",
    "gov.hu", "mnb.hu", "njt.hu", "bm.gov.hu",
    "yellowpages.com", "yelp.com",
    "jooble.org", "indeed.com", "profession.hu", "jobs.hu",  # job sites
    "eventbrite.com", "meetup.com", "ticketmaster.hu",  # event platforms
    "prezi.com", "slideshare.net",
    "starofservice.hu", "cylex.hu", "firmania.net",  # directories
    "teamlab.hu",  # blog/aggregator
    "emailondeck.com", "temp-mail.org", "guerrillamail.com",  # temp email
    "budapestpark.hu", "funcode.hu",  # entertainment venues, not B2B leads
    "uni-corvinus.hu", "bme.hu", "elte.hu",  # universities
    "opten.hu", "nemzeti-cegtar.hu", "e-cegjegyzek.hu",  # company registries
    "nemzeticegtar.hu", "ceginfo.hu",
})

SKIP_EXTENSIONS = frozenset({
    ".pdf", ".docx", ".xlsx", ".zip", ".jpg", ".jpeg", ".png",
    ".gif", ".svg", ".mp4", ".mp3", ".avi", ".mov",
})

# Contact page path probes (multilingual)
CONTACT_PATHS = [
    "/contact", "/contact-us", "/contacts", "/contact.html",
    "/kapcsolat", "/kapcsolatfelvetel",       # Hungarian
    "/impressum", "/impressum.html",           # German
    "/about", "/about-us", "/about.html",
    "/reach-us", "/get-in-touch",
    "/rolunk",                                 # Hungarian: about us
]

CONTACT_KEYWORDS = frozenset({
    "contact", "kapcsolat", "impressum", "about", "reach",
    "touch", "email", "phone", "call", "write",
    "rolunk", "elerhetoseg", "elérhetőség",
})

# Hungarian location/country detection keywords
HUNGARIAN_INDICATORS = frozenset({
    "budapest", "hungary", "magyarország", "magyar", "debrecen",
    "miskolc", "pécs", "győr", "nyíregyháza", "kecskemét",
    "székesfehérvár", "szombathely", "érd", "tatabánya",
    "hun", ".hu",
})

# Translation map for query expansion (niche -> English variants or Hungarian)
# Keys match substrings of the user query (lowercase)
NICHE_TRANSLATIONS: dict[str, list[str]] = {
    "event organizer": ["rendezvényszervező", "eseményszervező"],
    "event planning": ["rendezvénytervezés", "eseményszervezés"],
    "esemény": ["event organizer", "rendezvényszervező", "eseményszervező iroda"],
    "rendezvény": ["event organizer", "rendezvényszervező Budapest"],
    "wedding planner": ["esküvőszervező", "esküvő szervező"],
    "conference organizer": ["konferenciaszervező"],
    "marketing agency": ["marketing ügynökség"],
    "ai company": ["mesterséges intelligencia", "AI megoldás", "AI fejlesztő cég"],
    "ai solution": ["mesterséges intelligencia megoldás", "AI cég Hungary"],
    "software company": ["szoftvercég", "szoftver fejlesztő"],
    "it company": ["IT cég", "informatikai cég"],
    "startup": ["startup Hungary", "tech startup"],
    "restaurant": ["étterem", "vendéglő"],
    "hotel": ["szálloda", "szálló"],
    "catering": ["catering Budapest", "étkeztetés"],
    "fotós": ["fotós Budapest", "photographer Budapest"],
    "dj": ["DJ Budapest", "DJ szolgáltatás"],
}

# Email regex patterns
EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE,
)

# Obfuscated email patterns
OBFUS_EMAIL_REGEX = re.compile(
    r"([a-zA-Z0-9._%+\-]+)\s*(?:@|\[at\]|\(at\)|&#64;|\bat\b)\s*([a-zA-Z0-9.\-]+)\s*(?:\.|\[dot\]|\(dot\)|&#46;|\bdot\b)\s*([a-zA-Z]{2,})",
    re.IGNORECASE,
)

# Phone number patterns (rough match before phonenumbers lib validates)
PHONE_ROUGH_REGEX = re.compile(
    r"(?:(?:\+|00)[1-9]\d{0,3}[\s\-\.]?)?"
    r"(?:\(?\d{1,4}\)?[\s\-\.]?)?"
    r"\d{1,4}[\s\-\.]?\d{1,4}[\s\-\.]?\d{1,9}",
)
