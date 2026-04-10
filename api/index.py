"""Vercel entrypoint — re-exports the FastAPI app."""
from scraper.web.server import app  # noqa: F401
