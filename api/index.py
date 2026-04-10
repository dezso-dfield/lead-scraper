"""Vercel serverless entrypoint — re-exports the FastAPI ASGI app."""
from scraper.web.server import app  # noqa: F401
