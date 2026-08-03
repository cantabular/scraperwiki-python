"""
Local version of ScraperWiki Utils, documentation here:
https://scraperwiki.com/docs/python/python_help_documentation/
"""

from . import sql, utils
from .utils import pdftoxml, scrape, status, swimport

# Compatibility
sqlite = sql

__all__ = [
    "pdftoxml",
    "scrape",
    "sql",
    "status",
    "swimport",
    "utils",
]


class Error(Exception):
    """All ScraperWiki exceptions are instances of this class
    (usually via a subclass)."""


class CPUTimeExceededError(Error):
    """CPU time limit exceeded."""
