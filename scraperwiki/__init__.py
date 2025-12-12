'''
Local version of ScraperWiki Utils, documentation here:
https://scraperwiki.com/docs/python/python_help_documentation/
'''
from .utils import scrape, pdftoxml, status, swimport
from . import utils
from . import sql

# Compatibility
sqlite = sql

__all__ = [
        'scrape',
        'pdftoxml',
        'status',
        'swimport',
        'utils',
        'sql',
]

class Error(Exception):
    """All ScraperWiki exceptions are instances of this class
    (usually via a subclass)."""
    pass

class CPUTimeExceededError(Error):
    """CPU time limit exceeded."""
    pass
