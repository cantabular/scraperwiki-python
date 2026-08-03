"""
Local version of ScraperWiki Utils, documentation here:
https://scraperwiki.com/docs/python/python_help_documentation/
"""

import os
import shutil
import tempfile
import urllib.parse
import urllib.request
import warnings


def scrape(url, params=None, user_agent=None):
    """
    Scrape a URL optionally with parameters.
    This is effectively a wrapper around urllib.request.urlopen.
    """

    headers = {}

    if user_agent:
        headers["User-Agent"] = user_agent

    data = None
    if params:
        data = urllib.parse.urlencode(params).encode("utf-8")

    req = urllib.request.Request(url, data=data, headers=headers)

    with urllib.request.urlopen(req) as f:
        text = f.read()

    return text


def pdftoxml(pdfdata, options=""):
    """converts pdf file to xml file"""
    if not shutil.which("pdftohtml"):
        warnings.warn(
            "scraperwiki.pdftoxml requires pdftohtml, but pdftohtml was not found "
            "in the PATH. If you wish to use this function, you probably need to "
            "install pdftohtml."
        )
        return None

    with (
        tempfile.NamedTemporaryFile(suffix=".pdf") as pdffout,
        tempfile.NamedTemporaryFile(mode="r", suffix=".xml", encoding="utf-8") as xmlin,
    ):
        pdffout.write(pdfdata)
        pdffout.flush()

        tmpxml = xmlin.name  # "temph.xml"
        cmd = f'pdftohtml -xml -nodrm -zoom 1.5 -enc UTF-8 -noframes {options} "{pdffout.name}" "{os.path.splitext(tmpxml)[0]}"'
        # can't turn off output, so throw away even stderr yeuch
        cmd = cmd + " >/dev/null 2>&1"
        os.system(cmd)

        xmldata = xmlin.read()

    return xmldata


def status(type, message=None):
    """Retained for backwards compatibility."""
    warnings.warn(
        "status() is no longer in use following ScraperWiki/Quickcode application shutdown",
        DeprecationWarning,
        stacklevel=2,
    )


def swimport(scrapername):
    return __import__(scrapername)
