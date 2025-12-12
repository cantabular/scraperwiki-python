'''
Local version of ScraperWiki Utils, documentation here:
https://scraperwiki.com/docs/python/python_help_documentation/
'''
import os
import shutil
import sys
import warnings
import tempfile
import urllib.parse
import urllib.request


def scrape(url, params=None, user_agent=None):
    '''
    Scrape a URL optionally with parameters.
    This is effectively a wrapper around urllib.request.urlopen.
    '''

    headers = {}

    if user_agent:
        headers['User-Agent'] = user_agent

    data = None
    if params:
        data = urllib.parse.urlencode(params).encode('utf-8')

    req = urllib.request.Request(url, data=data, headers=headers)

    with urllib.request.urlopen(req) as f:
        text = f.read()

    return text


def pdftoxml(pdfdata, options=""):
    """converts pdf file to xml file"""
    if not shutil.which('pdftohtml'):
        warnings.warn(
            'scraperwiki.pdftoxml requires pdftohtml, but pdftohtml was not found '
            'in the PATH. If you wish to use this function, you probably need to '
            'install pdftohtml.'
        )
        return None
    pdffout = tempfile.NamedTemporaryFile(suffix='.pdf')
    pdffout.write(pdfdata)
    pdffout.flush()

    xmlin = tempfile.NamedTemporaryFile(mode='r', suffix='.xml', encoding="utf-8")
    tmpxml = xmlin.name  # "temph.xml"
    cmd = 'pdftohtml -xml -nodrm -zoom 1.5 -enc UTF-8 -noframes {} "{}" "{}"'.format(
        options, pdffout.name, os.path.splitext(tmpxml)[0])
    # can't turn off output, so throw away even stderr yeuch
    cmd = cmd + " >/dev/null 2>&1"
    os.system(cmd)

    pdffout.close()
    #xmlfin = open(tmpxml)
    xmldata = xmlin.read()
    xmlin.close()
    return xmldata


def status(type, message=None):
    """Retained for backwards compatibility."""
    warnings.warn("status() is no longer in use following ScraperWiki/Quickcode application shutdown", DeprecationWarning, stacklevel=2)
    return


def swimport(scrapername):
    return __import__(scrapername)
