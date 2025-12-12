#!/usr/bin/env python
from __future__ import absolute_import

# This file is part of Local Scraperlibs.

import warnings
import os
from distutils.core import setup


def has_external_dependency(name):
    'Check that a non-Python dependency is installed.'
    for directory in os.environ['PATH'].split(':'):
        if os.path.exists(os.path.join(directory, name)):
            return True
    return False

if not has_external_dependency('pdftohtml'):
    warnings.warn(
        'scraperwiki.pdftoxml requires pdftohtml, but pdftohtml was not found '
        'in the PATH. If you wish to use this function, you probably need to '
        'install pdftohtml.'
    )

config = dict(name='scraperwiki',
              author='Francis Irving',
              author_email='francis@sensiblecode.io',
              description='Local version of ScraperWiki libraries',
              url='https://github.com/scraperwiki/scraperwiki-python',
              classifiers=[
              'Intended Audience :: Developers',
              'Intended Audience :: Science/Research',
              'License :: OSI Approved :: '
              'GNU General Public License v3 or later (GPLv3+)',
              'Programming Language :: Python :: 2.7',
              'Programming Language :: Python :: 3.4',
              'Programming Language :: Python :: 3.5',
              'Programming Language :: SQL',
              'Topic :: Database :: Front-Ends',
              ],
              packages=['scraperwiki'],
              version='0.5.1',
              license='GPL',
              )

try:
    from setuptools import setup
    config['install_requires'] = ['requests', 'six',
                                  'sqlalchemy==1.3.0', 'alembic==1.14.0'],
except ImportError:
    pass

setup(**config)
