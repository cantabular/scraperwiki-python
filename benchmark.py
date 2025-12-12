#! /usr/bin/env python3
import scraperwiki
import os

rows = [{'id': i, 'test': i * 2, 's': "abc"} for i in range(1000)]

try:
    os.remove('scraperwiki.sqlite')
except FileNotFoundError:
    pass

scraperwiki.sql.save(['id'], rows)

for i, row in enumerate(rows):
    scraperwiki.sql.save(['id'], row)
