#!/usr/bin/env python

"""Dump a database contents to a JSON file.

Used to convert unit tests that relied on having a specific sqlite database
to ones which load the database from human-readable data.

First argument: database name, usually sqlite file.
Second argument: path to JSON file to dump to.
"""

import datetime
import json
import sys

import dbprocessing.DButils

dbu = dbprocessing.DButils.DButils(sys.argv[1])
res = {}
for t in dbu.metadata.sorted_tables:
    res[t.name] = [dict(row) for row in dbu.engine.execute(t.select())]
for tablename, tablecontents in res.items():
    if not tablecontents:
        continue
    for column in tablecontents[0]:
        # datetime can't go to JSON
        if isinstance(tablecontents[0][column],
                      (datetime.datetime, datetime.date)):
            for i in tablecontents:
                i[column] = i[column].strftime('%Y-%m-%dT%H:%M:%S.%f')
        # Some numerical columns erronously have an empty string
        if column in ('output_product', ):
            for i in tablecontents:
                if i[column] == '':
                    i[column] = None
            
with open(sys.argv[2], 'w') as f:
    json.dump(res, f, indent=4, sort_keys=True)
