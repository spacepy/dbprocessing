#!/usr/bin/env python
from __future__ import print_function

import argparse
import datetime
import sys

from dbprocessing import DButils

def output_html(items, products=None):
    output = """<!DOCTYPE html>
<html>
  <head>
    <title>DBprocessing</title>
    <style type="text/css">
        table, td, th
        {
        border:1px solid green;
        padding:3px 7px 2px 7px;
        border-collapse: collapse;
        }
        th
        {
        background-color:green;
        color:white;
        }
        tr.alt td
        {
        color:#000000;
        background-color:#EAF2D3;
        }
    </style>
  </head>


  <body>
"""
    output += '    <h1>{0}</h1>\n'.format(dbu.mission)
    output += '    <h2>{0}</h2>\n'.format(datetime.datetime.utcnow().isoformat())

    if products:
        output += '    <h2>{0}</h2>\n'.format('Products')
        output +="""    <table>
        <tr><th>product_id</th><th>product</th></tr>
"""
        for i, prod in enumerate(products):
            output += '        <tr{}>'.format(
                " class='alt'" if i % 2 else '')
            output += '<td>{0}</td><td>{1}</td></tr>\n'.format(
                prod.product_id, prod.product_name)
        output += "    </table>\n"
    output += '    <h2>{0}</h2>\n'.format('processQueue')
    output +="""    <table>
        <tr><th>file #</th><th>filename</th><th>product</th></tr>
"""
    for index, item in enumerate(items):
        if( index % 2 == 0 ):
            output += '        <tr>'
        else:
            output += "        <tr class='alt'>"

        output += '<td>{0}</td><td>{1}</td><td>{2}</td></tr>\n'.format(index, item['file'].filename, item['product'].product_name)

    output += "    </table>\n  </body>\n</html>"
    return output

def output_text(items, products=None):
    output = dbu.mission + '\n'
    output += datetime.datetime.utcnow().isoformat() + '\n'
    if products:
        output += 'Products\n{}\n'.format('\n'.join([
            '{}\t{}'.format(p.product_id, p.product_name)
            for p in products]))
    output += 'ProcessQueue\n'
    for index, item in enumerate(items):
        output += '{0}\t{1}\t{2}\n'.format(index, item['file'].filename, item['product'].product_name)

    return output

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--html", action='store_true',
                        help="Output in html")
    parser.add_argument("-q", "--quiet",  action='store_true',
                        help="Do not display output")
    parser.add_argument("-o", "--output",
                        help="Write output to a file")
    parser.add_argument(
        'database', action='store', type=str,
        help='Name of database (or sqlite file), i.e. mission, to open.')
    parser.add_argument(
        '-p', '--product', nargs='+',
        help='Limit result and counts to these product IDs or names.')
    parser.add_argument(
        "-s", "--sort", action='store_true',
        help="Sort output by date, then product (default: in queue order).")
    returncode = parser.add_mutually_exclusive_group()
    returncode.add_argument("-e", "--exist",  action='store_true',
                            help="Exit code 1 if queue empty; 0 (True) if not.")
    returncode.add_argument("-c", "--count", action='store_true',
                            help="Set exit code to count of files in queue.")
    options = parser.parse_args()
    if options.quiet and (options.html or options.output or options.sort):
        parser.error('--html, -o, and -s are useless with -q.')

    dbu = DButils.DButils(options.database)
    items = dbu.ProcessqueueGetAll()
    traceback = [dbu.getTraceback('File', v) for v in items]
    products = None if options.product is None\
               else [dbu.getEntry('Product', p) for p in options.product]
    if products:
        prod_ids = [p.product_id for p in products]
        traceback = [tb for tb in traceback if tb['product'].product_id
                     in prod_ids]
    if options.sort:
        traceback.sort(key=lambda x: (x['file'].utc_file_date,
                                      x['product'].product_name))
    if not options.quiet:
        out = (output_html if options.html else output_text)(
            traceback, products)
        if options.output is None:
            print(out)
        else:
            with open(options.output, 'w') as output:
                output.write(out)
    del dbu
    if options.exist:
        sys.exit(not(traceback))
    if options.count:
        sys.exit(min(len(traceback), 255))
