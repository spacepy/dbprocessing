#!/usr/bin/env python
from __future__ import print_function

import argparse
import datetime
import sys

from dbprocessing import DButils

def output_html(items):
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

    output += '    <h2>{0}</h2>\n'.format('processQueue')
    output +="""    <table>
        <tr><th>file_id</th><th>filename</th><th>product</th></tr>
"""
    for index, item in enumerate(items):
        if( index % 2 == 0 ):
            output += '        <tr>'
        else:
            output += "        <tr class='alt'>"

        output += '<td>{0}</td><td>{1}</td><td>{2}</td></tr>\n'.format(index, item['file'].filename, item['product'].product_name)

    output += "    </table>\n  </body>\n</html>"
    return output

def output_text(items):
    output = dbu.mission + '\n'
    output += datetime.datetime.utcnow().isoformat() + '\n'
    output += 'ProcessQueue\n'

    for index, item in enumerate(items):
        output += '{0}\t{1}\t{2}\n'.format(index, item['file'].filename, item['product'].product_name)

    return output

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--html", action='store_true',
                        help="Output in html",
                        default=False)
    parser.add_argument("-e", "--exist",  action='store_true',
                        help="return 1 if processqueue is empty 0 otherwise",
                        default=False)
    parser.add_argument("-q", "--quiet",  action='store_true',
                        help="Do not display output", 
                        default=False)
    parser.add_argument("-c", "--count", action='store_true',
                        help="Count how many files in the process queue", 
                        default=False)
    parser.add_argument("-o", "--output",
                        help="Write output to a file")
    parser.add_argument(
        'database', action='store', type=str,
        help='Name of database (or sqlite file), i.e. mission, to open.')

    options = parser.parse_args()

    dbu = DButils.DButils(options.database)
    items = dbu.ProcessqueueGetAll()
     
    # Check if the processque is empty 
    # Usually used for bash scripts
    #   return 0 if data exists (true)
    #   return 1 if processque is empty (false)
    if options.exist:
        if items: 
            print("Process Queue is not empty\n" if not options.quiet else "", end='')
            del dbu
            sys.exit(0)
        else:
            del dbu
            sys.exit(1)

    # return the number of items in processqueue 
    if options.count:
         print("Number of items in processqueue %d\n" % len(items) if not options.quiet else "", end='')
         del dbu
         sys.exit(len(items))

    traceback = []
    for v in items:
        traceback.append(dbu.getTraceback('File', v))

    if options.html:
        out = output_html(traceback)
    else:
        out = output_text(traceback)

    if options.output is None:
        print(out)
    else:
        output = open(options.output, 'w')
        output.write(out)
        output.close()
    del dbu
