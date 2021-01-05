#!/usr/bin/env python

from __future__ import print_function


import datetime
import glob
import os
import sys
import re

import rbsp #rbsp.mission_day_to_UTC

from dbprocessing import DButils, Utils, inspector
from rbsp import Version


hope_dir = os.path.join('/', 'n', 'space_data', 'cda', 'rbsp')
a_products = [12, 37, 43, 47, 50] # list of product ids # TODO hardcoded for now IN ORDER
b_products = [29, 40, 46, 48, 52] # list of product ids # TODO hardcoded for now IN ORDER
dirs = [os.path.join('{sc}', 'hope', 'level0', '*22e*'),
        os.path.join('{sc}', 'hope', 'level05', '*sci*'),
        os.path.join('{sc}', 'hope', 'level1', 'pre', '*sci*'),
        os.path.join('{sc}', 'hope', 'level2', 'pre', '*sci*'),
        os.path.join('{sc}', 'hope', 'level3', 'pre', '*-PA-*')]

dbu = None

def gather_files():
    """
    go through the directories and grab out the list of files (highest version number)
    for each of the above products and do that
    """
    a_files = {}
    for ii, product in enumerate(a_products):
        a_files[product] = dbu.getFilesByProduct(product, newest_version=True)
                                    
    b_files = {}
    for ii, product in enumerate(b_products):
        b_files[product] = dbu.getFilesByProduct(product, newest_version=True)

    return a_files, b_files




def _unicodeListToStrList(lst):
    """
    take a list of unicode and change it to a list of string
    """
    return [str(item) for item in lst]

def usage():
    """
    print the usage message out
    """
    print("Usage: {0} <mission> <filename>".format(sys.argv[0]))
    print("   -> mission db file to write to html")
    return


def makeHTML(mission, filename):
    global dbu
    header = """
    <!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01//EN" "http://www.w3.org/TR/html4/strict.dtd">
    <html><head>
      <meta content="text/html; charset=ISO-8859-1" http-equiv="content-type"><title>HOPE Coverage</title>
        <style type="text/css">
            table, td, th
            {
            border:1px solid green;
            padding:3px 7px 2px 7px;
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
    footer = \
    """
    <br>
    <br>
    </body></html>
    """

    dbu = DButils.DButils(mission)
    a, b, = gather_files()

    output = open(filename, 'w')
    output.writelines(header)
    _writeTable(dbu, output, a, b)
    output.writelines(footer)
    output.close()

def _writeTable(dbu, output, a, b):

    dates = [datetime.datetime(2012, 11, 1) + datetime.timedelta(days = v) for v in range((datetime.datetime.utcnow().date() - datetime.date(2012, 11, 1)).days)]
    a_date = {}
    a_version = {}
    for key in a:
        a_date[key] = [v.utc_file_date for v in a[key]]
        a_version[key] = [Version.Version(v.interface_version, v.quality_version, v.revision_version) for v in a[key]]

    b_date = {}
    b_version = {}
    for key in b:
        b_date[key] = [v.utc_file_date for v in b[key]]
        b_version[key] = [Version.Version(v.interface_version, v.quality_version, v.revision_version) for v in b[key]]


    output.write('<h1>{0}</h1>\n'.format(dbu.mission))
    output.write('<h2>{0}</h2>\n'.format(datetime.datetime.utcnow().isoformat()))

    output.write('<h2>{0}</h2>\n'.format('Files Present'))

    output.write('<table style="border: medium none ; border-collapse: collapse;" border="0" cellpadding="0" cellspacing="0">\n')
    # write out the header
    output.write('<tr>')
    output.write('<td></td>')
    output.write('<td></td>')

    output.write('<th></th>')

    output.write('<th>RBSPA</th>')
    output.write('<th></th>')
    output.write('<th></th>')
  
    output.write('<th>{0}</th>'.format(' ------- '))
    output.write('<th></th>')

    output.write('<th>RBSPB</th>')
    output.write('<th></th>')
    output.write('<th></th>')
    output.write('</tr>\n')


    output.write('<tr>')
    output.write('<td></td>')
    output.write('<td></td>')

    for prod in a_products:
        p = dbu.getEntry('Product', prod)
        output.write('<th>{0}</th>'.format(p.product_name))
    output.write('<td></td>')

    for prod in b_products:
        p = dbu.getEntry('Product', prod)
        output.write('<th>{0}</th>'.format(p.product_name))

    output.write('</tr>\n')

    output.write('<tr>')
    output.write('<td></td>')
    output.write('<td></td>')

    for prod in a_products:
        p = dbu.getEntry('Product', prod)
        output.write('<th>{0}</th>'.format(p.product_id))
    output.write('<td></td>')
    for prod in b_products:
        p = dbu.getEntry('Product', prod)
        output.write('<th>{0}</th>'.format(p.product_id))

    output.write('</tr>\n')
    #for attr in keys:
    #    output.write('<th>{0}</th>'.format('Date'))
    #    output.write('<th>{0}</th>'.format('Date'))
    #    output.write('<th>{0}</th>'.format('Date'))
    #output.write('</tr>\n')

    # and write all the data
    for ii, d in enumerate(dates[::-1]):
        if ii % 2 == 0:
            output.write('<tr>')
        else:
            output.write('<tr class="alt">')            
        output.write('<td>{0}</td>'.format(d.date().isoformat())) 
        output.write('<td>{0}</td>'.format(int(rbsp.UTC_to_mission_day('a', d))))


        for prod in a_products:
            try:
                aind = a_date[prod].index(d.date())  
                output.write('<td>{0}</td>'.format(a_version[prod][aind].tolist())) 
            except ValueError:
                output.write('<td>{0}</td>'.format(None))

        output.write('<td>{0}</td>'.format(' ------- '))
       
        for prod in b_products:
            try:
                bind = b_date[prod].index(d.date())  
                output.write('<td>{0}</td>'.format(b_version[prod][bind].tolist())) 
            except ValueError:
                output.write('<td>{0}</td>'.format(None))


        output.write('</tr>\n')

 


    output.write('</table>\n')


if __name__ == "__main__":
    if len(sys.argv) != 3:
        usage()
        sys.exit(2)
    makeHTML(sys.argv[1], sys.argv[2])
