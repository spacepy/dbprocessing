#!/usr/bin/env python
from __future__ import print_function

import datetime
import glob
import os
import sys
import re

import dateutil

import rbsp #rbsp.mission_day_to_UTC

from dbprocessing import Utils, inspector
from rbsp import Version

from dbprocessing import DButils

dbu = DButils.DButils('~ectsoc/MagEphem_processing.sqlite')

prods = dbu.getAllProducts()
ids = [v.product_id for v in prods]
names = [v.product_name for v in prods]
sc = [dbu.getTraceback('Product', v.product_id)['satellite'].satellite_name for v in prods]

a_files = []
b_files = []
a_dates = []
b_dates = []

a_products = []
b_products = []
a_versions = []
b_versions = []


aaaa = []
bbbb = []

aa = {}
bb = {}

for i, p in enumerate(prods):
    if sc[i] == 'rbspa':
        a_products.append(ids[i])
        aa[ids[i]] = {}
        for file in dbu.getFilesByProduct(ids[i]):
            aa[ids[i]][file.utc_file_date] = Version.Version(file.interface_version, file.quality_version, file.revision_version)
            aaaa.append(file)
            a_dates.append(file.utc_file_date)
            a_files.append(file.filename)
            a_versions.append(Version.Version(file.interface_version, file.quality_version, file.revision_version))
    if sc[i] == 'rbspb':
        b_products.append(ids[i])
        bb[ids[i]] = {}
        for file in dbu.getFilesByProduct(ids[i]):
            bb[ids[i]][file.utc_file_date] = Version.Version(file.interface_version, file.quality_version, file.revision_version)
            bbbb.append(file)
            b_dates.append(file.utc_file_date)
            b_files.append(file.filename)
            b_versions.append(Version.Version(file.interface_version, file.quality_version, file.revision_version))

aaaa = sorted(aaaa, key=lambda x: x.utc_file_date)
bbbb = sorted(bbbb, key=lambda x: x.utc_file_date)


        


def _unicodeListToStrList(lst):
    """
    take a list of unicode and change it to a list of string
    """
    return [str(item) for item in lst]

def usage():
    """
    print the usage message out
    """
    print("Usage: {0} <filename>".format(sys.argv[0]))
    print("   -> mission db file to write to html")
    return


def makeHTML(filename):

    header = """
    <!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01//EN" "http://www.w3.org/TR/html4/strict.dtd">
    <html><head>
      <meta content="text/html; charset=ISO-8859-1" http-equiv="content-type"><title>MagEphem Pre Coverage</title>
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

    output = open(filename, 'w')
    output.writelines(header)

    a = 0
    
    _writeTable(dbu, output)
    output.writelines(footer)
    output.close()

def _writeTable(dbu, output):
    dates = dateutil.rrule.rrule(dateutil.rrule.DAILY, dtstart=datetime.date(2012, 8, 31), until=datetime.datetime.utcnow().date() + datetime.timedelta(days=25))

    output.write('<h1>{0}</h1>\n'.format(dbu.mission))
    output.write('<h2>{0}</h2>\n'.format('Preliminary MagEphem'))
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


#    output.write('<tr>')
#    output.write('<td></td>')
#    output.write('<td></td>')


    def make_header(short=False):
        output.write('<tr>')
        output.write('<td></td>')
        output.write('<td></td>')
        for prod in a_products:
            p = dbu.getEntry('Product', prod)
            output.write('<th>{0}{1}</th>'.format(p.product_name.split('_')[-1], os.path.splitext(p.product_name)[1] ))
        output.write('<td></td>')

        for prod in b_products:
            p = dbu.getEntry('Product', prod)
            output.write('<th>{0}{1}</th>'.format(p.product_name.split('_')[-1], os.path.splitext(p.product_name)[1] ))

        output.write('</tr>\n')

        if not short:

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

    #make_header()

    # and write all the data
    for ii, d in enumerate(dates[::-1]):
        if d.day == 1:
             make_header()
        if ii % 2 == 0:
            output.write('<tr>')
        else:
            output.write('<tr class="alt">')            
        output.write('<td>{0}</td>'.format(d.date().isoformat())) 
        output.write('<td>{0}</td>'.format(int(rbsp.UTC_to_mission_day('a', d))))

        for p in a_products:
            if d.date() in aa[p]:
                output.write('<td>{0}</td>'.format(aa[p][d.date()])) 
            else:
                output.write('<td>{0}</td>'.format(None)) 

        output.write('<td>{0}</td>'.format('')) 

#        output.write('<td>{0}</td>'.format(d.date().isoformat())) 
#        output.write('<td>{0}</td>'.format(int(rbsp.UTC_to_mission_day('b', d))))

        
        for p in b_products:
            if d.date() in bb[p]:
                output.write('<td>{0}</td>'.format(bb[p][d.date()])) 
            else:
                output.write('<td>{0}</td>'.format(None)) 

        
    ##     for prod in a_products:
##             try:
##                 aind = a_date[prod].index(d.date())  
##                 output.write('<td>{0}</td>'.format(a_version[prod][aind].tolist())) 
##             except ValueError:
##                 output.write('<td>{0}</td>'.format(None))

##         output.write('<td>{0}</td>'.format(' ------- '))
       
##         for prod in b_products:
##             try:
##                 bind = b_date[prod].index(d.date())  
##                 output.write('<td>{0}</td>'.format(b_version[prod][bind].tolist())) 
##             except ValueError:
##                 output.write('<td>{0}</td>'.format(None))


        output.write('</tr>\n')

 


    output.write('</table>\n')


if __name__ == "__main__":
    if len(sys.argv) != 2:
        usage()
        sys.exit(2)
    makeHTML(sys.argv[1])
