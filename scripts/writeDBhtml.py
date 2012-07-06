# -*- coding: utf-8 -*-
#!/usr/bin/env python2.6

import datetime
import sys

import numpy as np

from dbprocessing import DBUtils2


def makeHTML(mission, filename):
    header = """
    <!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01//EN" "http://www.w3.org/TR/html4/strict.dtd">
    <html><head>
      <meta content="text/html; charset=ISO-8859-1" http-equiv="content-type"><title>DBprocessing</title>
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
        <table style="border: medium none ; border-collapse: collapse;" border="0" cellpadding="0" cellspacing="0">
    """
    footer = """
        </table>
    <br>
    <br>
    </body></html>
    """

    dbu = DBUtils2.DBUtils2(mission) # TODO don't assume RBSP later
    dbu._openDB()
    dbu._createTableObjects()

    prods = dbu.getProducts()
    data = {}
    keys1 = ['level', 'product_name', 'relative_path', 'format']
    for key in keys1:
        data[key] = [v.__getattribute__(key) for v in prods]

    keys2 = ['instrument_name']
    for key in keys2:
        data[key] = [dbu.getProductTraceback(v.product_id)['instrument'].__getattribute__(key) for v in prods]

    keys3 = ['satellite_name']
    for key in keys3:
        data[key] = [dbu.getProductTraceback(v.product_id)['satellite'].__getattribute__(key) for v in prods]

    dt = [datetime.datetime(1950, 1, 1), datetime.datetime(2050, 1, 1)]

    keys4 = ['n_files']
    for key in keys4:
        data[key] = [len(dbu.getFilesByProduct(v.product_id)) for v in prods]

    keys5 = ['latest']
    for key in keys5:
        data[key] = []
        for p in prods:
            tmp = dbu.getFilesByProduct(p.product_id)
            tmp = [t.utc_stop_time for t in tmp]
            tmp.sort()
            try:
                data[key].append(tmp[-1].isoformat())
            except IndexError:
                data[key].append(None)

    keys = keys3 + keys2 + keys1[:-1] + keys4 + keys5 + [keys1[-1]]

    # go through and sort the data by several keys
    ind = np.lexsort( (data['product_name'], data['level'], data['instrument_name'], data['satellite_name'],   ) )

    output = open(filename, 'w')
    output.writelines(header)

    output.write('<h1>{0}</h1>\n'.format(dbu.mission))
    output.write('<h2>{0}</h1>\n'.format(datetime.datetime.now().isoformat()))

    for attr in keys:
        output.write('<th>{0}</th>'.format(attr))
    output.write('</tr>')
    for i, idx in enumerate(ind):
        if i % 2 == 0:
            output.write('<tr>')
        else:
            output.write('<tr class="alt">')
        output.write('<td>{0}</td>'.format(data['satellite_name'][idx]))
        output.write('<td>{0}</td>'.format(data['instrument_name'][idx]))
        output.write('<td>{0}</td>'.format(data['level'][idx]))
        output.write('<td>{0}</td>'.format(data['product_name'][idx]))
        output.write('<td>{0}</td>'.format(data['relative_path'][idx]))
        output.write('<td>{0}</td>'.format(data['n_files'][idx]))
        output.write('<td>{0}</td>'.format(data['latest'][idx]))
        output.write('<td>{0}</td>'.format(data['format'][idx]))
        output.write('</tr>\n')


    output.writelines(footer)
    output.close()



def usage():
    """
    print the usage messag out
    """
    print "Usage: {0} <mission> <filename>".format(sys.argv[0])
    print "   -> mission name to write to html"
    return


if __name__ == "__main__":
    if len(sys.argv) != 3:
        usage()
        sys.exit(2)
    makeHTML(sys.argv[1], sys.argv[2])
