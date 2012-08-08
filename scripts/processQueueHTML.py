#!/usr/bin/env python2.6

import datetime
import sys

from dbprocessing import DBUtils2


def _unicodeListToStrList(lst):
    """
    take a list of unicode and change it to a list of string
    """
    return [str(item) for item in lst]

def usage():
    """
    print the usage messag out
    """
    print "Usage: {0} <mission> <filename>".format(sys.argv[0])
    print "   -> mission name to write to html"
    return


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

    """
    footer = \
    """
    <br>
    <br>
    </body></html>
    """

    dbu = DBUtils2.DBUtils2(mission) # TODO don't assume RBSP later
    dbu._openDB()
    dbu._createTableObjects()

    output = open(filename, 'w')
    output.writelines(header)
    _writeQueue(dbu, output)
    output.writelines(footer)
    output.close()

def _writeQueue(dbu, output):
    n_items = dbu.processqueueLen()
    items = dbu.processqueueGetAll()

    traceback = []
    for v in items:
        traceback.append(dbu.getFileTraceback(v))

    data = {}

    data['file_id'] = []
    data['filename'] = []
    data['product'] = []

    keys = ['file_id', 'filename', 'product', ] # need order so write then out

    for tb in traceback:
        for key in keys:
            if key == 'file_id':
                data[key].append(tb['file'].file_id)
            elif key == 'filename':
                data[key].append(tb['file'].filename)
            elif key == 'product':
                data[key].append(tb['product'].product_name)

    output.write('<h1>{0}</h1>\n'.format(dbu.mission))
    output.write('<h2>{0}</h2>\n'.format(datetime.datetime.now().isoformat()))

    output.write('<h2>{0}</h2>\n'.format('processQueue'))

    output.write('<table style="border: medium none ; border-collapse: collapse;" border="0" cellpadding="0" cellspacing="0">\n')
    # write out the header
    output.write('<tr>')
    for attr in keys:
        output.write('<th>{0}</th>'.format(attr))
    output.write('</tr>\n')

    # and write all the data
    for i, idx in enumerate(data['file_id']):
        output.write('\n')
        if i % 2 == 0:
            output.write('<tr>')
        else:
            output.write('<tr class="alt">')
        for key in keys:
            output.write('<td>{0}</td>'.format(data[key][i]))

    output.write('</table>\n')
    output.write('\n\n\nNote: Lower versions of the same file are not processed')


if __name__ == "__main__":
    if len(sys.argv) != 3:
        usage()
        sys.exit(2)
    makeHTML(sys.argv[1], sys.argv[2])
