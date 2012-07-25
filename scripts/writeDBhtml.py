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

    """
    footer = """
    <br>
    <br>
    </body></html>
    """

    dbu = DBUtils2.DBUtils2(mission) # TODO don't assume RBSP later
    dbu._openDB()
    dbu._createTableObjects()

    output = open(filename, 'w')
    output.writelines(header)
    _writeProducts(dbu, output)
    _writeProcesses(dbu, output)
    output.writelines(footer)
    output.close()


#def _writeProducts(dbu, output):
#    prods = dbu.getProducts()
#    data = {}
#    keys1 = ['level', 'product_name', 'product_description', 'relative_path', 'format']
#    for key in keys1:
#        data[key] = [v.__getattribute__(key) for v in prods]
#
#    keys2 = ['instrument_name']
#    for key in keys2:
#        data[key] = [dbu.getProductTraceback(v.product_id)['instrument'].__getattribute__(key) for v in prods]
#
#    keys3 = ['satellite_name']
#    for key in keys3:
#        data[key] = [dbu.getProductTraceback(v.product_id)['satellite'].__getattribute__(key) for v in prods]
#
#    keys4 = ['n_files']
#    for key in keys4:
#        data[key] = [len(dbu.getFilesByProduct(v.product_id)) for v in prods]
#
#    keys5 = ['latest']
#    for key in keys5:
#        data[key] = []
#        for p in prods:
#            tmp = dbu.getFilesByProduct(p.product_id)
#            tmp = [t.utc_stop_time for t in tmp]
#            tmp.sort()
#            try:
#                data[key].append(tmp[-1].isoformat())
#            except IndexError:
#                data[key].append(None)
#
#    keys = keys3 + keys2 + keys1[:-1] + keys4 + keys5 + [keys1[-1]]
#
#    # go through and sort the data by several keys
#    ind = np.lexsort( (data['product_name'], data['level'], data['instrument_name'], data['satellite_name'],   ) )
#
#    output.write('<h1>{0}</h1>\n'.format(dbu.mission))
#    output.write('<h2>{0}</h2>\n'.format(datetime.datetime.now().isoformat()))
#
#    output.write('<h2>{0}</h2>\n'.format('Products'))
#
#    output.write('<table style="border: medium none ; border-collapse: collapse;" border="0" cellpadding="0" cellspacing="0">\n')
#
#    output.write('<tr>')
#    for attr in keys:
#        output.write('<th>{0}</th>'.format(attr))
#    output.write('</tr>\n')
#    for i, idx in enumerate(ind):
#        if i % 2 == 0:
#            output.write('<tr>')
#        else:
#            output.write('<tr class="alt">')
#        output.write('<td>{0}</td>'.format(data['satellite_name'][idx]))
#        output.write('<td>{0}</td>'.format(data['instrument_name'][idx]))
#        output.write('<td>{0}</td>'.format(data['level'][idx]))
#        output.write('<td>{0}</td>'.format(data['product_name'][idx]))
#        output.write('<td>{0}</td>'.format(data['relative_path'][idx]))
#        output.write('<td>{0}</td>'.format(data['n_files'][idx]))
#        output.write('<td>{0}</td>'.format(data['latest'][idx]))
#        output.write('<td>{0}</td>'.format(data['format'][idx]))
#        output.write('</tr>\n')
#
#    output.write('</table>\n')

def _writeProducts(dbu, output):
    prods = dbu.getAllProducts()

    traceback = []
    for prod in prods:
        traceback.append(dbu.getProductTraceback(prod.product_id))

    data = {}

    data['satellite_name'] = []
    data['instrument_name'] = []
    data['level'] = []
    data['product_name'] = []
    data['product_description'] = []
    data['relative_path'] = []
    data['n_files'] = []
    data['format'] = []

    keys = ['satellite_name', 'instrument_name', 'level', 'product_name',
            'product_description', 'relative_path', 'n_files', 'format', ]

    for tb in traceback:
        for key in keys:
            if key == 'satellite_name':
                data[key].append(tb['satellite'].satellite_name)
            elif key == 'instrument_name':
                data[key].append(tb['instrument'].instrument_name)
            elif key == 'level':
                data[key].append(tb['product'].level)
            elif key == 'product_name':
                data[key].append(tb['product'].product_name)
            elif key == 'product_description':
                data[key].append(tb['product'].product_description)
            elif key == 'relative_path':
                data[key].append(tb['product'].relative_path)
            elif key == 'format':
                data[key].append(tb['product'].format)
            elif key == 'n_files':
                data[key].append(len(dbu.getFilesByProduct(tb['product'].product_id)))

    # go through and sort the data by several keys (right to left, ugg)
    ind = np.lexsort( (data['product_name'], data['level'], data['instrument_name'], data['satellite_name'],   ) )


    output.write('<h2>{0}</h2>\n'.format('Processes'))

    output.write('<table style="border: medium none ; border-collapse: collapse;" border="0" cellpadding="0" cellspacing="0">\n')
    # write out the header
    output.write('<tr>')
    for attr in keys:
        output.write('<th>{0}</th>'.format(attr))
    output.write('</tr>\n')

    # and write all the data
    for i, idx in enumerate(ind):
        if i % 2 == 0:
            output.write('<tr>')
        else:
            output.write('<tr class="alt">')
        for key in keys:
            output.write('<td>{0}</td>'.format(data[key][idx]))

    output.write('</table>\n')

def _writeProcesses(dbu, output):
    procs = dbu.getAllProcesses()

    traceback = []
    for proc in procs:
        traceback.append(dbu.getProcessTraceback(proc.process_id))

    data = {}

    data['satellite_name'] = []
    data['instrument_name'] = []
    data['process_name'] = []
    data['code_name'] = []
    data['code_description'] = []
    data['relative_path'] = []
    data['arguments'] = []
    data['output_product'] = []
    data['required_input_products'] = []
    data['optional_input_products'] = []

    keys = ['satellite_name', 'instrument_name', 'process_name', 'code_name',
            'code_description', 'relative_path', 'arguments', 'output_product',
            'required_input_products', 'optional_input_products'] # this is needed later so might as well make it now

    for tb in traceback:
        for key in keys:
            if key == 'satellite_name':
                data[key].append(tb['satellite'].satellite_name)
            elif key == 'process_name':
                data[key].append(tb['process'].process_name)
            elif key == 'code_name':
                data[key].append(tb['code'].filename)
            elif key == 'code_description':
                data[key].append(tb['code'].code_description)
            elif key == 'relative_path':
                data[key].append(tb['code'].relative_path)
            elif key == 'arguments':
                data[key].append(tb['code'].arguments)
            elif key == 'output_product':
                data[key].append(dbu.session.query(dbu.Product).get(tb['output_product'].product_id).product_name)
            elif key == 'instrument_name':
                data[key].append(tb['instrument'].instrument_name)
            elif key == 'required_input_products':
                tmp = []
                for inp, opt in tb['input_product']:
                    if opt:
                        continue
                    tmp.append(inp.product_name)
                data[key].append(_unicodeListToStrList(tmp))
            elif key == 'optional_input_products':
                tmp = []
                for inp, opt in tb['input_product']:
                    if not opt:
                        continue
                    tmp.append(inp.product_name)
                data[key].append(_unicodeListToStrList(tmp))

    # go through and sort the data by several keys (right to left, ugg)
    ind = np.lexsort( (data['code_name'], data['instrument_name'], data['satellite_name'],   ) )


    output.write('<h2>{0}</h2>\n'.format('Processes'))

    output.write('<table style="border: medium none ; border-collapse: collapse;" border="0" cellpadding="0" cellspacing="0">\n')
    # write out the header
    output.write('<tr>')
    for attr in keys:
        output.write('<th>{0}</th>'.format(attr))
    output.write('</tr>\n')

    # and write all the data
    for i, idx in enumerate(ind):
        if i % 2 == 0:
            output.write('<tr>')
        else:
            output.write('<tr class="alt">')
        for key in keys:
            output.write('<td>{0}</td>'.format(data[key][idx]))

    output.write('</table>\n')

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


if __name__ == "__main__":
    if len(sys.argv) != 3:
        usage()
        sys.exit(2)
    makeHTML(sys.argv[1], sys.argv[2])
