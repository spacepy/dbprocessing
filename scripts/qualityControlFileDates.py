#!/usr/bin/env python2.6

from optparse import OptionParser
import sys

from dbprocessing import DBUtils

def usage():
    """
    print the usage message out
    """
    print "Usage: {0} [product name or id]".format(sys.argv[0])
    print "   -> specify the product name to create a text file of the unchecked dates"
    return



def main():
    usage = \
    """usage: %prog [-f, --file= filename] [--html] product_name
        -f output filename (default <product_name>.txt/<product_name>.html)
        --html output in an html format
        product name (or ID)"""
    parser = OptionParser(usage)
    parser.add_option("-f", "--file", dest="filename",
                      help="output filename", default="{PRODUCT}.{EXT}")
    parser.add_option("", "--html", dest="html",
                      action="store_true", help="output html", default=False)

    (options, args) = parser.parse_args()
    if len(args) != 1:
        parser.error("incorrect number of arguments")

    a = DBUtils.DBUtils('rbsp')
    a._openDB()
    a._createTableObjects()
    prod_id = a.getProductID(args[0])
    prod_name = a.getProductName(prod_id)

    info = a.getFilesQC()
    files = filter(lambda x: x[3] == prod_name, info)
    dates = [v[1].isoformat() for v in files]

    if len(dates) == 0:
        sys.stderr.write("Warning: there were no dates for QC for product: {0}, no output written".format(prod_name))
        exit(0)

    if options.filename == "{PRODUCT}.{EXT}": # str replacement on default
        options.filename = options.filename.replace('{PRODUCT}', prod_name)
    if options.html:
        options.filename = options.filename.replace('{EXT}', 'html')
    else:
        options.filename = options.filename.replace('{EXT}', 'txt')

    with open(options.filename, 'w') as fp:
        if options.html:
            fp.write('<table border="1" cellpadding="2" cellspacing="2" width="100%">\n')
            fp.write('<tr>\n')
            fp.write('<td>{0}</td>\n'.format(prod_name))
            for i,v in enumerate(dates):
                if i % 4 == 0 and i != 0:
                    fp.write('</tr>\n<tr>\n<td></td>\n')
                fp.write('<td>{0}</td>\n'.format(v))
            fp.write('</tr>\n')
            fp.write('</table>\n')
        else:
            fp.write('Product: {0}\n'.format(prod_name))
            for v in dates:
                fp.write('{0}\n'.format(v))



if __name__ == "__main__":
    main()

