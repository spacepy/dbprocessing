#!/usr/bin/env python2.6

from optparse import OptionParser
import sys

from dbprocessing import DBUtils2

def usage():
    """
    print the usage message out
    """
    print "Usage: {0} [product name or id]".format(sys.argv[0])
    print "   -> specify the product name to create a text file of the unchecked dates"
    return



def main():
    usage = \
    """usage: %prog [-f, --file= filename] product_name
        -f output filename (default QC_dates.txt)
        product name (or ID)"""
    parser = OptionParser(usage)
    parser.add_option("-f", "--file", dest="filename",
                      help="output filename", default="QC_dates.txt")
    (options, args) = parser.parse_args()
    if len(args) != 1:
        parser.error("incorrect number of arguments")

    a = DBUtils2.DBUtils2('rbsp')
    a._openDB()
    a._createTableObjects()
    prod_id = a._getProductID(args[0])
    prod_name = a.getProductName(prod_id)

    info = a.getFilesQC()
    files = filter(lambda x: x[3] == prod_name, info)
    dates = [v[1].isoformat() for v in files]

    if len(dates) == 0:
        sys.stderr.write("Warning: there were no dates for QC for product: {0}, no output written".format(prod_name))
        exit(0)

    with open(options.filename, 'w') as fp:
        fp.write('Product: {0}\n'.format(prod_name))
        for v in dates:
            fp.write('{0}\n'.format(v))



if __name__ == "__main__":
    main()


