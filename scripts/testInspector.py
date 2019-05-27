#!/usr/bin/env python

import imp
from optparse import OptionParser

from dbprocessing import inspector, DButils
from dbprocessing.Utils import strargs_to_args

if __name__ == '__main__':
    usage = "usage: %prog -m mission -i inspector -p product_id -f file [-a args]"
    parser = OptionParser(usage=usage)
    parser.add_option("-m", "--mission", dest="mission",
                      help="selected mission database")
    parser.add_option("-f", "--file", dest="file",
                      help="The file to test the inspector on")
    parser.add_option("-i", "--inspector", dest="inspector",
                      help="The inspector to test")
    parser.add_option("-p", "--product", dest="product",
                      help="The product the file belongs to")
    parser.add_option("-a", "--args", dest="args",
                      help="kwargs to pass to the inspector(optional)", default=None)

    (options, args) = parser.parse_args()

    dbu = DButils.DButils(options.mission)

    inspect = imp.load_source('inspect', options.inspector)

    if options.args:
        kwargs = strargs_to_args(options.args)
        df = inspect.Inspector(options.file, dbu, options.product, **kwargs)
    else:
        df = inspect.Inspector(options.file, dbu, options.product, )
    print(df)