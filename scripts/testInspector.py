#!/usr/bin/env python

import argparse
import imp

from dbprocessing import inspector, DButils
from dbprocessing.Utils import strargs_to_args

if __name__ == '__main__':
    usage = "usage: %prog -m mission -i inspector -p product_id -f file [-a args]"
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mission", required=True,
                      help="selected mission database")
    parser.add_argument("-f", "--file", required=True,
                      help="The file to test the inspector on")
    parser.add_argument("-i", "--inspector", required=True,
                      help="The inspector to test")
    parser.add_argument("-p", "--product", required=True,
                      help="The product the file belongs to")
    parser.add_argument("-a", "--args", dest="args",
                      help="kwargs to pass to the inspector(optional)", default=None)

    options = parser.parse_args()

    dbu = DButils.DButils(options.mission)

    inspect = imp.load_source('inspect', options.inspector)

    if options.args:
        kwargs = strargs_to_args(options.args)
        df = inspect.Inspector(options.file, dbu, options.product, **kwargs)()
    else:
        df = inspect.Inspector(options.file, dbu, options.product, )()
    print(df)
    dbu.closeDB()
    del dbu
