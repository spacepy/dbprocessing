#!/usr/bin/env python


# following the recipe in the document I wrote


#==============================================================================
# INPUTS
#==============================================================================
# mission name
# satellite name
# product name
## prod name
## product rel path
## prod filename format
## prod level
# <- add the prod
# <- create the inst_prod link
from __future__ import print_function

import ConfigParser
import sys

from dbprocessing import DButils

sections = ['base', 'product', 'inspector',]



def writeconfig(my_cfg, config_filepath):
    cfg=ConfigParser.ConfigParser()
    for section in my_cfg:
        cfg.add_section(section)
        for key in my_cfg[section]:
            cfg.set(section, key, my_cfg[section][key])
    fp=open(config_filepath, "wb")
    cfg.write(fp)
    fp.close()
    print('wrote: {0}'.format(config_filepath))
    return


def getStuff(prod_name, filename):
    cfg = {}
    dbu = DButils.DButils('rbsp') # TODO don't assume RBSP later
    dbu.openDB()
    dbu._createTableObjects()
    # are we trying to write files for all the products?
    if prod_name.lower() == 'all':
        prods = dbu.getAllProducts()
        for prod in prods:
            getStuff(prod.product_name, prod.product_name + filename)
        return
    try:
        prod_name = dbu.getProductID(int(prod_name))
    except ValueError:
        prod_name = dbu.getProductID(prod_name)
    # get instances of all the tables in a product traceback
    sq = dbu.getTraceback('Product', prod_name)
    for section in sq:
        attrs = dir(sq[section])
        cfg[section] = {}
        for val in attrs:
            if val[0] != '_':
                cfg[section][val] = sq[section].__getattribute__(val)

    writeconfig(cfg, filename)


def usage():
    """
    print the usage messag out
    """
    print("Usage: {0} <product name> <filename>".format(sys.argv[0]))
    print("   -> product name (or number) to write to config file")
    return


if __name__ == "__main__":
    if len(sys.argv) != 3:
        usage()
        sys.exit(2)
    getStuff(sys.argv[1], sys.argv[2])

