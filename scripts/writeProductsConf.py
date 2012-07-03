# -*- coding: utf-8 -*-
#!/usr/bin/env python2.6


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

import ConfigParser
from dateutil import parser as dup
import sys

from dbprocessing import DBUtils2
from dbprocessing import Version

sections = ['base', 'product', 'inspector',]



def usage():
    """
    print the usage messag out
    """
    print "Usage: {0} <product name> <filename>".format(sys.argv[0])
    print "   -> product name (or number) to write to config file"
    return

def toBool(value):
    if value in ['True', 'true', True, 1, 'Yes', 'yes']:
        return True
    else:
        return False

def toNone(value):
    if value == '':
        return None
    else:
        return value

def writeconfig(my_cfg, config_filepath):
    cfg=ConfigParser.ConfigParser()
    for section in my_cfg:
        cfg.add_section(section)
        for key in my_cfg[section]:
            cfg.set(section, key, my_cfg[section][key])
    fp=open(config_filepath, "wb")
    cfg.write(fp)
    fp.close()
    return


def getStuff(prod_name, filename):
    cfg = {}
    dbu = DBUtils2.DBUtils2('rbsp') # TODO don't assume RBSP later
    print("Connected to DB")
    dbu._openDB()
    dbu._createTableObjects()
    sq = dbu.session.query(dbu.Mission).get(dbu._getMissionID())
    cfg[repr(sq.__module__).split('.')[-1][:-1]] = {}
    attrs = dir(sq)
    for val in attrs:
        if val[0] != '_':
            cfg[repr(sq.__module__).split('.')[-1][:-1]][val] = sq.__getattribute__(val)
    sq = dbu.session.query(dbu.Satellite).get(dbu._getMissionID())
    cfg[repr(sq.__module__).split('.')[-1][:-1]] = {}
    attrs = dir(sq)
    for val in attrs:
        if val[0] != '_':
            cfg[repr(sq.__module__).split('.')[-1][:-1]][val] = sq.__getattribute__(val)






    writeconfig(cfg, filename)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        usage()
        sys.exit(2)
    getStuff(sys.argv[1], sys.argv[2])

