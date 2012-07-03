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
    print "Usage: {0} <filename>".format(sys.argv[0])
    print "   -> config file to read"
    return

def toBool(value):
    if value in ['True', 'true', True, 1, 'Yes', 'yes']:
        return True
    else:
        return False

def readconfig(config_filepath, section):
    # Create a ConfigParser object, to read the config file
    cfg=ConfigParser.SafeConfigParser()
    cfg.read(config_filepath)
    # Read each parameter in turn
    global sections
    try:
        tmp = dict(cfg.items(section))
    except ConfigParser.NoSectionError:
        tmp = {}
    return tmp

def addStuff(filename):
    vals = readconfig(filename, 'mission')
    if not vals:
        raise(ConfigParser.NoSectionError("No section [mission]"))
    dbu = DBUtils2.DBUtils2(vals['name'])
    print("Connected to DB")
    dbu._openDB()
    dbu._createTableObjects()
#    sat_id = int(dbu._getSatelliteID(vals['satellite']))
    vals = readconfig(filename, 'instrument')
    if not vals:
        raise(ConfigParser.NoSectionError("No section [instrument]"))
    inst_id = int(dbu._getInstruemntID(vals['name']))
    vals = readconfig(filename, 'product')
    if not vals:
        raise(ConfigParser.NoSectionError("No section [product]"))
    # add the product
    prod_id = dbu.addProduct(vals['name'], inst_id, vals['path'], None, vals['format'], vals['level'])
    print("added product {0}:{1}".format(prod_id, vals['name']))
    # add the link
    dbu.addInstrumentproductlink(inst_id, prod_id)
    print("added Instrumentproductlink {0}:{1}".format(inst_id, prod_id))
    # add inspector
    vals = readconfig(filename, 'inspector')
    if not vals:
        raise(ConfigParser.NoSectionError("No section [inspector]"))
    insp_id = dbu.addInspector(vals['filename'], vals['path'], vals['description'],
                     Version.Version(*vals['version'].split('.')), toBool(vals['active']),
                     dup.parse(vals['date_written']), int(vals['output_interface_version']),
                     toBool(vals['newest']), prod_id, vals['arguments'])
    print("added Inspector {0}:{1}".format(insp_id, vals['filename']))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        usage()
        sys.exit(2)
    addStuff(sys.argv[-1])

