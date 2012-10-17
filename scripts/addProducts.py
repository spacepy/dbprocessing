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

from dbprocessing import DBUtils
from dbprocessing import Version
from dbprocessing.Utils import toBool, toNone

sections = ['base', 'product', 'inspector',]


def readconfig(config_filepath):
    # Create a ConfigParser object, to read the config file
    cfg=ConfigParser.SafeConfigParser()
    cfg.read(config_filepath)
    sections = cfg.sections()
    # Read each parameter in turn
    ans = {}
    for section in sections:
        ans[section] = dict(cfg.items(section))
    return ans

def configCheck(conf, dbu):
    """
    go through a file that has been read in and make sure that it is going to
    work before we do anything
    """
    # check the sections
    sections = conf.keys()
    expected = ['satellite', 'mission', 'product', 'instrument', 'inspector' ]
    for exp in expected:
        if exp not in sections:
            raise(ValueError('Section {0} missing from file'.format(exp)))


def addStuff(filename):
    cfg = readconfig(filename)
    # setup the db
    dbu = DBUtils.DBUtils('rbsp') # TODO no rbsp hardcode later
    dbu._openDB()
    dbu._createTableObjects()

    configCheck(cfg, dbu)

    print '{0}'.format(filename)

    # add the product
    satellite_id = dbu.getSatelliteID(cfg['satellite']['satellite_name'])
    instrument_id = dbu.getInstrumentID(cfg['instrument']['instrument_name'], satellite_id)

    prod_id = dbu.addProduct(cfg['product']['product_name'],
                             instrument_id,
                            cfg['product']['relative_path'],
                            None,
                            cfg['product']['format'],
                            float(cfg['product']['level']),
                            )
    print '   added product {0}'.format(prod_id)

    # add instrumentproductlink
    dbu.addInstrumentproductlink(instrument_id, prod_id)

    # add inspector
    version = Version.Version(*cfg['inspector']['version'].split('.'))
    date_written = dup.parse(cfg['inspector']['date_written'])

    insp_id = dbu.addInspector(cfg['inspector']['filename'],
                               cfg['inspector']['relative_path'],
                                cfg['inspector']['description'],
                                version,
                                toBool(cfg['inspector']['active_code']),
                                date_written,
                                int(cfg['inspector']['output_interface_version']),
                                toBool(cfg['inspector']['newest_version']),
                                prod_id,
                                toNone(cfg['inspector']['arguments']),
                                )
    print '   added inspector {0}'.format(insp_id)
    dbu.updateProductSubs(prod_id)



def usage():
    """
    print the usage message out
    """
    print "Usage: {0} <filename>".format(sys.argv[0])
    print "   -> config file to read"
    return


if __name__ == "__main__":
    if len(sys.argv) != 2:
        usage()
        sys.exit(2)
    addStuff(sys.argv[-1])



