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


def toBool(value):
    if value in ['True', 'true', True, 1, 'Yes', 'yes']:
        return True
    else:
        return False

def toNone(value):
    if value in ['', 'None']:
        return None
    else:
        return value

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
    expected = ['code', 'output_product', 'process', 'input_product0' ]
    for exp in expected:
        if exp not in sections:
            raise(ValueError('Section {0} missing from file'.format(exp)))
    print(sys.argv[-1])
    print '  All sections present'
    # check that the products already exist
    n_inputs = [0,0] #  (reg, opt)
    prods = [conf['output_product']['product_name']]
    for sec in sections:
        if 'input_product' in sec:
            try:
                if conf[sec]['optional']: # it is optional
                    n_inputs[0] += 1
                else:
                    n_inputs[1] += 1
                prods.append(conf[sec]['product_name'])
            except KeyError:
                raise(ValueError('Malformed information in section {0} missing from file (optional missing)'.format(sec)))
    print '  There are {0} required and {1} optional inputs'.format(*n_inputs)
    # do all the products exist in the db already?  (prods)
    prod_id_dict = {}
    for prod in prods:
        try:
            prod_id_dict[prod] = dbu._getProductID(prod)
        except DBUtils2.DBNoData:
            raise(DBUtils2.DBNoData('Product {0} was not already in the DB, check spelling or add'.format(prod)))
    print '  All products are in the db.  Continuing with add'
    return prod_id_dict


def addStuff(filename):
    cfg = readconfig(filename)
    # setup the db
    dbu = DBUtils2.DBUtils2('rbsp') # TODO no rbsp hardcode later
    dbu._openDB()
    dbu._createTableObjects()

    prod_id_dict = configCheck(cfg, dbu)

    # add the process
    proc_id = dbu.addProcess(cfg['process']['process_name'],
                             prod_id_dict[cfg['output_product']['product_name']],
                             cfg['process']['output_timebase'],
                             toNone(cfg['process']['extra_params']),
                             None)
    # add the productprocesslink (there are several)
    for sec in cfg:
        if 'input_product' in sec:
            dbu.addproductprocesslink(prod_id_dict[cfg[sec]['product_name']], proc_id, cfg[sec]['optional'] )

    # add code
    code_start_date = dup.parse(cfg['code']['code_start_date'])
    code_stop_date = dup.parse(cfg['code']['code_stop_date'])
    date_written = dup.parse(cfg['code']['date_written'])
    version = cfg['code']['version'].split('.')
    version = Version.Version(*version)
    code_id = dbu.addCode(cfg['code']['filename'],
                          cfg['code']['relative_path'],
                          code_start_date,
                          code_stop_date,
                          toNone(cfg['code']['code_description']),
                          proc_id,
                          version,
                          toBool(cfg['code']['active_code']),
                          date_written,
                          cfg['code']['output_interface_version'],
                          toBool(cfg['code']['newest_version']),
                          toNone(cfg['code']['arguments']) )

    dbu.updateProcessSubs(proc_id)


def usage():
    """
    print the usage messag out
    """
    print "Usage: {0} <filename>".format(sys.argv[0])
    print "   -> config file to read"
    return


if __name__ == "__main__":
    if len(sys.argv) != 2:
        usage()
        sys.exit(2)
    addStuff(sys.argv[-1])

