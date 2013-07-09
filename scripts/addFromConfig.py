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

import itertools
from optparse import OptionParser

import ConfigParser
from dateutil import parser as dup
import os
import sys

from dbprocessing import DBUtils
from dbprocessing import Version
from dbprocessing.Utils import toBool, toNone

expected = ['mission', 'satellite', 'instrument', 'product', 'process',]

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

def _sectionCheck(conf):
    """
    check the sections to be sure hey are correct and readable
    """
    # check the section names that are there.
    keys = conf.keys()
    for key, exp in itertools.product(conf.keys(), expected):
        if key == exp or key.startswith(exp):
            keys.remove(key)
            continue
    # do we have any left over keys?
    if keys:
        for k in keys:
            print('Section name: "{0}" not understood'.format(k))
        raise(ValueError('Section error, {0} was not understood'.format(keys)))
    # check that all the required sections are there
    for req in expected[:-2]:
        if not req in conf:
            raise(ValueError('Required section: "{0}" was not found'.format(req)))

def _keysCheck(conf, section, keys):
    """
    go over a section and see that everything is right
    """
    for k in keys:
        if k not in conf[section]:
            raise(ValueError('Required key: "{0}" was not found in [{1}] section'.format(k, section) ))
    for k in conf[section]:
        if k not in keys:
            raise(ValueError('Specified key: "{0}" is not allowed in [{1}] section'.format(k, section) ))

def configCheck(conf):
    """
    go through a file that has been read in and make sure that it is going to
    work before we do anything
    """
    _sectionCheck(conf)
    _keysCheck(conf, 'mission', ['mission_name', 'rootdir', 'incoming_dir'])
    _keysCheck(conf, 'satellite', ['satellite_name'])
    _keysCheck(conf, 'instrument', ['instrument_name'])
    # loop over the processes
    for k in conf:
        if k.startswith('process'):
            _keysCheck(conf, k, ['code_start_date', 'code_stop_date',
                                 'code_filename', 'code_relative_path',
                                 'required_input1', 'code_version',
                                 'process_name', 'code_output_interface',
                                 'code_newest_version', 'code_date_written',
                                 'code_description', 'output_product',
                                 'code_active', 'code_arguments',
                                 'extra_params', 'output_timebase'])
    # loop over the products
    for k in conf:
        if k.startswith('product'):
            _keysCheck(conf, k, ['inspector_output_interface', 'inspector_version',
                                 'inspector_arguments', 'format', 'level',
                                 'product_description', 'relative_path',
                                 'inspector_newest_version', 'inspector_relative_path',
                                 'inspector_date_written', 'inspector_filename',
                                 'inspector_description', 'inspector_active', 'product_name'])




def addStuff(cfg, options):
    # setup the db
    dbu = DBUtils.DBUtils(options.mission)

    # is the mission in the DB?  If not add it
    if cfg['mission']['mission_name'] not in dbu.getMissions(): # was it there?
        # add it
        mission_id = dbu.addMission(**cfg['mission'])
    else:
        mission_id = dbu.getMissionID(cfg['mission']['mission_name'])

    # is the satellite in the DB?  If not add it
    try:
        satellite_id = dbu.getEntry('Satellite', cfg['satellite']['satellite_name']).satellite_id
    except DBUtils.DBNoData:
        # add it
        satellite_id = dbu.addSatellite(mission_id=mission_id, **cfg['satellite'])


    1/0



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


if __name__ == "__main__":
    usage = "usage: %prog [options] filename"
    parser = OptionParser(usage=usage)
    parser.add_option("-m", "--mission", dest="mission", type="string",
                      help="mission to connect to", default='~ectsoc/RBSP_processing.sqlite')
    parser.add_option("-v", "--verify", dest="verify", action='store_true',
                      help="Don't do anything other than verify the config file", default=False)

    (options, args) = parser.parse_args()
    if len(args) != 1:
        parser.error("incorrect number of arguments")

    filename = os.path.expanduser(args[0])

    if not os.path.isfile(filename):
        parser.error("file: {0} does not exist or is not readable".format(filename))

    conf = readconfig(filename)
    configCheck(conf)
    if options.verify: # we are done here if --verify is set
        sys.exit(0)

    addStuff(conf, options)




