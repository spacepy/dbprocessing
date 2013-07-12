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

def _keysCheck(conf, section, keys, ignore=None):
    """
    go over a section and see that everything is right
    """
    for k in keys:
        if k not in conf[section]:
            raise(ValueError('Required key: "{0}" was not found in [{1}] section'.format(k, section) ))
    for k in conf[section]:
        if k not in keys and ignore not in k:
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
                                 'code_version',
                                 'process_name', 'code_output_interface',
                                 'code_newest_version', 'code_date_written',
                                 'code_description', 'output_product',
                                 'code_active', 'code_arguments',
                                 'extra_params', 'output_timebase'], ignore='input')
    # loop over the products
    for k in conf:
        if k.startswith('product'):
            _keysCheck(conf, k, ['inspector_output_interface', 'inspector_version',
                                 'inspector_arguments', 'format', 'level',
                                 'product_description', 'relative_path',
                                 'inspector_newest_version', 'inspector_relative_path',
                                 'inspector_date_written', 'inspector_filename',
                                 'inspector_description', 'inspector_active', 'product_name'])

def _fileTest(filename):
    """
    open up the file as txt and do a check that there are no repeated section headers
    """
    def rep_list(inval):
        seen = set()
        seen_twice = set( x for x in inval if x in seen or seen.add(x) )
        return list(seen_twice)

    with open(filename, 'r') as fp:
        data = fp.readlines()
    data = [v.strip() for v in data if v[0] == '[']
    seen_twice = rep_list(data)
    if seen_twice:
        raise(ValueError('Specified section(s): "{0}" is repeated!'.format(seen_twice) ))


def addStuff(cfg, options):
    # setup the db
    dbu = DBUtils.DBUtils(options.mission)

    # is the mission in the DB?  If not add it
    if cfg['mission']['mission_name'] not in dbu.getMissions(): # was it there?
        # add it
        mission_id = dbu.addMission(**cfg['mission'])
        print('Added Mission: {0}'.format(mission_id))
    else:
        mission_id = dbu.getMissionID(cfg['mission']['mission_name'])
        print('Found Mission: {0}'.format(mission_id))

    # is the satellite in the DB?  If not add it
    try:
        satellite_id = dbu.getEntry('Satellite', cfg['satellite']['satellite_name'].replace('{MISSION}', cfg['mission']['mission_name'])).satellite_id
        print('Found Satellite: {0}'.format(satellite_id))
    except DBUtils.DBNoData:
        # add it
        satellite_id = dbu.addSatellite(mission_id=mission_id, **cfg['satellite'])
        print('Added Satellite: {0}'.format(satellite_id))

    # is the instrument in the DB?  If not add it
    try:
        instrument_id = dbu.getEntry('Instrument', cfg['instrument']['instrument_name']).instrument_id
        print('Found Instrument: {0}'.format(instrument_id))
    except DBUtils.DBNoData:
        # add it
        instrument_id = dbu.addInstrument(satellite_id=satellite_id, **cfg['instrument'])
        print('Added Instrument: {0}'.format(instrument_id))

    # loop over all the products, check if they are there and add them if not
    products = [k for k in cfg if k.startswith('product')]
    db_products = [v.product_name for v in dbu.getAllProducts()]
    for p in products:
        # is the product in the DB?  If not add it
        if cfg[p]['product_name'] in db_products:
            p_id = dbu.getEntry('Product', cfg[p]['product_name']).product_id
            cfg[p]['product_id'] = p_id
            print('Found Product: {0}'.format(p_id))
        else:
            tmp = dict((k, cfg[p][k]) for k in cfg[p] if not k.startswith('inspector'))
            p_id = dbu.addProduct(instrument_id=instrument_id, **tmp)
            print('Added Product: {0}'.format(p_id))
            cfg[p]['product_id'] = p_id
            ippl = dbu.addInstrumentproductlink(instrument_id, p_id)
            print('Added Instrumentproductlink: {0}'.format(ippl))
            dbu.updateProductSubs(p_id)

            # if the product was not there we will assume the inspector is not either (requies a product_id)
            tmp = dict((k, cfg[p][k]) for k in cfg[p] if k.startswith('inspector'))

            replace_dict = {'inspector_output_interface':'output_interface_version',
                            'inspector_version':'version',
                            'inspector_arguments': 'arguments',
                            'inspector_description':'description',
                            'inspector_newest_version':'newest_version',
                            'inspector_relative_path':'relative_path',
                            'inspector_date_written':'date_written',
                            'inspector_filename':'filename',
                            'inspector_active': 'active_code'}
            for rd in replace_dict:
                tmp[replace_dict[rd]] = tmp.pop(rd)
            insp_id = dbu.addInspector(product=p_id, **tmp)
            print('Added Inspector: {0}'.format(insp_id))
            dbu.updateInspectorSubs(insp_id)

    # loop over all the processes, check if they are there and add them if not
    processes = [k for k in cfg if k.startswith('process')]
    db_processes = dbu.getAllProcesses()
    for p in processes:
        # is the process in the DB?  If not add it
        if cfg[p]['process_name'] in db_processes:
            p_id = dbu.getEntry('Process', cfg[p]['process_name']).process_id
            print('Found Process: {0}'.format(p_id))
        else:
            tmp = dict((k, cfg[p][k]) for k in cfg[p] if not k.startswith('code') and 'input' not in k)
            # need to repace the output product with the right ID
            # if it is a key then have to get the name from cfg, or it is a name itself
            tmp['output_product'] = cfg[tmp['output_product']]['product_id']
            p_id = dbu.addProcess(**tmp)
            print('Added Process: {0}'.format(p_id))

            # now add the productprocesslink
            tmp = dict((k, cfg[p][k]) for k in cfg[p] if 'input' in k)
            for k in tmp:
                ppl = dbu.addproductprocesslink(cfg[tmp[k]]['product_id'], p_id, 'optional' in k)
                print('Added Productprocesslink: {0}'.format(ppl))

            # if the process was not there we will assume the code is not either (requies a process_id)
            tmp = dict((k, cfg[p][k]) for k in cfg[p] if k.startswith('code'))

            replace_dict = {'code_filename':'filename',
                            'code_arguments':'arguments',
                            'code_relative_path': 'relative_path',
                            'code_version':'version',
                            'code_output_interface':'output_interface_version',
                            'code_newest_version':'newest_version',
                            'code_date_written':'date_written',
                            'code_active':'active_code'}
            for rd in replace_dict:
                tmp[replace_dict[rd]] = tmp.pop(rd)
            code_id = dbu.addCode(process_id=p_id, **tmp)
            print('Added Code: {0}'.format(code_id))



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

    _fileTest(filename)
    
    conf = readconfig(filename)
    configCheck(conf)
    if options.verify: # we are done here if --verify is set
        sys.exit(0)

    addStuff(conf, options)




