#!/usr/bin/env python


# following the recipe in the document I wrote


# ==============================================================================
# INPUTS
# ==============================================================================
# mission name
# satellite name
# product name
## prod name
## product rel path
## prod filename format
## prod level
# <- add the prod
# <- create the inst_prod link

import collections
import os
import shutil
import sys
import tempfile
import argparse

from sqlalchemy.orm.exc import NoResultFound

from dbprocessing import DButils
import dbprocessing.Utils

expected = ['mission', 'satellite', 'instrument', 'product', 'process']
# All keywords that are permitted in a section (optional or required)
expected_keyword = { }
# Subset of expected_keyword that are permitted not required
optional_keyword = collections.defaultdict(list)
expected_keyword['mission'] = ['incoming_dir', 'mission_name', 'rootdir',
                               'codedir', 'inspectordir', 'errordir']
optional_keyword['mission'] = ['codedir', 'inspectordir', 'errordir']
expected_keyword['satellite'] = ['satellite_name']
expected_keyword['instrument'] = ['instrument_name']
expected_keyword['product'] = ['product_name', 'relative_path',
                               'level', 'format', 'product_description',
                               'inspector_filename', 'inspector_relative_path',
                               'inspector_description', 'inspector_version',
                               'inspector_output_interface', 'inspector_active',
                               'inspector_date_written', 'inspector_newest_version',
                               'inspector_arguments']
expected_keyword['process'] = ['process_name', 'output_product',
                               'output_timebase', 'extra_params',
                               'required_input', 'optional_input',
                               'code_filename', 'code_relative_path',
                               'code_start_date', 'code_stop_date',
                               'code_description', 'code_version',
                               'code_output_interface', 'code_active',
                               'code_date_written', 'code_newest_version',
                               'code_arguments', 'code_cpu', 'code_ram']


def _sectionCheck(conf):
    """
    Check the sections to be sure they are correct and readable
    """
    # check the section names that are there.
    keys = list(conf)
    for key in list(conf):
        # startswith allows you to supply a tuple of strings to test for
        if key.startswith(tuple(expected + ["DEFAULT"])):
            keys.remove(key)

    # do we have any left over keys?
    if keys:
        raise ValueError('Section error, {0} was not understood'.format(keys[0]))

    # check that all the required sections are there
    for req in expected[:-2]:
        if not req in conf:
            raise ValueError('Required section: "{0}" was not found'.format(req))


def _keysCheck(conf, section):
    """
    go over a section and see that everything is right
    """
    if section.startswith('process'):
        section_ex = 'process'
    elif section.startswith('product'):
        section_ex = 'product'
    else:
        section_ex = section
    keys = expected_keyword[section_ex]
    optional = optional_keyword[section_ex]
    for k in keys:
        if k.startswith(('required_input', 'optional_input')) \
           or k in optional:
            continue
        if k not in conf[section]:
            raise ValueError('Required key: "{0}" was not found in [{1}] section'.format(k, section))


def _keysRemoveExtra(conf, section):
    """
    go over a section and remove keys form the dict that are not needed
    -- they were either added by [default] or just extra and not used
    """
    if section.startswith('process'):
        section_ex = 'process'
    elif section.startswith('product'):
        section_ex = 'product'
    else:
        section_ex = section
    keys = list(conf[section])
    for k in keys:
        if k.startswith('required_input') or k.startswith('optional_input'):
            continue
        else:
            if k not in expected_keyword[section_ex]:
                print('Removed keyword {0}[{1}][{2}]={3}'.format('conf', section, k, conf[section][k]))
                del conf[section][k]
    return conf[section]


def _keysPresentCheck(conf):
    """
    loop over each key looking for cross-references and complain if they are not there
    """
    for k in conf:
        if k.startswith('process'):
            for k2 in conf[k]:
                if 'input' in k2:
                    if conf[k][k2][0] not in conf:
                        raise ValueError('Key {0} referenced in {1} was not found'.format(conf[k][k2], k))
                elif 'output_product' in k2:
                    if conf[k][k2] not in conf and conf[k]['output_timebase'] != "RUN":
                        raise ValueError('Key {0} referenced in {1} was not found'.format(conf[k][k2], k))


def configCheck(conf):
    """
    Go through a file that has been read in and make sure that it is going to
    work before we do anything
    """
    _sectionCheck(conf)

    for k in conf:
        _keysCheck(conf, k)
        conf[k] = _keysRemoveExtra(conf, k)

    _keysPresentCheck(conf)


def _fileTest(filename):
    """
    Open up the file as txt and do a check that there are no repeated section headers
    """
    with open(filename, 'r') as fp:
        data = fp.readlines()
    data = [v.strip() for v in data if v[0] == '[']

    seen_twice = set([x for x in data if data.count(x) > 1]) # It's O(n^2), but it's small enough it doesn't matter
    if seen_twice:
        raise ValueError('Specified section(s): "{0}" is repeated!'.format(seen_twice))


def addStuff(cfg, options):
    # setup the db
    dbu = DButils.DButils(options.mission)
    # is the mission in the DB?  If not add it
    if cfg['mission']['mission_name'] not in dbu.getMissions():  # was it there?
        # add it
        mission_id = dbu.addMission(**cfg['mission'])
        print('Added Mission: {0} {1}'.format(mission_id, dbu.getEntry('Mission', mission_id).mission_name))
    else:
        mission_id = dbu.getMissionID(cfg['mission']['mission_name'])
        print('Found Mission: {0} {1}'.format(mission_id, dbu.getEntry('Mission', mission_id).mission_name))

    # is the satellite in the DB?  If not add it
    try:
        satellite_id = dbu.getSatelliteID(cfg['satellite']['satellite_name'])
        print('Found Satellite: {0} {1}'.format(satellite_id, dbu.getEntry('Satellite', satellite_id).satellite_name))
    except (DButils.DBNoData, NoResultFound):
        # add it
        satellite_id = dbu.addSatellite(mission_id=mission_id, **cfg['satellite'])
        print('Added Satellite: {0} {1}'.format(satellite_id, dbu.getEntry('Satellite', satellite_id).satellite_name))

    # is the instrument in the DB?  If not add it
    try:
        inst_id = dbu.getInstrumentID(cfg['instrument']['instrument_name'], satellite_id)
        instrument = dbu.session.query(dbu.Instrument).get(inst_id)
        if instrument.satellite_id != satellite_id:
            raise ValueError()  # this means it is the same name on a different sat, need to add
        instrument_id = instrument.instrument_id
        print(
            'Found Instrument: {0} {1}'.format(instrument_id,
                                               dbu.getEntry('Instrument', instrument_id).instrument_name))
    except (DButils.DBNoData, ValueError, NoResultFound):
        # add it
        instrument_id = dbu.addInstrument(satellite_id=satellite_id, **cfg['instrument'])
        print(
            'Added Instrument: {0} {1}'.format(instrument_id,
                                               dbu.getEntry('Instrument', instrument_id).instrument_name))

    # loop over all the products, check if they are there and add them if not
    products = [k for k in cfg if k.startswith('product')]
    db_products = [v.product_name for v in dbu.getAllProducts()]
    for p in products:
        # is the product in the DB?  If not add it
        if cfg[p]['product_name'] in db_products:
            p_id = dbu.getEntry('Product', cfg[p]['product_name']).product_id
            cfg[p]['product_id'] = p_id
            print('Found Product: {0} {1}'.format(p_id, dbu.getEntry('Product', p_id).product_name))
        else:
            tmp = dict((k, cfg[p][k]) for k in cfg[p] if not k.startswith('inspector'))
            p_id = dbu.addProduct(instrument_id=instrument_id, **tmp)
            print('Added Product: {0} {1}'.format(p_id, dbu.getEntry('Product', p_id).product_name))
            cfg[p]['product_id'] = p_id
            ippl = dbu.addInstrumentproductlink(instrument_id, p_id)
            print('Added Instrumentproductlink: {0}'.format(ippl))
            dbu.updateProductSubs(p_id)

            # if the product was not there we will assume the inspector is not either (requires a product_id)
            tmp = dict((k, cfg[p][k]) for k in cfg[p] if k.startswith('inspector'))

            replace_dict = { 'inspector_output_interface': 'output_interface_version',
                             'inspector_version': 'version',
                             'inspector_arguments': 'arguments',
                             'inspector_description': 'description',
                             'inspector_newest_version': 'newest_version',
                             'inspector_relative_path': 'relative_path',
                             'inspector_date_written': 'date_written',
                             'inspector_filename': 'filename',
                             'inspector_active': 'active_code' }
            for rd in replace_dict:
                tmp[replace_dict[rd]] = tmp.pop(rd)
            insp_id = dbu.addInspector(product=p_id, **tmp)
            print('Added Inspector: {0} {1}'.format(insp_id, dbu.getEntry('Inspector', insp_id).filename))
            dbu.updateInspectorSubs(insp_id)

    # loop over all the processes, check if they are there and add them if not
    processes = [k for k in cfg if k.startswith('process')]
    db_processes = [v.process_name for v in dbu.getAllProcesses()]
    for p in processes:
        # is the process in the DB?  If not add it
        if cfg[p]['process_name'] in db_processes:
            p_id = dbu.getEntry('Process', cfg[p]['process_name']).process_id
            print('Found Process: {0} {1}'.format(p_id, dbu.getEntry('Process', p_id).process_name))
        else:
            tmp = dict((k, cfg[p][k]) for k in cfg[p] if not k.startswith('code') and 'input' not in k)
            # need to replace the output product with the right ID
            # if it is a key then have to get the name from cfg, or it is a name itself
            if tmp['output_product'] != '':
                tmp['output_product'] = cfg[tmp['output_product']]['product_id']
            p_id = dbu.addProcess(**tmp)
            print('Added Process: {0} {1}'.format(p_id, dbu.getEntry('Process', p_id).process_name))

            # now add the productprocesslink
            tmp = dict((k, cfg[p][k]) for k in cfg[p] if 'input' in k)
            for k in tmp:
                ppl = dbu.addproductprocesslink(cfg[tmp[k][0]]['product_id'], p_id, 'optional' in k, tmp[k][1], tmp[k][2])
                print('Added Productprocesslink: {0}'.format(ppl))

            # if the process was not there we will assume the code is not either (requires a process_id)
            tmp = dict((k, cfg[p][k]) for k in cfg[p] if k.startswith('code'))

            replace_dict = { 'code_filename': 'filename',
                             'code_arguments': 'arguments',
                             'code_relative_path': 'relative_path',
                             'code_version': 'version',
                             'code_output_interface': 'output_interface_version',
                             'code_newest_version': 'newest_version',
                             'code_date_written': 'date_written',
                             'code_active': 'active_code',
                             'code_ram': 'ram',
                             'code_cpu': 'cpu' }
            for rd in replace_dict:
                tmp[replace_dict[rd]] = tmp.pop(rd)
            code_id = dbu.addCode(process_id=p_id, **tmp)
            print('Added Code: {0} {1}'.format(code_id, dbu.getEntry('Code', code_id).filename))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mission", type=str,
                        help="mission to connect to", default='')
    parser.add_argument("-v", "--verify", action='store_true',
                        help="Don't do anything other than verify the config file", default=False)
    parser.add_argument('config_file', action='store', type=str,
                        help='Configuration file to read from')

    options = parser.parse_args()

    filename = os.path.expanduser(options.config_file)

    if not os.path.isfile(filename):
        parser.error("file: {0} does not exist or is not readable".format(filename))

    _fileTest(filename)

    conf = dbprocessing.Utils.readconfig(filename)
    configCheck(conf)
    if options.verify:  # we are done here if --verify is set
        sys.exit(0)

    # do substitutions and use a tempfile for the processing
    MISSION = conf['mission']['mission_name']
    SPACECRAFT = conf['satellite']['satellite_name'].replace('{MISSION}', MISSION)
    INSTRUMENT = conf['instrument']['instrument_name'].replace('{MISSION}', MISSION).replace('{SPACECRAFT}', SPACECRAFT)
    print('Adding to mission {0}, spacecraft {1}, instrument {2}'.format(
        MISSION, SPACECRAFT, INSTRUMENT))
    with open(filename, 'r') as fp:
        cfg = fp.readlines()
    for ii, l in enumerate(cfg):
        cfg[ii] = cfg[ii].replace('{MISSION}', MISSION)
        cfg[ii] = cfg[ii].replace('{SPACECRAFT}', SPACECRAFT)
        cfg[ii] = cfg[ii].replace('{INSTRUMENT}', INSTRUMENT)

    try:
        tmpf = tempfile.NamedTemporaryFile(mode='wt', delete=False,
                                           suffix='_conf_file')
        tmpf.file.writelines(cfg)
        tmpf.close()
        # recheck the temp file
        conf = dbprocessing.Utils.readconfig(tmpf.name)
        configCheck(conf)
        if os.path.isfile(options.mission): # sqlite
            # do all our work on a temp version of the DB, if it all works, move tmp on top of existing
            #   if it fails just delete the tmp and do nothing
            orig_db = options.mission
            tmp_db = tempfile.NamedTemporaryFile(delete=False,
                                                 suffix='_temp_db')
            tmp_db.close()
            shutil.copy(orig_db, tmp_db.name)
            options.mission = tmp_db.name
            try:
                addStuff(conf, options)
                shutil.copy(tmp_db.name, orig_db)
            finally:
                os.remove(tmp_db.name)
        else: # postgresql
            addStuff(conf, options)
    finally:
        os.remove(tmpf.name)
