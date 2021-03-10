#!/usr/bin/env python


try:
    import configparser
except ImportError: # Py2
    import ConfigParser as configparser
import os
import sys
import argparse

from dbprocessing import DButils
from dbprocessing import Utils
from dbprocessing import Version

header_comments = """
###############################################################################
# Honored database substitutions used as {Y}{MILLI}{PRODUCT}
#	Y: 4 digit year
#	m: 2 digit month
#	b: 3 character month (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)
#	d: 2 digit day
#	y: 2 digit year
#	j: 3 digit day of year
#	H: 2 digit hour (24-hour time)
#	M: 2 digit minute
#	S: 2 digit second
#	MILLI: 3 digit millisecond
#	MICRO: 3 digit microsecond
#	QACODE: the QA code (ok|ignore|problem)
#	VERSION: version string, interface.quality.revision
#	DATE: the UTC date from a file, same as Ymd
#	MISSION: the mission name from the db
#	SPACECRAFT: the spacecraft name from the db
#	PRODUCT: the product name from the db
###############################################################################

###############################################################################
# MANUAL
#
# Loops over the configuration file and if the DB does not have the mission,
# satellite, instrument entries present they are added, skipped otherwise.
# the Product and process entries must be unique and not present.
#
# THERE IS CURRENTLY NO UPDATE IN THE DB BASED ON THIS CONFIG SCRIPT
#
###############################################################################

###############################################################################
# Required elements
#
# [mission]  <- once and only once with
#   rootdir  (string)
#   mission_name  (string)
#   incoming_dir  (string)
# [satellite] <- once and only once with
#   satellite_name  (string)
# [instrument] <- once and only once with
#   instrument_name  (string)
##### products and inspector are defined together since they are a one-to-one
# [product] <- multiple entries starting with "product" then a unique identifier
#   product_name  (string)
#   relative_path  (string)
#   level  (float)
#   format  (string)
#   product_description (string)
#   inspector_filename (string)
#   inspector_relative_path (string)
#   inspector_description (string)
#   inspector_version (version e.g. 1.0.0)
#   inspector_output_interface (integer)
#   inspector_active (Boolean e.g. True or 1 or False or 0)
#   inspector_date_written (date e.g. 2013-07-12)
#   inspector_newest_version  (Boolean e.g. True or 1 or False or 0)
#   inspector_arguments (string)
#### processes and codes operate on the names of the products, they can be in
#### this config file or already in the db codes are one-to-one with processes
# [process] <- multiple entries starting with "process" then a unique identifier
#   process_name (string)
#   output_product (string)  - identifier from section heading
#   output_timebase  (string, FILE/DAILY/WEEKLY/MONTHLY/YEARLY)
#   extra_params (string)
## A collection of input names entered as such
## the required portion is "optional_input" or "required_input" then some
## unique identifier on the end
#   optional_input1  (string) name of product - identifier from section heading
#   optional_input2  (string) name of product - identifier from section heading
#   optional_input3  (string) name of product - identifier from section heading
#   required_input1  (string) name of product - identifier from section heading
#   required_input2  (string) name of product - identifier from section heading
#   required_input3  (string) name of product - identifier from section heading
## code is entered as part of process
#   code_filename (string)
#   code_relative_path (string)
#   code_start_date (date, 2000-01-01)
#   code_stop_date  (date, 2050-12-31)
#   code_description (string)
#   code_version  (version e.g. 1.0.0)
#   code_output_interface  (integer)
#   code_active (Boolean e.g. True or 1 or False or 0)
#   code_date_written   (date, 2050-12-31)
#   code_newest_version (Boolean e.g. True or 1 or False or 0)
#   code_arguments (string)
###############################################################################

"""

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.set_defaults(mission=None)
    parser.set_defaults(force=False)
    parser.set_defaults(satellite=None)
    parser.set_defaults(nocomments=False)

    parser.add_argument("-m", "--mission", dest="mission", type=str,
                        default=None,
                        help="mission to connect to")
    parser.add_argument("-f", "--force", dest="force", action="store_true",
                        default=False,
                        help="Force the processing, overwrite the outfile")
    parser.add_argument("-s", "--satellite", dest="satellite", type=str,
                        default=None,
                        help="satellite to write to conf file (one per file)")
    parser.add_argument("-i", "--instrument", dest="instrument", type=str,
                        help="instrument to write to conf file (one per file)")
    parser.add_argument("-c", "--nocomments", dest="nocomments", action='store_true',
                        default=False,
                        help="Do not add comments to the top of the config file")
    parser.add_argument('config_file', action='store', type=str,
                        help='Configuration file to write')

    options = parser.parse_args()

    filename = os.path.expandvars(os.path.expanduser(options.config_file))

    if os.path.isfile(filename) and not options.force:
        parser.error("file: {0} exists and will not be overwritten (use --force)".format(filename))

    dbu = DButils.DButils(options.mission)

    # ==============================================================================
    #     This will make a configparser of the needed parts then write to file
    # ==============================================================================

    # "Safe" deprecated in 3.2, but still present, so version is only way
    #  to avoid stepping on the deprecation. "Safe" preferred before 3.2
    out = (configparser.SafeConfigParser if sys.version_info[:2] < (3, 2)
           else configparser.ConfigParser)()

    # get mission
    out.add_section('mission')
    missions = dbu.getMissions()
    if not missions:
        raise ValueError("No mission in DB, this must be an empty db")
    if len(missions) > 1:
        raise NotImplementedError("Can't yet handle multi mission db")
    mission = missions[0]
    mission = dbu.getEntry('Mission', mission)
    for d in dir(mission):
        if d[0] == '_': continue
        out.set('mission', d, str(getattr(mission, d)))

    # get satellite
    out.add_section('satellite')
    sats = dbu.getAllSatellites()
    for v in sats:
        if v['mission'].mission_id != mission.mission_id:
            raise NotImplementedError("Can't yet handle multi mission db")
    nsats = len(Utils.unique([v['satellite'].satellite_id for v in sats]))
    if nsats > 1 and options.satellite is None:
        raise ValueError("More than one sat in db and no --satellite set\n    {0}".format(
            [v['satellite'].satellite_name for v in sats]))
    elif nsats == 1 and options.satellite is None:
        options.satellite = sats['satellite'].satellite_name
    for s in sats:
        if s['satellite'].satellite_name != options.satellite:
            continue
        for d in dir(s['satellite']):
            if d[0] == '_': continue
            out.set('satellite', d, str(getattr(s['satellite'], d)))

    # get instrument
    out.add_section('instrument')
    insts = dbu.getAllInstruments()
    for v in insts:
        if v['mission'].mission_id != mission.mission_id:
            raise NotImplementedError("Can't yet handle multi mission db")
        #        if v['satellite'].satellite_name != options.satellite:
        #            raise(ValueError("More than one sat in db and no --satellite set\n    {0}".format([v['satellite'].satellite_name for v in sats])))
    ninsts = len(Utils.unique(
        [v['instrument'].instrument_id for v in insts if v['satellite'].satellite_name == options.satellite]))
    if ninsts > 1 and options.instrument is None:
        raise ValueError("More than one instrument in db and no --instrument set\n    {0}".format(
            [v['instrument'].instrument_name for v in insts]))
    elif ninsts == 1 and options.instrument is None:
        # find the inst related to the right sat
        for i in insts:
            if i['satellite'].satellite_name == options.satellite:
                options.instrument = i['instrument'].instrument_name
    for i in insts:
        if i['satellite'].satellite_name != options.satellite or i['instrument'].instrument_name != options.instrument:
            continue
        for d in dir(i['instrument']):
            if d[0] == '_': continue
            out.set('instrument', d, str(getattr(i['instrument'], d)))

    # get products
    ## TODO maybe add a sort by level in here
    prods_tmp = dbu.getAllProducts()
    prods = []
    for p in prods_tmp:
        tmp = dbu.getTraceback('Product', p.product_id)
        # if this is not our sallite continue
        if tmp['satellite'].satellite_name != options.satellite:
            continue
        # if this is not our instrument continue
        if tmp['instrument'].instrument_name != options.instrument:
            continue
        prods.append(tmp)
    for p in prods:
        pname = 'product_' + p['product'].product_name
        out.add_section(pname)
        for d in dir(p['product']):
            if d[0] == '_': continue
            out.set(pname, d, str(getattr(p['product'], d)))
        for d2 in dir(p['inspector']):
            if d2[0] == '_': continue
            if d2 == 'output_interface_version':  # annoying special case
                out.set(pname, 'inspector_output_interface', str(getattr(p['inspector'], d2)))
            elif d2 == 'active_code':  # annoying special case
                out.set(pname, 'inspector_active', str(getattr(p['inspector'], d2)))
            elif not d2.startswith('inspector'):
                out.set(pname, 'inspector_' + d2, str(getattr(p['inspector'], d2)))
            else:
                out.set(pname, d2, str(getattr(p['inspector'], d2)))

    # get process
    procs_tmp = dbu.getAllProcesses()
    procs = []
    for p in procs_tmp:
        tmp = dbu.getTraceback('Process', p.process_id)
        # if this is not our satellite continue
        if tmp['satellite'].satellite_name != options.satellite:
            continue
        # if this is not our instrument continue
        if tmp['instrument'].instrument_name != options.instrument:
            continue
        procs.append(tmp)
    for p in procs:
        pname = 'process_' + p['process'].process_name
        out.add_section(pname)
        for d in dir(p['process']):
            if d[0] == '_': continue
            if d == 'output_product':
                product_id = str(getattr(p['process'], d))
                product_name  = dbu.getEntry('Product', product_id).product_name
                out.set(pname, d, 'product_' + product_name)
                continue
            out.set(pname, d, str(getattr(p['process'], d)))
        # loop over the input_product key and add them to the conf
        ip_req_counter = 1
        ip_opt_counter = 1
        for ip in p['input_product']:
            if ip[1] is False:
                out.set(pname, 'required_input{0}'.format(ip_req_counter), 'product_' + str(ip[0].product_name))
                ip_req_counter += 1
            elif ip[1] is True:
                out.set(pname, 'optional_input{0}'.format(ip_opt_counter), 'product_' + str(ip[0].product_name))
                ip_opt_counter += 1
        for d2 in dir(p['code']):
            if d2[0] == '_': continue
            if d2 == 'output_interface_version':  # annoying special case
                out.set(pname, 'code_output_interface', str(getattr(p['code'], d2)))
            elif d2 == 'active_code':  # annoying special case
                out.set(pname, 'code_active', str(getattr(p['code'], d2)))
            elif not d2.startswith('code'):
                val = str(getattr(p['code'], d2))
                if not val: val = str(None)
                out.set(pname, 'code_' + d2, val)
            else:
                out.set(pname, d2, str(getattr(p['code'], d2)))

    # loop over everything and delete individual versions making one
    for sec in out.sections():
        interface_version = None
        quality_version = None
        revision_version = None
        name = None
        for opt in out.options(sec):
            if 'interface_version' in opt:
                interface_version = out.get(sec, opt)
                out.remove_option(sec, opt)
                name = opt.split('_interface_version')[0]
            elif 'quality_version' in opt:
                quality_version = out.get(sec, opt)
                out.remove_option(sec, opt)
            elif 'revision_version' in opt:
                revision_version = out.get(sec, opt)
                out.remove_option(sec, opt)
        if interface_version is not None:
            out.set(sec, name + '_version', str(Version.Version(interface_version, quality_version, revision_version)))

    # write out the conf file
    with open(filename, 'wb') as configfile:
        if not options.nocomments:
            configfile.writelines(header_comments)
        out.write(configfile)
