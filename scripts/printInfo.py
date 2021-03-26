#!/usr/bin/env python

import argparse
import datetime
import itertools
import glob
import os
import re
import sys
import traceback
import warnings

from dateutil import parser as dup
from dateutil.relativedelta import relativedelta
from spacepy import pycdf

from dbprocessing import DButils
from dbprocessing import Version


def getProductString(p, basepath):
    return "{0:4} {1:5} {2:45} {3}".format(p.product_id,
                                           p.level,
                                           p.product_name,
                                           os.path.join(basepath, p.relative_path))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--product",
                        help='Product id (or name) to use, only used for "File"', default=None)
    parser.add_argument("-s", "--startDate", dest="startDate", type=str,
                        help='Date to start printing, only used for "File"  (e.g. 2012-10-02)', default=None)
    parser.add_argument("-e", "--endDate", dest="endDate", type=str,
                      help='Date to end printing, only used for "File" (e.g. 2012-10-25)', default=None)
    parser.add_argument(
        'database', action='store', type=str,
        help='Name of database (or sqlite file), i.e. mission, to open.')
    parser.add_argument(
        'field', action='store', type=str,
        help='Field to print (Product, Mission; more to come)')


    options = parser.parse_args()

    mission = options.database
    field = options.field.capitalize()

    dbu = DButils.DButils(mission)

    basepath = dbu.session.query(dbu.Mission).filter_by(mission_name=dbu.getMissions()[0]).all()[0].rootdir
    if not hasattr(dbu, field):
        dbu.closeDB()
        parser.error('Field: "{0}" not found in database: "{1}"'.format(field, mission))

    if field == 'Product':
        print("{0:4} {1:5} {2:40} {3}".format("ID", "LEVEL", "NAME", "PATH"))
        for p in dbu.getAllProducts():
            print(getProductString(p, basepath))
                
    elif field == 'Mission':
        print("{0:4} {1:40} {2:40} {3:40}".format("ID", "NAME", "ROOT", "INCOMING"))
        for m in dbu.session.query(dbu.Mission).all():
            print("{0:4} {1:40} {2:40} {3:40}".format(m.mission_id,
                                                      m.mission_name,
                                                      m.rootdir,
                                                      m.incoming_dir))
    elif field == 'Process':
        print("{0:4} {1:40} {2:10} {3:45}".format("ID", "NAME", "TIMEBASE", "OUTPUT"))
        procs = dbu.getAllProcesses()
        procs = sorted(procs, key=lambda x: x.process_id)
        for p in procs:
            prod_id = p.output_product
            if p.output_timebase == 'RUN':
                prod_name = 'N/A'
                prod = -1 #Not actually used below anyhow....
            else:
                prod_name = dbu.getEntry('Product', prod_id).product_name
                prod = dbu.getEntry('Product', prod_id)
            print("{0:4} {1:40} {2:10} {3:45}".format(p.process_id,
                                                      p.process_name,
                                                      p.output_timebase,
                                                      "({0:3}) {1}".format(prod_id, prod_name)))
#            print("{0} {1}".format(getProductString(prod, basepath),
#                                   "({0}){1}".format(pid, prod_name) ) )
            products = dbu.getInputProductID(p.process_id)
            for pp, opt in products:
                opt_print = "optional" if opt else ""
                print("\t\t{0:10} ({1:3}) {2:45}".format(opt_print,
                                                         pp, dbu.getEntry('Product', pp).product_name))
            codes = dbu.getAllCodesFromProcess(p.process_id)
            for c, sd, ed in codes:
                print("\t\t{0:10}c({1:3}) {2:45} {3}->{4}".format("", c,
                                                          dbu.getEntry('Code', c).filename,
                                                          sd.isoformat(),
                                                          ed.isoformat()))
    elif field == 'Code':
        print("{0:4} {1:40} {2:10} ({3})   {4:40}  {5} {6:40}   {7}-{8}  {9}  {10} {11:10} -> {12:10}".format('id',
                                                                                                 'filename',
                                                                                                 'version',
                                                                                                 'outV',
                                                                                                 'path',
                                                                                                 'p_id',
                                                                                                 'process',
                                                                                                 'A','N',
                                                                                                 'ram', 'cpu',
                                                                                                 'start', 'stop'))
        codes = dbu.getAllCodes()
        codes = [c['code'] for c in codes]
        codes = sorted(codes, key=lambda x: x.code_id)
        for c in codes:
            ans = {}
            ans['code_id'] = c.code_id
            ans['filename'] = c.filename
            ans['version'] = Version.Version(c.interface_version, c.quality_version, c.revision_version)
            ans['path'] = os.path.join(basepath, c.relative_path)
            ans['process_id'] = c.process_id
            ans['process_name'] = dbu.getEntry('Process', ans['process_id']).process_name
            ans['out_version'] = c.output_interface_version
            ans['active_code'] = c.active_code
            ans['newest_version'] = c.newest_version
            ans['args'] = c.arguments
            ans['ram'] = c.ram
            ans['cpu'] = c.cpu
            ans['date'] = c.date_written
            ans['code_start_date'] = c.code_start_date
            ans['code_stop_date'] = c.code_stop_date
            print('{code_id:4} {filename:40} {version:10} ({out_version}.Y.Z)    {path:40} ({process_id:3}) {process_name:40} {active_code:1}-{newest_version:1} {ram:4}-{cpu:2}  {code_start_date} -> {code_stop_date}'.format(**ans))

    elif field == 'File':
        if options.product is None:
            dbu.closeDB()
            parser.error("To print File info a product_id is required via -p,--product")

        options.product = dbu.getProductID(options.product) # make sure the p_id is here or change name to p_id
        
        if options.startDate is not None:
            startDate = dup.parse(options.startDate)
        else:
            startDate = None
        if options.endDate is not None:
            try:
                endDate = datetime.datetime.strptime(options.endDate, "%Y%m") # yyyymm
                endDate += relativedelta(months=1)
                endDate -= datetime.timedelta(days=1)
            except ValueError:
                endDate = dup.parse(options.endDate)
        else:
            endDate = None
        if startDate is None and endDate is not None:
            startDate = datetime.datetime(1957, 10, 4, 19, 28, 34)
        if startDate is not None and endDate is None:
            endDate = datetime.datetime(2100, 12, 31, 23, 59, 59)

        if startDate is None:
            files = dbu.getFilesByProduct(options.product)
        else:
            files = dbu.getFilesByProductDate(options.product, [startDate, endDate])

        if files:
            # sort the files
            files = sorted(files, key=lambda x: x.filename)
            # get the file path:
            filepath = os.path.dirname(dbu.getFileFullPath(files[0].file_id))
            print("{0:6} {1:4} {2:80} {3:40} {4:6}".format('f_id', 'p_id', 'full path', 'filename', 'newest'))
            for f in files:
                print("{0:6} {1:4} {2:80} {3:40} {4:6}".format(
                    f.file_id, options.product,
                    os.path.join(filepath, f.filename), f.filename,
                    getattr(f, 'newest_version', 'N/A')))
            
        else:
            print("No files found for product {0} in date range".format(options.product))
              
                                    
        
    else:
        dbu.closeDB()
        raise NotImplementedError('Attr: "{0}" not yet implemented'.format(field) )


    dbu.closeDB()
        
