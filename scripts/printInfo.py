#!/usr/bin/env python2.6

import itertools
import glob
import os
from optparse import OptionParser
import re
import sys
import traceback
import warnings

from spacepy import pycdf

from dbprocessing import DBUtils
from dbprocessing import Version


def getProductString(p, basepath):
    return "{0:4} {1:5} {2:45} {3}".format(p.product_id,
                                           p.level,
                                           p.product_name,
                                           os.path.join(basepath, p.relative_path))


if __name__ == '__main__':
    usage = "usage: %prog database field \n Field can be: Product, Mission (more to come)"
    parser = OptionParser(usage=usage)

    (options, args) = parser.parse_args()

    if len(args) != 2:
        parser.error("incorrect number of arguments")

    mission = args[0]
    field = args[1].capitalize()

    dbu = DBUtils.DBUtils(mission)

    basepath = dbu.session.query(dbu.Mission).filter_by(mission_name=dbu.getMissions()[0]).all()[0].rootdir
    if not hasattr(dbu, field):
        dbu._closeDB()
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
            

    else:
        dbu._closeDB()        
        raise(NotImplementedError('Attr: "{0}" not yet implemented'.format(field) ))


    dbu._closeDB()
        
