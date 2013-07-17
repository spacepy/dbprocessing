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

   

if __name__ == '__main__':
    usage = "usage: %prog database field \n Field can be: Product, Mission (more to come)"
    parser = OptionParser(usage=usage)

    (options, args) = parser.parse_args()

    if len(args) != 2:
        parser.error("incorrect number of arguments")

    mission = args[0]
    field = args[1].capitalize()

    dbu = DBUtils.DBUtils(mission)

    if not hasattr(dbu, field):
        dbu._closeDB()
        parser.error('Field: "{0}" not found in database: "{1}"'.format(field, mission))

    basepath = dbu.session.query(dbu.Mission).filter_by(mission_name=dbu.getMissions()[0]).all()[0].rootdir

    if field == 'Product':
        print("{0:4} {1:5} {2:40} {3}".format("ID", "LEVEL", "NAME", "PATH"))
        for p in dbu.getAllProducts():
            print("{0:4} {1:5} {2:40} {3}".format(p.product_id,
                                                         p.level,
                                                         p.product_name,
                                                         os.path.join(basepath, p.relative_path)))
    elif file == 'Mission':
        print("{0:4} {1:40} {2:40} {3:40}".format("ID", "NAME", "ROOT", "INCOMING"))
        for m in dbu.session.query(dbu.Mission).all():
            print("{0:4} {1:40} {2:40} {3:40}".format(m.mission_id,
                                                      m.mission_name,
                                                      m.rootdir,
                                                      m.incoming_dir))
    else:
        dbu._closeDB()        
        raise(NotImplementedError('Attr: "{0}" not yet implemented'.format(field) ))


    dbu._closeDB()
        
