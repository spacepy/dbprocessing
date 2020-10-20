#!/usr/bin/env python


# following the recipe in the document I wrote



import itertools
from optparse import OptionParser
import os
import shutil
import sys

from dbprocessing import DButils
from dbprocessing import Version
from sqlalchemy.orm.exc import NoResultFound



if __name__ == "__main__":
    usage = "usage: %prog [options] -m mission_db code_id newversion newname"
    parser = OptionParser(usage=usage)
    parser.add_option("-m", "--mission", dest="mission", type="string",
                      help="mission to connect to", default='~ectsoc/RBSP_processing.sqlite')

    (options, args) = parser.parse_args()
    if len(args) != 3:
        parser.error("incorrect number of arguments")

    try:
        version = Version.Version.fromString(args[1])
    except ValueError:
        parser.error("Invalid version: {0}, must be X.Y.Z".format(args[1]))


    try:
        code_id = int(args[0])
    except ValueError:
        parser.error("Invalid code_id: {0}, must be an int".format(args[0]))

    newname = args[2]
    
    dbu = DButils.DButils(os.path.expanduser(options.mission))

    try:
        code = dbu.getEntry('Code', code_id)
    except DButils.DBNoData:
        parser.error("Invalid code_id: {0}, must be in the database".format(code_id))

    old_version = Version.Version(code.interface_version, code.quality_version, code.revision_version)
    if version <= old_version:
        parser.error("New version, {0}, must be larger than old version {1}".format(version,old_version ))
    

    # make a new code and copy across the needed keys
    attrs = [ u'active_code',
              u'arguments',
              u'code_description',
              # u'code_id',
              u'code_start_date',
              u'code_stop_date',
              u'cpu',
              u'date_written',
              # u'filename',
              #u'interface_version',
              #u'newest_version',
              u'output_interface_version',
              u'process_id',
              #u'quality_version',
              u'ram',
              u'relative_path',
              #u'revision_version',
              u'shasum']

    code2 = dbu.Code()
    for attr in attrs:
        try:
            setattr(code2, attr, getattr(code, attr))
        except AttributeError:
            print("Attribute {0} not found".format(attr))
    code2.interface_version = version.interface
    code2.quality_version = version.quality
    code2.revision_version = version.revision
    code2.filename = newname
    code2.newest_version = True
    dbu.session.add(code2)
    code.newest_version = False
    dbu.session.add(code)
    dbu.session.commit()
