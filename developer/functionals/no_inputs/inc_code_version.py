#!/usr/bin/env python

"""Increment the version of the only code in the database, for func test"""

import os
import os.path
import shutil

import dbprocessing.DButils


dbu = dbprocessing.DButils.DButils('no_inputs.sqlite')
codes = dbu.getAllCodes()
# Should only be the one active code!
assert len(codes) == 1
oldcode = codes[0]['code']
del codes
newcode = dbu.Code()
for k in dir(newcode):
    if not k.startswith('_'):
        setattr(newcode, k, getattr(oldcode, k))
newcode.code_id = None
newcode.quality_version = oldcode.quality_version + 1
dbu.session.add(newcode)
dbu.commitDB()
# Old code no longer active
dbu.updateCodeNewestVersion(oldcode.code_id)

# And move the actual files around
oldver = '{}.{}.{}'.format(oldcode.interface_version, oldcode.quality_version,
                           oldcode.revision_version)
newver = '{}.{}.{}'.format(oldcode.interface_version, newcode.quality_version,
                           oldcode.revision_version)
oldpath  = os.path.join('root', 'dbp_codes', 'scripts',
                        'inputless_v{}'.format(oldver),
                        'inputless_v{}.py'.format(oldver))
newpath = os.path.join('root', 'dbp_codes', 'scripts',
                        'inputless_v{}'.format(newver))
os.mkdir(newpath)
shutil.copy2(oldpath, os.path.join(newpath, 'inputless_v{}.py'.format(newver)))
