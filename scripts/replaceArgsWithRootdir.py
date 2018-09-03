#!/usr/bin/env python

"""Update mission db, replace root dir strings with ROOTDIR

Grabs rootdir from mission table. Finds any occurrence of that string in
arguments column of code table, replaces with {ROOTDIR} so it will carry
across moves/changes in root dir.
"""

import os.path
import sys

import sqlalchemy


assert(len(sys.argv) == 2)
engine = sqlalchemy.create_engine('sqlite:///{}'.format(sys.argv[1]))
meta = sqlalchemy.MetaData()
conn = engine.connect()
meta.reflect(bind=engine)

mission = sqlalchemy.Table('mission', meta, autoload=True, autoload_with=engine)
rootdir = conn.execute(sqlalchemy.select([mission.c.rootdir])).\
          fetchone()[0]
#We have a trailing slash. Don't replace it, so we get {ROOTDIR}/foo
#not {ROOTDIR}foo
if os.path.join(rootdir, '') == rootdir:
    rootdir = rootdir[:-1]

code = sqlalchemy.Table('code', meta, autoload=True, autoload_with=engine)
for rd in (rootdir, os.path.abspath(os.path.expanduser(rootdir))):
    q = code.update().where(code.c.arguments.like("%{}%".format(rd))).\
        values(arguments=sqlalchemy.func.replace(
            code.c.arguments, rd, '{ROOTDIR}'))
    rp = conn.execute(q)
    print('{} updated'.format(rp.rowcount))
    if rp.rowcount:
        #Got an update, so don't try updating with the expanded path
        rp.close()
        break
    else:
        rp.close()

conn.close()
engine.dispose()
