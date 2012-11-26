#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 26 15:37:21 2012

@author: balarsen
"""

import datetime
import os
import subprocess
import sys
import tempfile


if len(sys.argv) != 4:
    print('{0} YYYYmmdd YYYYmmdd a/b'.format(sys.argv[0]))
    sys.exit(0)


startT = datetime.datetime.strptime(sys.argv[1], '%Y%m%d')
stopT  = datetime.datetime.strptime(sys.argv[2], '%Y%m%d')

#tfile = tempfile.NamedTemporaryFile(delete=False)
#tfile.close()
#cmdline = [os.path.expanduser(os.path.join('~', '.local', 'bin', 'newMetaKernel.py')), '-p', '{0}'.format(tfile.name)]
#print 'running:', ' '.join(cmdline)
#subprocess.check_call(cmdline)

cmdline = [ os.path.expanduser(os.path.join('/', 'n', 'space_data', 'cda', 'rbsp', 'MagEphem', 'codes', 'MagEphemFromSpiceKernel_${HWSWID}_v1.0.0')),
                '-F', '-S {0}'.format(startT.strftime('%Y-%m-%d')), '-E {0}'.format(stopT.strftime('%Y-%m-%d')),
                '-e OP77Q', '-b rbspa', '/u/ectsoc/svnhome/dbprocessing/OneOffs/bigKer.ker',
                os.path.expanduser(os.path.join('/', 'n', 'space_data', 'cda', 'rbsp', 'MagEphem', 'predicted', sys.argv[3],
                                                'rbsp{0}_pre_MagEphem_OP77Q_%YYYY%MM%DD_v1.0.0'.format(sys.argv[3] )))]
print 'running:', ' '.join(cmdline)
subprocess.check_call(' '.join(cmdline), shell=True)


#os.remove(tfile.name)



