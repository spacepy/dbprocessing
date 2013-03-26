#!/usr/bin/env python2.6

"""
in a given directory make symlinks to all the newest versions of files into another directory
"""

import itertools
import glob
import os
from optparse import OptionParser, OptionGroup
import traceback
import warnings

from rbsp import Version

from dbprocessing import inspector


################################################################
# 1) In the current directory get all the file ids from the directory
# 2) If those files are not current version remove them from set
# 3) Create sumlinks to the file in a specified dir (latest by default)
################################################################




def get_all_files(indir, glb='*'):
    """
    in indir get all the files that follow the glob glb
    - indir is a full path
    - glb is a file glob 
    """
    files = glob.glob(os.path.join(indir, glb))
    return files

def cull_to_newest(files):
    """
    given a list of files cull to only the newest ones
    """
    # make a list of tuples,  datetime, version, filename, product part of filename 
    date_ver = [(inspector.extract_YYYYMMDD(v), inspector.extract_Version(v), v, v.split('20')[0]) for v in files
                if inspector.extract_YYYYMMDD(v) is not None]
    date_ver = sorted(date_ver, key=lambda x: x[0])
    u_dates = set(zip(*date_ver)[0])

    # cycle over all the u_dates and keep the newest version of each
    u_prods = list(set(zip(*date_ver)[3]))  # get the unique products
    ans = []
    for d, p in itertools.product(u_dates, u_prods):
        tmp = [v for v in date_ver if v[0]==d and v[3]==p]
        if tmp:
            ans.append(max(tmp, key=lambda x: x[1])[2])

    return ans

def make_symlinks(files, outdir, options):
    """
    for all the files make symlinks into outdir
    """
    if not hasattr(files, '__iter__'):
        files = [files]
    for f in files:
        try:
            os.symlink(f, os.path.join(outdir, os.path.basename(f)))
        except OSError:
            if options.force:
                os.remove(os.path.join(outdir, os.path.basename(f)))
                make_symlinks(f, outdir, options)
        except:
            warnings.warn("File {0} not linked:\n\t{1}".format(f, traceback.format_exc()))


def delete_symlinks(outdir):
    """
    delete all symlinks in outdir
    """
    files = glob.glob(os.path.join(outdir, '*'))
    for f in files:
        if os.path.islink(f):
            try:
                os.unlink(os.path.join(outdir, f))
            except OSError:
                warnings.warn("Link could not be deleted: {0}".format(os.path.join(outdir, f)))
  


if __name__ == '__main__':
    usage = "usage: %prog indir"
    parser = OptionParser(usage=usage)
    parser.add_option("-g", "--glob",
                  dest="glb",
                  help="The glob to use for files", default='*')
    parser.add_option("-f", "--force",
                  dest="force", action='store_true',
                  help="Allow symlinks to overwrite exists links of same name", default=False)
    parser.add_option("-o", "--outdir",
                  dest="outdir", 
                  help="Output directory for symlinks", default='latest')
    parser.add_option("-d", "--delete",
                  dest="delete", action='store_true',
                  help="Delete all the existing symlinks in the destination directory", default=False)    

    (options, args) = parser.parse_args()

    if len(args) != 1:
        parser.error("incorrect number of arguments")

    indir  = os.path.abspath(os.path.expanduser((os.path.expandvars(args[0]))))
    if options.outdir == 'latest':
        outdir = os.path.join(indir, options.outdir)
    else:
        outdir = os.path.abspath(os.path.expanduser((os.path.expandvars(options.outdir))))

    if indir == outdir:
        parser.error("outdir cannor be the same as indir, would clobber files")
        
    if not os.path.isdir(outdir):
        if options.force:
            os.makedirs(outdir)
        else:
            parser.error("outdir: {0} does not exist, create or use --force".format(outdir))

        
    files = get_all_files(indir, options.glb)
    files = cull_to_newest(files)
    if options.delete:
        delete_symlinks(outdir)
    make_symlinks(files, outdir, options)


