#!/usr/bin/env python2.6

"""
in a given directory make symlinks to all the newest versions of files into another directory
"""

import itertools
import glob
import os
from optparse import OptionParser
import re
import traceback
import warnings

from rbsp import Version

from dbprocessing import inspector


################################################################
# 1) In the current directory get all the file ids from the directory
# 2) If those files are not current version remove them from set
# 3) Create sumlinks to the file in a specified dir (latest by default)
################################################################


def argsort(seq):
    #http://stackoverflow.com/questions/3382352/equivalent-of-numpy-argsort-in-basic-python/3382369#3382369
    #by ubuntu
    return sorted(range(len(seq)), key=seq.__getitem__)

def get_all_files(indir, glb='*'):
    """
    in indir get all the files that follow the glob glb
    - indir is a full path
    - glb is a file glob
    """
    files = glob.glob(os.path.join(indir, glb))
    return files

def getBaseVersion(f):
    """
    given an input filename return a tuple of base an version
    """
    base = re.split(r'v\d\d?\.\d\d?\.\d\d?\.', f)[0]
    version = inspector.extract_Version(f)
    return base, version

def cull_to_newest(files, nodate=False, options=None):
    """
    given a list of files cull to only the newest ones

    if nodate is set just match everything in front of v\d\d?\.\d\d?\.\d\d?\.
    """
    if not nodate:
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
                ans.append(max(tmp, key=lambda x: x[2])[2])
        return ans
    else:
        ans = []
        # make a set of all the file bases
        bases = []
        versions = []
        for f in files:
            tmp = getBaseVersion(f)
            if options.all:
                if options.verbose: print("Added file1 {0}".format(f))
                bases.append(tmp[0])
                versions.append(tmp[1])
            elif tmp[1] is not None:
                if options.verbose: print("Added file2 {0}".format(f))
                bases.append(tmp[0])
                versions.append(tmp[1])
            else:
                if options.verbose: print("Skipped file {0}".format(f))
        uniq_bases = list(set(bases))
        for ub in uniq_bases:
            if bases.count(ub) == 1: # there is only one
                ans.append(files[bases.index(ub)])
            else: # must be more than 
                indices = [i for i, x in enumerate(bases) if x == ub]
                tmp = []
                for i in indices:
                    tmp.append((bases[i], versions[i], files[i]))
                if options.verbose: print("tmp:: {0}".format(tmp))
                if options.verbose: print("tmpmax:: {0}".format(max(tmp, key=lambda x: x[1])[2]))
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
            if os.path.isfile(f):
                if options.verbose: print("linking1 {0}->{1}".format(f, os.path.join(outdir, os.path.basename(f))))
                os.symlink(f, os.path.join(outdir, os.path.basename(f)))
            elif options.dir:
                if options.verbose: print("linking2 {0}->{1}".format(f, os.path.join(outdir, os.path.basename(f))))                
                os.symlink(f, os.path.join(outdir, os.path.basename(f)))

        except OSError:
            if options.force:
                os.remove(os.path.join(outdir, os.path.basename(f)))
                if options.verbose: print("linking3 {0}->{1}".format(f, os.path.join(outdir, os.path.basename(f))))                
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
    parser.add_option("", "--nodate",
                  dest="nodate", action='store_true',
                  help="Do not use the date part of a filename in finding latest", default=False)
    parser.add_option("", "--dir",
                  dest="dir", action='store_true',
                  help="Also make symlinks of directories", default=False)
    parser.add_option("", "--all",
                  dest="all", action='store_true',
                  help="Make symlinks for files that do not have vx.y.z versions", default=False)
    parser.add_option("", "--verbose",
                  dest="verbose", action='store_true',
                  help="Print out verbose information", default=False)


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
    files = cull_to_newest(files, nodate=options.nodate, options=options)
    if options.delete:
        delete_symlinks(outdir)
    make_symlinks(files, outdir, options)


