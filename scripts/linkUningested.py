#!/usr/bin/env python

"""Find files that have not been ingested and link them to incoming"""

import argparse
import os.path
import posixpath
import re
import sys

import dbprocessing.DButils
import dbprocessing.DBstrings


def parse_args(argv=None):
    """Parse arguments for this script

    Parameters
    ----------
    argv : list
        Argument list, default from sys.argv

    Returns
    -------
    kwargs : dict
        Keyword arguments for :func:`main`.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mission", required=True,
                        help="selected mission database")
    parser.add_argument("-p", "--product", action='append', dest="products",
                        help="Product name or ID to check; specify multiple"
                        " times for multiple products (default: all).")
    options = parser.parse_args(argv)
    return vars(options)


def list_files(dbu, product):
    """List all files on disk matching a product

    Parameters
    ----------
    dbu : dbprocess.DButils.DButils
        Open mission database

    product : int or str
        Product name or ID

    Returns
    -------
    list
        Path to all matching files relative to mission directory.
    """
    prod = dbu.getEntry('Product', product)
    tb = dbu.getTraceback('Product', prod.product_id)
    kwargs = {'INSTRUMENT': tb['instrument'].instrument_name,
              'MISSION': tb['mission'].mission_name,
              'PRODUCT': prod.product_name,
              'SATELLITE': tb['satellite'].satellite_name,
              'SPACECRAFT': tb['satellite'].satellite_name,
              }
    fmtr = dbprocessing.DBstrings.DBformatter()
    pat = fmtr.re(prod.format, **kwargs)
    proddir = fmtr.re(prod.relative_path, **kwargs)
    md = os.path.normpath(dbu.getMissionDirectory())
    # Because in general the relative path may include wildcards,
    # we have to go through everything. It would be a nice improvement
    # to find the static parts of the relative path and only hit that...
    # spacepy.datamanager would do that for us.
    pat = posixpath.normpath(posixpath.join(proddir, pat))
    if os.path.sep == '\\':  # Path-separator and regex escape are ambiguous.
        # This path is well-formatted (from normpath); can be handled naively.
        pat = '\\\\'.join(pat.split(posixpath.sep))
    flist = (os.path.join(dn, f)[len(md) + 1:]
             for dn, _, fnames in os.walk(md) for f in fnames)
    flist = [f for f in flist if re.match(pat, f)]
    return flist


def indb(dbu, fname, prod):
    """Checks if a filename is in database

    Parameters
    ----------

    dbu : dbprocess.DButils.DButils
        Open mission database

    fname : str
        Filename (no pathing)

    product : int
        Product ID

    Returns
    -------
    bool
        True if file with that name and product are present.
    """
    return bool(dbu.session.query(dbu.File)
                .filter_by(product_id=prod, filename=fname).count())


def main(mission, products=None):
    """Check for files not in database

    Find all files that match a product specification but are not in the
    database and symlink them to incoming.

    Parameters
    ----------
    mission : str
        Path to the mission file

    products : list
        Product names or IDs to check; default all
    """
    dbu = dbprocessing.DButils.DButils(mission)
    md = os.path.normpath(dbu.getMissionDirectory())
    plist = dbu.getAllProducts() if products is None \
            else [dbu.getEntry('Product', p) for p in products]
    inc = dbu.getIncomingPath()
    for prod in plist:
        flist = list_files(dbu, prod.product_id)
        missing = [f for f in flist
                   if not indb(dbu, os.path.basename(f), prod.product_id)]
        for m in missing:
            os.symlink(os.path.join(md, m),
                       os.path.join(inc, os.path.basename(m)))


if __name__ == "__main__":
    main(**parse_args())

