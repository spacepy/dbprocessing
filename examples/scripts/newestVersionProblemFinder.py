#!/usr/bin/env python

"""
Run both the old and new methods of finding newestVersion and report differences
"""

import argparse
from operator import itemgetter
from datetime import date

from sqlalchemy.sql import func

from dbprocessing import DButils

def oldGetFilesByProductDate(dbu, product_id, daterange, newest_version=False):
    """
    Return the files in the db by product id that have data in the date specified
    """
    dates = []
    for d in daterange:
        try:
            dates.append(d.date())
        except AttributeError:
            dates.append(d)

    if newest_version:
        # don't trust that the db has this correct
        # create a tabel populated with
        #   versionnum, file_id, utc_file_date


        # BUG DISCOVERED 2014-12-6 BAL
        # the logic in these queries does not use the max version for each utc_file_date
        # independently but instead the max in a range
        # this workaround fixes this but could be better

        aa = (dbu.session.query((dbu.File.interface_version * 1000
                                  + dbu.File.quality_version * 100
                                  + dbu.File.revision_version).label('versionnum'),
                                 dbu.File.file_id,
                                 dbu.File.filename, dbu.File,
                                 dbu.File.utc_file_date)
              .filter(dbu.File.utc_file_date.between(*dates))
              .filter(dbu.File.product_id == product_id)
              # .order_by(dbu.File.filename.asc())
              .group_by(dbu.File.utc_file_date, 'versionnum')).all()

        sq = []
        for ele in aa:
            tmp = [v for v in aa if v[4] == ele[4]]
            t2 = max(tmp, key=lambda x: x[0])
            sq.append(t2[2])
        sq = sorted(list(set(list(sq))))

    else:
        sq = dbu.session.query(dbu.File).filter_by(product_id=product_id). \
            filter(dbu.File.utc_file_date.between(dates[0], dates[1])).all()
    return sq

def _makeVersionnumTable(dbu, prod_id=None):
    # don't trust that the db has this correct
    # create a table populated with
    #   versionnum, file_id, utc_file_date
    if prod_id is None:
        version = (dbu.session.query((dbu.File.interface_version * 1000
                                       + dbu.File.quality_version * 100
                                       + dbu.File.revision_version).label('versionnum'),
                                      dbu.File.file_id,
                                      dbu.File.utc_file_date,
                                      dbu.File.product_id)
                   .group_by(dbu.File.file_id).group_by(dbu.File.product_id).subquery())
    else:
        version = (dbu.session.query((dbu.File.interface_version * 1000
                                       + dbu.File.quality_version * 100
                                       + dbu.File.revision_version).label('versionnum'),
                                      dbu.File.file_id,
                                      dbu.File.utc_file_date,
                                      dbu.File.product_id)
                   .filter(dbu.File.product_id == prod_id)
                   .group_by(dbu.File.file_id).subquery())
    return version

def _makeVersionnumSubquery(dbu, prod_id=None):
    version = _makeVersionnumTable(dbu=dbu, prod_id=prod_id)

    subq = (dbu.session.query(func.max(version.c.versionnum))
            .group_by(version.c.utc_file_date))
    subq = subq.scalar_subquery() if hasattr(subq, 'scalar_subquery')\
           else subq.subquery().as_scalar() # Deprecated 1.4
    return subq

def oldGetFilesByProduct(dbu, prod_id, newest_version=False):
    """
    Given a product_id or name return all the file instances associated with it

    if newest is set return only the newest files
    """
    prod_id = dbu.getProductID(prod_id)
    if newest_version:
        version = _makeVersionnumTable(dbu=dbu, prod_id=prod_id)
        subq = _makeVersionnumSubquery(dbu=dbu, prod_id=prod_id)

        sq = dbu.session.query(version.c.file_id).filter(version.c.versionnum == subq).all()

        sq = list(map(itemgetter(0), sq))
        sq = dbu.session.query(dbu.File).filter(dbu.File.file_id.in_(sq))

    else:
        sq = dbu.session.query(dbu.File).filter_by(product_id=prod_id)
    return sq.all()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mission", dest="mission", required=True,
                        help="selected mission database")
    parser.add_argument("--settest", dest="settest", default=True, action="store_true",
                        help="Run the tests that compare the results of the \
                        old and new implementations of newest_version")
    parser.add_argument("--errortest", dest="errortest", default=True, action="store_true",
                        help="Check if any file has a newer file_create_date but lower version number")

    options = parser.parse_args()

    dbu = DButils.DButils(options.mission)

    if options.settest:
        new = set(dbu.getAllFilenames(fullPath=False, newest_version=True))
        old1 = set()
        old2 = set()

        prods = dbu.getAllProducts(id_only=True)

        for p in prods:
            old1.update(oldGetFilesByProductDate(dbu, p, [date(1970, 1, 1), date(2070, 1, 1)], True))
            old2.update(d.filename for d in oldGetFilesByProduct(dbu, p, True))

        print("Sizes - New: {0}, getFilesByProductDate: {1}, getFilesByProduct: {2}".format(len(new), len(old1), len(old2)))

        for x in new - old1:
            print("{0} in new but not getFilesByProductDate".format(x))

        for y in old1 - new:
            print("{0} in getFilesByProductDate but not new".format(y))

        for x in new - old2:
            print("{0} in new but not getFilesByProduct".format(x))

        for y in old2 - new:
            print("{0} in getFilesByProduct but not new".format(y))

    if options.errortest:
        files = dbu.getFiles(newest_version=True)
        for f in files:
            files2 = dbu.getFiles(product=f.product_id, startDate=f.utc_file_date, endDate=f.utc_file_date)
            for f2 in files2:
                if f.file_create_date < f2.file_create_date:
                    print('{0} is the "Newest Version", however was created after {1}'.format(f.filename, f2.filename))
    del dbu
