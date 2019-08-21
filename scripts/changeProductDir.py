#!/usr/bin/env python

"""Change the directory for a product and move files

This includes creating subdirectories where necessary.
"""

import argparse
import os
import os.path

import dbprocessing.DButils


class ProductMoveApp(object):
    """Move a product from one directory to another"""

    def __init__(self, mission, product, newdir):
        self.mission = mission
        """Path to mission file"""
        self.product = product
        """Product name or ID to move"""
        self.newdir = newdir
        """dbp-style specification of new directory. Accepts DBStrings format
           specifiers."""

    def main(self):
        self.dbu = dbprocessing.DButils.DButils(self.mission)
        pid = self.dbu.getProductID(self.product)
        #First get all the OLD file locations
        oldfiles = [(f.file_id, self.dbu.getFileFullPath(f.file_id))
                     for f in self.dbu.getFiles(product=pid)
                    if f.exists_on_disk]
        product = self.dbu.getEntry('Product', pid)
        product.relative_path = self.newdir
        self.dbu.session.commit()
        for fileid, oldpath in oldfiles:
            newpath = self.dbu.getFileFullPath(fileid)
            newdir = os.path.dirname(newpath)
            if not os.path.isdir(newdir):
                os.makedirs(newdir)
            os.rename(oldpath, newpath)
        self.dbu.closeDB()

    @classmethod
    def from_args(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            '-m', '--mission', dest='mission',
            help='Path to mission database file', required=True)
        parser.add_argument('product',
                            help='Product name or ID to move')
        parser.add_argument('newdir',
                            help='Specification of new directory')
        args = parser.parse_args()
        return cls(**vars(args))


if __name__ == "__main__":
    ProductMoveApp.from_args().main()
