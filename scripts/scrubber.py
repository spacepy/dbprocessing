#!/usr/bin/env python

import argparse

from dbprocessing import DButils

class scrubber(object):
    def __init__(self, mission):
        self.dbu = DButils.DButils(mission)

    def __del__(self):
        del self.dbu

    def parents_are_newest(self):
        n = self.dbu.getAllFileIds(newest_version=True)

        xp = self.dbu.session.query(self.dbu.Filefilelink.source_file).filter(self.dbu.Filefilelink.resulting_file.in_(n)).all()
        np = set(list(zip(*xp))[0])

        if np.issubset(n):
            print("All parents of newest are newest")
        else:
            print("Parents of newest aren't newest")
            print(np.difference(n))

    def version_number_check(self):
        x = self.dbu.session.execute("SELECT max(interface_version), max(quality_version), max(revision_version) FROM file").fetchone()
        if x[0] >= 1000:
            print("A interface version is too large")
        if x[1] >= 1000:
            print("A quality version is too large")
        if x[2] >= 1000:
            print("A revision version is too large")
        

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-m",
        "--mission",
        required=True,
        help="selected mission database",
        default=None)

    options = parser.parse_args()

    scrubber = scrubber(options.mission)
    scrubber.parents_are_newest()
    scrubber.version_number_check()
    del scrubber
