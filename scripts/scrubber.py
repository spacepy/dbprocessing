#!/usr/bin/env python

from optparse import OptionParser

from dbprocessing import DButils

class scrubber(object):
    def __init__(self, mission):
        self.dbu = DButils.DButils(mission)

    def parents_are_newest(self):
        x = self.dbu.getFiles(newest_version=True)
        n = set([i.file_id for i in x])

        xp = self.dbu.session.query(self.dbu.Filefilelink.source_file).filter(self.dbu.Filefilelink.resulting_file.in_(n)).all()
        np = set(zip(*xp)[0])

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
    usage = "usage: %prog -m database"
    parser = OptionParser(usage=usage)
    parser.add_option(
        "-m",
        "--mission",
        dest="mission",
        help="selected mission database",
        default=None)

    (options, args) = parser.parse_args()

    scrubber = scrubber(options.mission)
    #scrubber.parents_are_newest()
    scrubber.version_number_check()