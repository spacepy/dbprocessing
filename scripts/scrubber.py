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
            print("True")
        else:
            print("Parents of newest aren't newest")
            print(np.difference(n))

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
    scrubber.parents_are_newest()