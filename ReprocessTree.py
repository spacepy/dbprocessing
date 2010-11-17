#!/usr/bin/env python2.6

import DBUtils2
import DBfile
import Diskfile
import DBqueue
import glob
import os.path
import os

#class EmptyQueue(Exception):
#    pass

class ReprocessTree(object):
    def __init__(self,
                 mission,
                 level,
                 debug=False):

        dbu = DBUtils2.DBUtils2(mission)
        dbu._openDB()
        dbu._createTableObjects()
        self.dbu = dbu
        self.debug = debug
        self.queue = DBqueue.DBqueue()
        self.level = level # the level to read and reprocess

    def _fillQueue(self):
        if not isinstance(self.level, (list, tuple)):
           self.level = [self.level]
        for lvl in self.level:
            sq1 = self.dbu.session.query(self.dbu.File).filter_by(data_level = lvl)
            for val in sq1:
                self.queue.append(val.file_id)
        if self.debug: print("\tFile queue contains %s" % (self.queue))

    def checkNewestVerion(self):
        self._fillQueue()
        for val in self.queue.popleftiter():
            sq1 = self.dbu.session.query(self.dbu.File.utc_file_date).filter_by(file_id = val).subquery()
            sq2 = self.dbu.session.query(self.dbu.File.product_id).filter_by(utc_file_date = sq1)
            p_ids = set(sq2.all())
            while len(p_ids) !=0:
                v2 = p_ids.pop()
                sq3 = self.dbu.session.query(self.dbu.File).filter_by(product_id = v2[0])
                print("%d %d" % (v2[0] , sq3.count()))

            1/0

    def checkExisitsOnDisk(self):
        # if the loop does not raise any exceptions then all the files in the DB are in the directory if they should be
        self._fillQueue()
        for val in self.queue.popleftiter():
            fname = self.dbu._getFilename(val)
            basedir = self.dbu._getMissionDirectory()
            # search the whole dir tree for it
            # TODO do I really want to do this or just look where it should be?
            foundname = False
            for v1 in os.walk(basedir):
                for v2 in v1:
                    if fname in v2:
                        foundname = True
            if foundname == False:
                print("Found %s in DB that is not on disk" % (fname))
            else:
                if self.debug: print("\tFound %s (%d) on disk and in DB" %(fname, val))

    def checkDirectoryAndDB(self):
        # get the files in the directory and make sure they are all int he DB
        basedir = self.dbu._getMissionDirectory()
        files = glob.glob(basedir + '/data/*')
        filequeue = DBqueue.DBqueue()
        i = 0
        more = '/*'
        while True: # go forever deep and then break when we dont find anyting
            tmp = glob.glob(basedir + '/data/*' + more * i)
            if tmp == []:
                break
            filequeue.extend(tmp)
            i = i+ 1
        for val in filequeue.popleftiter():
            sq1 = self.dbu.session.query(self.dbu.File).filter_by(filename = os.path.basename(val)).count()
            if os.path.isdir(val) == True:
                continue  # found a directory not a data file
            elif sq1 == 0:
                print("File: %s found in directory and is not in DB" % (val))
            else:
                pass











