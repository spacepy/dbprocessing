"""Database representations of data files and operations."""

from __future__ import absolute_import
from __future__ import print_function

import os
import shutil
import tarfile

from . import DBlogging
from . import Diskfile
from . import Utils


class DBfileError(Exception):
    """Exception that is raised by DBfile class"""
    pass


class DBfile(object):
    """Maps a physical file on disk to database file entry.

    DBfile class is an extension of Diskfile that takes a physical
    file on disk and maps it to the database file entry, this is not
    mapped to the file table in the DB but instead a bridge between
    the two.
    """

    def __init__(self, diskfile, dbu, makeDiskFile=False):
        """
        Setup a DBfile class.

        .. todo:: Do we need this keyword or the functionality?

        Parameters
        ----------
        diskfile : :class:`str` or :class:`.Diskfile`
            A file name or diskfile instance to create a DBfile from
        dbu : :class:`~.DButils.DButils`, optional
            Current database connection. If not specified, creates a
            new connection.
        makeDiskFile : :class:`bool`, default False
            If true, ``diskfile`` is a filename and needs a
            :class:`.Diskfile` made.
        """
        if makeDiskFile:
            diskfile = Diskfile.Diskfile(diskfile, dbu)
        if not isinstance(diskfile, Diskfile.Diskfile):
            raise DBfileError('Wrong input, must input a Diskfile object')

        self.dbu = dbu
        self.diskfile = diskfile

    def __repr__(self):
        return "<DBfile.DBfile object: {0}>".format(self.diskfile.infile)

    __str__ = __repr__

    def addFileToDB(self):
        """
        Wrapper around :meth:`~.DButils.addFile` to take params dict to keywords

        Returns
        -------
        :class:`int`
            :sql:column:`~file.file_id` of the newly added file
        """
        return self.dbu.addFile(**self.diskfile.params)

    def getDirectory(self):
        """
        Query the DB and get the directory that the file should exist in

        Returns
        -------
        :class:`str`
            The full path for the DBfile
        """
        relative_path = self.dbu.session.query(self.dbu.Product.relative_path).filter_by(
            product_id=self.diskfile.params['product_id'])

        relative_path = relative_path.all()

        if len(relative_path) > 1:
            raise DBfileError('more than one rel path found')
        if len(relative_path) == 0:
            raise DBfileError('zero rel path found')

        basepath = self.dbu.getMissionDirectory()
        return os.path.join(basepath, relative_path[0][0])

    def move(self):
        """
        Move the DBfile from its current location to where it belongs

        If the file is a symbolic link it is assumed already in the target
        directory and not
        moved, the link is just removed

        Returns
        -------
        :class:`tuple` of :class:`str`
            from and to arguments to move with full path info
        """
        path = self.getDirectory()
        ## need to do path replacements
        path = Utils.dirSubs(path, self.diskfile.params['filename'], self.diskfile.params['utc_file_date'],
                             self.diskfile.params['utc_start_time'], '{0}'.format(str(self.diskfile.params['version'])))

        # if the file is a link just remove the link and pretend we moved it, this means
        # that this file is tracked only as a dependency
        if os.path.islink(self.diskfile.infile):
            os.unlink(self.diskfile.infile)
            DBlogging.dblogger.info(
                "file {0} was a link, it was added and removed".format(os.path.basename(self.diskfile.infile)))
        else:
            try:
                shutil.move(self.diskfile.infile, os.path.join(path, self.diskfile.params['filename']))
            except IOError:
                dirname = os.path.split(os.path.join(path, self.diskfile.params['filename']))[0]
                os.makedirs(dirname)
                DBlogging.dblogger.warning("created a directory to put the date into: {0}".format(dirname))
                shutil.move(self.diskfile.infile, os.path.join(path, self.diskfile.params['filename']))
            DBlogging.dblogger.info("file {0} moved to {1}".format(os.path.basename(self.diskfile.infile),
                                                                   os.path.dirname(os.path.join(path,
                                                                                                self.diskfile.params[
                                                                                                    'filename']))))
            DBlogging.dblogger.debug("self.diskfile.filename: {0}".format(self.diskfile.filename))
            # if the file we are moving is a tgz file then we want to extract it in the place we moved it to and move the tgz file into a tgz directory
            try:
                tf = tarfile.open(os.path.join(path, self.diskfile.params['filename']), 'r:gz')
                if tf.getmembers():  # false if it does nat have any files inside i.e. is not a tarfile or empty, deal with empty later
                    tf.extractall(path=os.path.join(path, '..'))  # up one dir level
                tf.close()
            except tarfile.ReadError:
                pass

        return (self.diskfile.infile, os.path.join(path, self.diskfile.params['filename']))
