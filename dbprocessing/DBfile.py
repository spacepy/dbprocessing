import os
import shutil
import tarfile

import DBlogging
import Diskfile


__version__ = '2.0.3'


class DBfileError(Exception):
    """Exception that is raised by DBfile class

    @author: Brian Larsen
    @organization: Los Alamos National Lab
    @contact: balarsen@lanl.gov

    @version: V1: 05-Oct-2010 (BAL)
    """
    pass




class DBfile(object):
    """
    DBfile class is an extension of Diskfile that takes a physical file on disk and
    maps it to the database file entry, this is not mapped to te file table in the DB
    but instead a bridge between the two.

    @author: Brian Larsen
    @organization: Los Alamos National Lab
    @contact: balarsen@lanl.gov

    @version: V1: 05-Oct-2010 (BAL)
    """
    def __init__(self,
                 diskfile, dbu, makeDiskFile = False):
        """
        setup a DBfile classinputs a diskfile instance or a filename with the makeDiskFile keyword

        @TODO - do we need this keyword or the functionality?

        @param diskfile: a diskfile instance to create a DBfile from
        @type infile: Diskfile
        @param dbu: pass in the current DBUtils session so that a new connection is not made
        @type dbu: DBUtils

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 05-Oct-2010 (BAL)
        """
        if makeDiskFile == True:
            diskfile = Diskfile.Diskfile(diskfile)
        if not isinstance(diskfile, Diskfile.Diskfile):
            raise(DBfileError('Wrong input, must input a Diskfile object'))

        # this keeps opening connections
        #dbu = DBUtils.DBUtils(diskfile.mission)
        #dbu._openDB()
        #dbu._createTableObjects()
        self.dbu = dbu
        self.diskfile = diskfile
        self.checkVersion()

    def __repr__(self):
        return "<DBfile.DBfile object: {0}>".format(self.diskfile.infile)

    __str__ = __repr__

    def checkVersion(self):
        """
        checks the DBfile to see if it is the newest version

        @return: True the file is newest, False it is not
        @rtype: bool

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 05-Oct-2010 (BAL)
        @TODO add code here
        """
        self.diskfile.params['newest_version'] = True
        return True


    def addFileToDB(self):
        """
        wrapper around DBUtils.addFile to take params dict to keywords

        @return: the file_id of the newly added file
        @rtype: long

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 05-Oct-2010 (BAL)
        """
        f_id = self.dbu.addFile(filename = self.diskfile.params['filename'],
                        data_level = self.diskfile.params['data_level'],
                        version = self.diskfile.params['version'],
                        file_create_date = self.diskfile.params['file_create_date'],
                        exists_on_disk =  self.diskfile.params['exists_on_disk'],
                        product_id = self.diskfile.params['product_id'],
                        utc_file_date =self.diskfile.params['utc_file_date'],
                        utc_start_time = self.diskfile.params['utc_start_time'],
                        utc_stop_time = self.diskfile.params['utc_stop_time'],
                        check_date = self.diskfile.params['check_date'],
                        verbose_provenance = self.diskfile.params['verbose_provenance'],
                        quality_comment = self.diskfile.params['quality_comment'],
                        caveats = self.diskfile.params['caveats'],
                        met_start_time = self.diskfile.params['met_start_time'],
                        met_stop_time = self.diskfile.params['met_stop_time'],
                        newest_version = self.diskfile.params['newest_version'],
                        shasum = self.diskfile.params['shasum'],
                        process_keywords = self.diskfile.params['process_keywords'])
        return f_id

    def getDirectory(self):
        """
        query the DB and get the directory that the file should exist in

        @return: the full path for the DBfile
        @rtype: str

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 05-Oct-2010 (BAL)
        """
        relative_path = self.dbu.session.query(self.dbu.Product.relative_path).filter_by(product_id  = self.diskfile.params['product_id'])
        if relative_path.count() > 1:
            raise(DBfileError('more than one rel path found'))
        if relative_path.count() == 0:
            raise(DBfileError('zero rel path found'))
        relative_path = relative_path.all()[0][0]
        basepath = self.dbu.getMissionDirectory()
        path = os.path.join(str(basepath), str(relative_path))
        return path

    def move(self):
        """
        Move the DBfile from its current location to where it belongs

        If the file is a symbolic link it is assumed already in the target directory and not
        moved, the link is just removed

        @return: the from and to arguments to move with full path info
        @rtype: list

        """
        path = self.getDirectory()
        ## need to do path replacements
        path = self._doDirSubs(path)
        
        # if the file is a link just remove the link and pretend we moved it, this means
        # that this file is tracked only as a dependency
        if os.path.islink(self.diskfile.infile):
            os.unlink(self.diskfile.infile)
            DBlogging.dblogger.info("file {0} was a link, it was added and removed".format(os.path.basename(self.diskfile.infile)))
        else:
            try:
                shutil.move(self.diskfile.infile, os.path.join(path, self.diskfile.params['filename']))
            except IOError:
                dirname = os.path.split(os.path.join(path, self.diskfile.params['filename']))[0]
                os.makedirs(dirname)
                DBlogging.dblogger.warning("created a directory to put the date into: {0}".format(dirname))
                shutil.move(self.diskfile.infile, os.path.join(path, self.diskfile.params['filename']))
            DBlogging.dblogger.info("file {0} moved to {1}".format(os.path.basename(self.diskfile.infile), os.path.dirname(os.path.join(path, self.diskfile.params['filename']))))
            DBlogging.dblogger.debug("self.diskfile.filename: {0}".format(self.diskfile.filename))
            # if the file we are moving is a tgz file then we want to extract it in the place we moved it to and move the tgz file into a tgz directory
            try:
                tf = tarfile.open(os.path.join(path, self.diskfile.params['filename']), 'r:gz')
                if tf.getmembers(): # false if it does nat have any files inside i.e. is not a tarfile or empty, deal with empty later
                    tf.extractall(path=os.path.join(path, '..')) # up one dir level
                tf.close()
            except tarfile.ReadError:
                pass

        return (self.diskfile.infile, os.path.join(path, self.diskfile.params['filename']))

    def _doDirSubs(self, path):
        """
        do any substitutions that are needed to put ting in the right place
        # Honored database substitutions used as {Y}{MILLI}{PRODUCT}
        #	Y: 4 digit year
        #	m: 2 digit month
        #	b: 3 character month (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)
        #	d: 2 digit day
        #	y: 2 digit year
        #	j: 3 digit day of year
        #	H: 2 digit hour (24-hour time)
        #	M: 2 digit minute
        #	S: 2 digit second
        #	VERSION: version string, interface.quality.revision
        #	DATE: the UTC date from a file, same as Ymd
        #	MISSION: the mission name from the db
        #	SPACECRAFT: the spacecraft name from the db
        #	PRODUCT: the product name from the db
        """
        if '{INSTRUMENT}' in path or '{SATELLITE}' in path:        
            ftb = self.dbu.getFileTraceback(self.diskfile.params['filename'])
            if '{INSTRUMENT}' in path : # need to replace with the instrument name
                path = path.replace('{INSTRUMENT}', ftb['instrument'].instrument_name)
            if '{SATELLITE}' in path : # need to replace with the instrument name
                path = path.replace('{SATELLITE}', ftb['satellite'].satellite_name)
            if '{SPACECRAFT}' in path : # need to replace with the instrument name
                path = path.replace('{SPACECRAFT}', ftb['satellite'].satellite_name)
            if '{MISSION}' in path:
                path = path.replace('{MISSION}', ftb['mission'].mission_name)
            if '{PRODUCT}' in path:
                path = path.replace('{PRODUCT}', ftb['product'].product_name)

        if '{Y}' in path:
            path = path.replace('{Y}', self.diskfile.params['utc_file_date'].strftime('%Y'))   
        if '{m}' in path:
            path = path.replace('{m}', self.diskfile.params['utc_file_date'].strftime('%m'))   
        if '{d}' in path:
            path = path.replace('{d}', self.diskfile.params['utc_file_date'].strftime('%d'))   
        if '{b}' in path:
            months = {1:'Jan', 2:'Feb', 3:'Mar', 4:'Apr', 5:'May', 6:'Jun', 7:'Jul', 8:'Aug', 9:'Sep', 10:'Oct', 11:'Nov', 12:'Dec'}
            path = path.replace('{b}', months[self.diskfile.params['utc_file_date'].month])
        if '{y}' in path:
            path = path.replace('{y}', self.diskfile.params['utc_file_date'].strftime('%y'))                       
        if '{j}' in path:
            path = path.replace('{j}', self.diskfile.params['utc_file_date'].strftime('%j'))               
        if '{H}' in path:
            path = path.replace('{H}', self.diskfile.params['utc_start_time'].strftime('%H'))
        if '{M}' in path:
            path = path.replace('{M}', self.diskfile.params['utc_start_time'].strftime('%M'))
        if '{S}' in path:
            path = path.replace('{S}', self.diskfile.params['utc_start_time'].strftime('%S'))
        if '{VERSION}' in path:
            path = path.replace('{VERSION}', '{0}.{1}.{2}'.format(self.diskfile.params['interface_version'],
                                                                  self.diskfile.params['quality_version'],
                                                                  self.diskfile.params['revision_version']))
        if '{DATE}' in path:
             path = path.replace('{DATE}', self.diskfile.params['utc_file_date'].strftime('%Y%m%d'))
        return path



