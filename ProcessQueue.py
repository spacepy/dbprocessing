#!/usr/bin/env python2.6

import DBUtils2
import DBqueue
import shutil
import DBfile
import Diskfile
import os.path
import Executor

import DBlogging

__version__ = '2.0.2'


class ProcessException(Exception):
    """Class for errors in ProcessQueue"""
    pass


class ForException(Exception):
    """Cheezy but separate excpetion for breaking out of a nested loop"""
    pass


class ProcessQueue(object):
    """
    Main code used to process the Queue, looks in incioming and builds all
    possible files

    @author: Brian Larsen
    @organization: Los Alamos National Lab
    @contact: balarsen@lanl.gov

    @version: V1: 02-Dec-2010 (BAL)
    """
    def __init__(self,
                 mission):

        dbu = DBUtils2.DBUtils2(mission)
        dbu._openDB()
        dbu._createTableObjects()
        self.dbu = dbu
        self.childrenQueue = DBqueue.DBqueue()
        self.moved = DBqueue.DBqueue()
        self.depends = DBqueue.DBqueue()
        self.queue = DBqueue.DBqueue()
        self.findChildren = DBqueue.DBqueue()
        DBlogging.dblogger.info("Entering ProcessQueue")

    def depDict(self, infile, file=None, code=None):
        """
        Dict to keep track of dependencies
        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 02-Dec-2010 (BAL)
        """

        DBlogging.dblogger.debug("Entered depDict:")

        # maybe this should be a class
        if not isinstance(file, (list, tuple)):
            file = [file]
        if not isinstance(code, (list, tuple)):
            code = [code]
        return {'infile':infile, 'code':code, 'file':file}


    def checkIncoming(self):
        """
        Goes out to incoming and grabs all files there adding them to self.queue

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 02-Dec-2010 (BAL)
        """
        DBlogging.dblogger.debug("Entered checkIncoming:")

        self.queue.extendleft(self.dbu._checkIncoming())
        # step through and remove duplicates
        # if python 2.7 deque has a .count() otherwise have to use
        #  this workaropund
        for i in range(len(self.queue )):
            try:
                if list(self.queue).count(self.queue[i]) != 1:
                    self.queue.remove(self.queue[i])
            except IndexError:
                pass   # this means it was shortened
        DBlogging.dblogger.debug("Queue contains (%d): %s" % (len(self.queue),
                                                              self.queue))


    ## def doProcess(self):
    ##     DBlogging.dblogger.info("Entering doProcess()")
    ##     self.process()

    def moveToError(self, fname):
        """
        Moves a file from incoming to error

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 02-Dec-2010 (BAL)
        """
        DBlogging.dblogger.debug("Entered moveToError:")

        DBlogging.dblogger.warning("**ERROR** %s moved to %s"%(fname,
                                                '/n/projects/cda/Test/errors/'))

        if os.path.isfile('/n/projects/cda/Test/errors/' + \
                          os.path.basename(fname)):
        #TODO do I realy want to remove old version:?
            os.remove( '/n/projects/cda/Test/errors/' + \
                      os.path.basename(fname))
        shutil.move(fname, '/n/projects/cda/Test/errors/')


    def importFromIncoming(self):
        """
        Import a file from incoming into the database

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 02-Dec-2010 (BAL)
        """
        DBlogging.dblogger.debug("Entering importFromIncoming, %d to import" %  \
                                 (len(self.queue)))

        for val in self.queue.popleftiter() :
            self.current_file = val
            DBlogging.dblogger.debug("popped '%s' from the queue" % (val))
            fname = Diskfile.Diskfile(val)
            prod = fname.figureProduct()
            if prod == None:
                DBlogging.dblogger.debug("prod==None so moving to error")
                self.moveToError(val)
                continue
            DBlogging.dblogger.debug("fname created %s" % (fname.filename))
            if fname.mission == self.dbu.mission:
                # if the file is the wrong mission skip it
                dbf = DBfile.DBfile(fname)
                try:
                    f_id = dbf.addFileToDB()
                except (DBUtils2.DBInputError, DBUtils2.DBError) as errmsg:
                    DBlogging.dblogger.warning("Except adding file to db so" + \
                                               " moving to error: %s" % (errmsg))
                    self.moveToError(val)
                    continue

                # add to findChildren queue
                self.findChildren.append(dbf)

                DBlogging.dblogger.debug("f_id = %s" % (str(f_id)))
                self.depends.append(self.depDict(f_id))

                mov = dbf.move()
                self.moved.append(mov[1])  # only want to dest file
                DBlogging.dblogger.debug("file %s moved to  %s" % \
                                         (mov[0], mov[1]))

                DBlogging.dblogger.debug("Length of findChildren is %d" % \
                                         (len(self.findChildren)))

    def getProcessFromOutputProduct(self, outProd):
        """
        Gets process from the db that have the output product

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 02-Dec-2010 (BAL)
        """
        # TODO maybe this should move to DBUtils2
        DBlogging.dblogger.debug("Entered getProcessFromOutputProduct:")

        ans = []
        for val in outProd:
            sq1 =  self.dbu.session.query(self.dbu.Process.process_id).filter_by(output_product = val)  # should only have one value
            ans.extend( sq1[0] )
        return ans

    def getCodeFromProcess(self, proc_id):
        """
        given a process id return the code that makes perfoms that process

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 02-Dec-2010 (BAL)
        """
        DBlogging.dblogger.debug("Entered getCodeFromProcess:")

        ans = []
        for val in proc_id:
            sq1 =  self.dbu.session.query(self.dbu.Code.code_id).filter_by(process_id = val)  # should only have one value
            try:
                ans.extend( sq1[0])
            except IndexError:
                continue
        return ans

    def getCodePath(self, code_id):
        """
        Given a code_id list return the full name (path and all) of the code

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 02-Dec-2010 (BAL)
        """
        DBlogging.dblogger.debug("Entered getCodePath:")

        ans = []
        for val in code_id:
            sq1 =  self.dbu.session.query(self.dbu.Code.relative_path).filter_by(code_id = val)  # should only have one value
            sq2 =  self.dbu.session.query(self.dbu.Mission.rootdir).filter_by(mission_name = self.dbu.mission)  # should only have one value
            sq3 =  self.dbu.session.query(self.dbu.Code.filename).filter_by(code_id = val)  # should only have one value
            ans.append(sq2[0][0] + '/' + sq1[0][0]  + '/' + sq3[0][0])  # the [0][0] is ok (ish) since there can only be one
        return ans


    def buildChildren(self):
        """
        given the values in self.childrenQueue build them is possible

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 02-Dec-2010 (BAL)
        """
        # TODO maybe this should be broken up
        DBlogging.dblogger.debug("Entered buildChildren:")
        for val in self.childrenQueue.popleftiter():
            # val is a dbfile, product id tuple
            DBlogging.dblogger.debug("popleft %s from self.childrenQueue" % \
                                     (str(val)))

            # check to see if there is a process defined on this output product
            proc_id = self.getProcessFromOutputProduct([val[1]])[0]
            if proc_id == []:  #TODO should this be a None?
                continue # there is no process defined


            # since we have a process do we have a code that does it?
            code_id = self.getCodeFromProcess([proc_id])
            try:
                code_id = code_id[0]
            except IndexError:
                continue  # The code_id was []
            # figure out the codes name and path
            codep = self.getCodePath([code_id])[0]

            sq1 = self.dbu.session.query(self.dbu.Product).filter_by(product_id = val[1])[0]
            product = sq1.product_id
            root_path = self.dbu.session.query(self.dbu.Mission.rootdir).filter_by(mission_name = self.dbu.mission)[0][0]  # should only have one value

            date = val[0].diskfile.params['utc_file_date']
            version = val[0].diskfile.params['version']
            out_path = '/tmp/' + val[0].diskfile.makeProductFilename(product, date, version)
            # now we have everything it takes to build the file

            # ####### get all the input_product_id and make sure they all exist
            #    before we build the child.
            # from the process get all the input_product_id
            products = self.dbu._getInputProductID(proc_id)
            # query for the files that match the products for the right date
            # TODO this is another place that is one day to one day limited
            try:
                for pval in products:
                    sq1 = self.dbu.session.query(self.dbu.File).filter_by(product_id = pval).filter_by(utc_file_date = date)
                    if sq1.count() == 0:
                        DBlogging.dblogger.debug("Skipping file since " + \
                                                 "requirement not available" + \
                                                 "(sq1.count)")
                        raise(ForException())
                    DBlogging.dblogger.debug("<>Looking for product %d for " + \
                                             "date %s" % (pval, date))
                    # get an in_path for exe
                    in_path = self.dbu._getFileFullPath(val[0].diskfile.makeProductFilename(pval, date, version))
                    if in_path == None:
                        DBlogging.dblogger.debug("Skipping file since " + \
                                                 "requirement not available" + \
                                                 "(in_path)")
                        raise(ForException())
            except ForException:
                continue
            sq1 = self.dbu.session.query(self.dbu.Process).filter_by(process_id = proc_id)[0]
            extra_params_in = sq1.extra_params_in
            if extra_params_in != None:
                # now we need to play with what is in the query and parse it
                if '%DATE' in extra_params_in:
                    extra_params_in = extra_params_in.replace('%DATE',
                                                    date.strftime('%Y%m%d'))
                    DBlogging.dblogger.debug("-*-* extra_params_in=%s" %  \
                                             (extra_params_in))
                if '%OUTFILE' in extra_params_in:
                    extra_params_in = extra_params_in.replace('%OUTFILE',
                                                    out_path)
                    DBlogging.dblogger.debug("-*-* extra_params_in=%s" %  \
                                             (extra_params_in))
                # TODO what other %CODEs are allowed?
                if '%BASEDIR' in extra_params_in:
                    extra_params_in = extra_params_in.replace('%BASEDIR',
                                                    root_path)

                ep = extra_params_in.split(' ')
                in_path = ep
                tmp = []
                for val2 in in_path:
                    tmp.append(str(val2))
                in_path = tmp
                DBlogging.dblogger.debug("--**-- in_path=%s" % (in_path))

            exe = Executor.Executor(codep, in_path, out_path)
            exe.doIt()
            self.queue.append(out_path)
            self.importFromIncoming()  # we added something it is time to import it


            # we have all the info needed to add the links
            # filefilelink is out_path in_path
            self.dbu._addFilefilelink(self.dbu._getFileID(os.path.basename(val[0].diskfile.filename)),
                                      self.dbu._getFileID(os.path.basename(out_path)) )
            # TODO, think here if this is really ok to do
            try:
                self.dbu._addFilecodelink(self.dbu._getFileID(os.path.basename(out_path)),
                                          self.dbu._getCodeID(os.path.basename(codep)) )
            except DBUtils2.DBError:
                pass



    def findChildrenProducts(self):
        """
        from the queue self.findChildren go though and find all the products of
        the children

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 02-Dec-2010 (BAL)
        """
        # childern is a sub sub query
        # select * from product where product_id = (SELECT output_product from
        #   process where process_id
        #  = (select product_id from productprocesslink where product_id = 16));
        DBlogging.dblogger.debug( "Entered findChildrenProducts()" )

        for val in self.findChildren.popleftiter():
            DBlogging.dblogger.debug("popleftiter %s from self.findChildren" % \
                                     (str(val)))

            proc_ids = self.dbu.session.query(self.dbu.Productprocesslink.process_id).filter_by(input_product_id = val.diskfile.params['product_id'])

            for pval in proc_ids:
                sq2 = self.dbu.session.query(self.dbu.Process.output_product).filter_by(process_id = pval[0]).subquery()

                sq_ans = self.dbu.session.query(self.dbu.Product).filter_by(product_id = sq2)
                for val2 in sq_ans:
                    self.childrenQueue.append( (val, val2.product_id) )
                    DBlogging.dblogger.debug( "found products for children: %s: %s" % \
                                             (val, val2.product_id))


def processRunning(pid):
    """
    given a PID see if it is currently running

    @param pid: a pid
    @type pid: long

    @return: True if pid is running, False otherwise
    @rtype: bool

    @author: Brandon Craig Rhodes
    @organization: Stackoverflow
    http://stackoverflow.com/questions/568271/check-if-pid-is-not-in-use-in-python

    @version: V1: 02-Dec-2010 (BAL)
    """
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True





if __name__ == "__main__":
    # TODO decide if we really want to run this way, works for now

    pq = ProcessQueue('Test')
    # check currently processing
    curr_proc = pq.dbu._currentlyProcessing()
    if curr_proc:  # returns False or the PID
        # check if the PID is running
        if processRunning(curr_proc):
            # we still have an instance processing, dont start another
            pq.dbu._closeDB()
            DBlogging.dblogger.error( "There is a process running, can't start another: PID: %d" % (curr_proc))
            raise(ProcessException("There is a process running, can't start another: PID: %d" % (curr_proc)))
        else:
            # There is a processing flag set but it died, dont start another
            pq.dbu._closeDB()
            DBlogging.dblogger.error( "There is a processing flag set but it died, dont start another" )
            raise(ProcessException("There is a processing flag set but it died, dont start another"))
    # start logging as a lock
    pq.dbu._startLogging()


    pq.checkIncoming()
    while len(pq.queue) != 0 or len(pq.findChildren) !=0 or len(pq.childrenQueue):
        pq.importFromIncoming()
        pq.findChildrenProducts()
        pq.buildChildren()


    pq.dbu._stopLogging('Nominal Exit')
    pq.dbu._closeDB()
