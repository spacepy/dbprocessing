

import subprocess
import os
import os.path

import DBlogging


class ExecutorError(Exception):
    DBlogging.dblogger.warning("\t\tEntered ExecutorError:")

class Executor(object):
    """
    Class that wraps subprocess.call() so that we can run more complicated command lines

    @author: Brian Larsen
    @organization: Los Alamos National Lab
    @contact: balarsen@lanl.gov

    @version: V1: 05-Oct-2010 (BAL)
    """
    def __init__(self,
                 code,
                 inVal,
                 output):
        """
        Initializer

        @param code: the executable code to run (use full path)
        @type code: str
        @param inVal: a list of input paramters to the code
        @type param: str
        @param output: output filename to pass to the code
        @type output: str

        @return: whatever is returned from the code (hopefully a return code)
        @rtype: long

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 05-Oct-2010 (BAL)
        """
        DBlogging.dblogger.info("Entered Executor:")
        if not isinstance(code, (str, unicode)):
            raise(ExecutorError("Only one code can be executed, must be a string"))
        if not isinstance(output, (str, unicode)) and  output!=None:
            raise(ExecutorError("Only one output can be created, must be a string"))
        if not isinstance(inVal, (list, tuple)) and  inVal!=None:
            inVal = [inVal]
        self.code = code
        self.inVal = inVal
        self.output = output

    def checkExists(self):
        if not os.path.isfile(self.code):
            raise(ExecutorError("Code did not exist"))
        if self.inVal != None:
            for val in self.inVal:
                if not os.path.isfile(val):
                    raise(ExecutorError("input %s did not exist"% (val)))
        if self.output != None:
            if not os.path.isdir(os.path.dirname(self.output)):
                raise(ExecutorError("Invalid path for output: %s" % (self.output)))

    def doIt(self):
        cmd = []
        cmd.append(self.code)
        if self.inVal != None:
            cmd.extend(self.inVal)
        if self.output != None:
            cmd.append(self.output)
        DBlogging.dblogger.info("Executing: %s" % (cmd))
        subprocess.call(cmd)





