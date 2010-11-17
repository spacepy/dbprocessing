

import subprocess
import os
import os.path

import DBlogging


class ExecutorError(Exception):
    DBlogging.dblogger.warning("\t\tEntered ExecutorError:")
    pass

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
                 input,
                 output,
                 debug=False):
        """
        Initializer

        @param code: the executable code to run (use full path)
        @type code: str
        @param input: a list of input paramters to the code
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
        DBlogging.dblogger.debug("\t\tEntered Executor:")
        if not isinstance(code, (str, unicode)):
            raise(ExecutorError("Only one code can be executed, must be a string"))
        if not isinstance(output, (str, unicode)):
            raise(ExecutorError("Only one output can be created, must be a string"))
        if isinstance(input, (str, unicode)):
            input = [input]
        self.code = code
        self.input = input
        self.output = output
        self.debug = debug

    def checkExists(self):
        if not os.path.isfile(self.code):
            raise(ExecutorError("Code did not exist"))
        for val in input:
            if not os.path.isfile(val):
                raise(ExecutorError("input %s did not exist"% (val)))
        if not os.path.isdir(os.path.dirname(self.output)):
            raise(ExecutorError("Invalid path for output"% (val)))

    def doIt(self):
        cmd = []
        cmd.append(self.code)
        cmd.extend(self.input)
        cmd.append(self.output)
        DBlogging.dblogger.debug("\t\tExecuting: %s" % (cmd))
        subprocess.call(cmd)





