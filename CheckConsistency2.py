

import sqlalchemy as sql
from Utils import Utils

class CheckConsistency(Utils):
    def __init__(self, mission='Test', level=0.0, echo=False, verbose=True):
        Utils.__init__(self,mission, level, echo, verbose)

        



    def doCheck(self):
        ## follow the flow chart
        # get a Lx file
        # gather its file dependenices
        # are the file depen up to date?
        # gather its code dependencies
        # are the code depen up to date?
        # if all yes we are done
        # otherwise do the process
        # and update the db
        for i, val in enumerate(self._fileID):
            if self.verbose: print("#### Processing fileID " + str(val))
            fdepenID = self._Utils__gatherFileDepen(val)
            if self.verbose:  
                print("     Its file dependencies are:")
                if fdepenID != None:
                    for tmp in fdepenID: 
                        print("         " + str(tmp)) 
                else:
                    print("          None" )
#            cdepenID = self.__gathCodeDepen(val)
            cdepenID = None
            if self.verbose:  
                print("     Its code dependencies are:")
                if cdepenID != None:
                    for tmp in cdepenID: 
                        print("         " + str(tmp)) 
                else:
                    print("          None (not implemented)" )
            fprocID = self._Utils__fileIDProcesses(val)
            if self.verbose:  
                print("     Its L+1 process is:")
                if fprocID != None:
                    for tmp in fprocID: 
                        print("          " + str(tmp)) 
                else:
                    print("          None" )
            
            



if __name__ == '__main__':
    c1=CheckConsistency()
    #c1.tables[c1.tables.keys()[2]]










