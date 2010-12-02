
from collections import deque


__version__ = '2.0.2'


class DBqueue(deque):

    def popleftiter(self):
        """
        Allow a for loop to iterate and pop items from the DBqueue

        >>> a = DBqueue([1,2,3])
        >>>  for i in a.popleftiter():
        ...:     print(i)
        1
        2
        3

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 6-Oct-2010 (BAL)
        """
        while len(self) != 0:
            yield self.popleft()



    def popiter(self):
        """
        Allow a for loop to iterate and pop items from the DBqueue

        >>> a = DBqueue([1,2,3])
        >>>  for i in a.poptiter():
        ...:     print(i)
        3
        2
        1

        @author: Brian Larsen
        @organization: Los Alamos National Lab
        @contact: balarsen@lanl.gov

        @version: V1: 6-Oct-2010 (BAL)
        """
        while len(self) != 0:
            yield self.pop()

