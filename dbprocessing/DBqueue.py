"""General deque implementation, used for processing queue."""

from __future__ import print_function

from collections import deque


class DBqueue(deque):
    """General deque

    Extension to :class:`~collections.deque` to add methods
    :meth:`popiter` and :meth:`popleftiter`.
    """
    def popleftiter(self):
        """
        Allow a for loop to iterate and pop items from the DBqueue

        Yields
        ------
        any
            Leftmost (0th) item in queue.

        Examples
        --------
        >>> a = DBqueue([1,2,3])
        >>>  for i in a.popleftiter():
        ...:     print(i)
        1
        2
        3
        """
        while len(self) != 0:
            yield self.popleft()

    def popiter(self):
        """
        Allow a for loop to iterate and pop items from the DBqueue

        Yields
        ------
        any
            Rightmost (last) item in queue.

        Examples
        --------
        >>> a = DBqueue([1,2,3])
        >>>  for i in a.poptiter():
        ...:     print(i)
        3
        2
        1
        """
        while len(self) != 0:
            yield self.pop()
