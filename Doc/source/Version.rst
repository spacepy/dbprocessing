Version
=======
Module to handle version information of files and codes.

Handles boolean operators (>, <, =, !=) and database interface for version is also implemented in another module

The version scheme is X,Y,Z where:
* X is the interface version, incremented only when the interface to a file or code changes
* Y is the quality version, incremented when a change is made to a file that affects quality
* Z is the revision version, incremented when a revision has been done to a code or file, as minor as fixing a typo

`Note:` The interface version starts at 1


Classes
-------
.. automodule:: Version
    :members:
