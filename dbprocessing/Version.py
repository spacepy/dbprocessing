from __future__ import division
"""
Module to handle version information of files and codes.

Handles Boolean operators (>, <, =, !=) and database interface for version is also implemented in another module

The version scheme is X,Y,Z where:
* X is the interface version, incremented only when the interface to a file or code changes
* Y is the quality version, incremented when a change is made to a file that affects quality
* Z is the revision version, incremented when a revision has been done to a code or file, as minor as fixing a typo

Note
====
The interface version starts at 1

"""

__version__ = '2.0.3'


class VersionError(Exception):
    """Error class for Version that calls out that an invalid version has been specified"""
    pass

class Version(object):
    """
    A version class to simplify pushing around version information

    `Author:` Brian Larsen, LANL

    Parameters
    ==========
    interface_version : int
        the interface version for the object
    quality_version : int
        the quality version for the object
    revision_version : int
        the revision version of the object

    Attributes
    ==========
    interface : int
        interface version of the object
    quality : int
        quality version of the object
    revision : int
        revision version of the object


    Examples
    ========
    >>> import Version
    >>> v = Version.Version(1,1,1)
    >>> print(v)
    1.1.1

    Version objects can perform Boolean operations

    >>> v2 = Version.Version(1,2,1)
    >>> print(v2 > v)
    True

    Increment the version

    >>> v.incQuality()
    >>> print(v)
    1.2.1

    Same version is equal

    >>> v == v2
    True
    """
    def __init__(self,
            interface_version,
            quality_version,
            revision_version):
        self.interface = int(interface_version)
        self.revision = int(revision_version)
        self.quality = int(quality_version)
        self._checkVersion()

    @staticmethod
    def fromString(inval):
        """
        given a string of the form x.y.z return a Version object
        """
        return Version(*inval.split('.'))

    def _checkVersion(self):
        """
        check a version to make sure it is valid, works on current object
        """
        if self.interface == 0:
            raise(VersionError("interface_version starts at 1"))

    def __repr__(self):
        return 'Version: ' + str(self.interface) + '.' + str(self.quality) + '.' + \
            str(self.revision)

    def __str__(self):
        return str(self.interface) + '.' + str(self.quality) + '.' + \
            str(self.revision)

    def incInterface(self):
        """increment the interface version and reset the other two"""
        self.interface += 1
        self.quality = 0
        self.revision = 0
        self._checkVersion()

    def incQuality(self):
        """increment the quality version and reset the revision"""
        self.quality += 1
        self.revision = 0
        self._checkVersion()

    def incRevision(self):
        """increment the revision version"""
        self.revision += 1
        self._checkVersion()

    def __eq__(self, other):
        """Same version numbers is equal"""
        if self.interface == other.interface:
            if self.quality == other.quality:
                if self.revision == other.revision:
                    return True
        return False

    def __gt__(self, other):
        val_s = 10000*self.interface + 100*self.quality + self.revision
        val_o = 10000*other.interface + 100*other.quality + other.revision
        if val_s > val_o:
            return True
        else:
            return False

    def __lt__(self, other):
        val_s = 10000*self.interface + 100*self.quality + self.revision
        val_o = 10000*other.interface + 100*other.quality + other.revision
        if val_s < val_o:
            return True
        else:
            return False

    def __ne__(self, other):
        if self.interface != other.interface:
            return True
        elif self.quality != other.quality:
            return True
        elif self.revision != other.revision:
            return True
        else:
            return False

    def __sub__(self, other):
        return [self.interface - other.interface, self.quality - other.quality, self.revision - other.revision]

    def __add__(self, other):
        return [self.interface + other.interface, self.quality + other.quality, self.revision + other.revision]



