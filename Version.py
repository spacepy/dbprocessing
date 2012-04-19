from __future__ import division
"""
Module to handle version information of files and codes.

Handles boolean operators (>, <, =, !=) and database interface for version is also implemented in another module

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

    Version objects can perform boolean operations

    >>> v2 = Version.Version(1,2,1)
    >>> print(v2 > v)
    True

    Incrementing the version

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
        self.__checkVersion()

    def __checkVersion(self):
        """
        check a version to make sure it is valid, works on current object
        """
        if not isinstance(self.interface, (int, long)):
            raise(VersionError("Versions are int or long"))
        if not isinstance(self.quality, (int, long)):
            raise(VersionError("Versions are int or long"))
        if not isinstance(self.revision, (int, long)):
            raise(VersionError("Versions are int or long"))
        if self.interface == 0:
            raise(VersionError("interface_version starts at 1"))

    def __repr__(self):
        return 'Version: ' + str(self.interface) + '.' + str(self.quality) + '.' + \
            str(self.revision)

    def __str__(self):
        return str(self.interface) + '.' + str(self.quality) + '.' + \
            str(self.revision)

    def incInterface(self):
        """increment the interface version"""
        self.interface += 1
        self.__checkVersion()

    def incQuality(self):
        """increment the quality version"""
        self.quality += 1
        self.__checkVersion()

    def incRevision(self):
        """increment the revision version"""
        self.revision += 1
        self.__checkVersion()

    def __eq__(self, other):
        """Same version numbers is equal"""
        if self.interface == other.interface:
            if self.quality == other.quality:
                if self.revision == other.revision:
                    return True
        return False

    def __gt__(self, other):
        if self.interface > other.interface:
            return True
        elif self.quality > other.quality:
            return True
        elif self.revision > other.revision:
            return True
        else:
            return False

    def __lt__(self, other):
        if self.interface < other.interface:
            return True
        elif self.quality < other.quality:
            return True
        elif self.revision < other.revision:
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
