from __future__ import division
from __future__ import print_function

from functools import total_ordering


class VersionError(Exception):
    """Error class for Version that calls out that an invalid version has been specified"""
    pass

@total_ordering
class Version(object):
    """
    A version class to simplify pushing around version information
    Handles Boolean operators (>, <, =, !=) and database interface for version is also implemented in another module

    The version scheme is X,Y,Z where:
    * X is the interface version, incremented only when the interface to a file or code changes
    * Y is the quality version, incremented when a change is made to a file that affects quality
    * Z is the revision version, incremented when a revision has been done to a code or file, as minor as fixing a typo

    .. note:: The interface version starts at 1

    :attribute interface: The interface version for the object
    :attribute quality: The quality version for the object
    :attribute revision: The revision version for the object

    :Example:

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

    def __init__(self, interface_version, quality_version, revision_version):
        """
        Sets the class attributes

        :param interface_version: The interface version for the object
        :type interface_version: int
        :param quality_version: The quality version for the object
        :type quality_version: int
        :param revision_version: The revision version for the object
        :type revision_version: int
        """
        self.interface = int(interface_version)
        self.revision = int(revision_version)
        self.quality = int(quality_version)
        self._checkVersion()

    @staticmethod
    def fromString(inval):
        """
        Given a string of the form x.y.z return a Version object

        :param inval: String in the form xx.yy.zz
        :type inval: str
        :return: Version instance created from the string
        :rtype: :class:`Version`
        """
        return Version(*inval.split('.'))

    def _checkVersion(self):
        """
        Check a version to make sure it is valid, works on current object
        """
        if self.interface == 0:
            raise VersionError("interface_version starts at 1")

    def __repr__(self):
        return 'Version: ' + self.__str__()

    def __str__(self):
        return str(self.interface) + '.' + str(self.quality) + '.' + \
               str(self.revision)

    def __format__(self, *args, **kwargs):
        """Explicitly format as string"""
        return format(str(self), *args, **kwargs)

    def incInterface(self):
        """Increment the interface version and reset the other two"""
        self.interface += 1
        self.quality = 0
        self.revision = 0
        self._checkVersion()

    def incQuality(self):
        """Increment the quality version and reset the revision"""
        self.quality += 1
        self.revision = 0
        self._checkVersion()

    def incRevision(self):
        """Increment the revision version"""
        self.revision += 1
        self._checkVersion()

    def __eq__(self, other):
        return ((self.interface, self.quality, self.revision) == \
                (other.interface, other.quality, other.revision))

    def __ne__(self, other):
        return not (self == other)

    def __lt__(self, other):
        return ((self.interface, self.quality, self.revision) < \
                (other.interface, other.quality, other.revision))

    def __sub__(self, other):
        """
        Subtract works on each version number

        :param other: The other Version object
        :type other: Version
        """
        return [self.interface - other.interface, self.quality - other.quality, self.revision - other.revision]
