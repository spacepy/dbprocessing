class VersionError(Exception):
    pass



class Version(object):
    """
    A version class to simplify pushing around version  information all the time

    @author: Brian Larsen
    @organization: Los Alamos National Lab
    @contact: balarsen@lanl.gov

    @version: V1: 7-Jul-2010 (BAL)
    """


    def __init__(self,
            interface_version,
            quality_version,
            revision_version):
        """
        @param interface_version: the interface version
        @type interface_version: int
        @param quality_version: the quality version
        @type quality_version: int
        @param revision_version: the revision version
        @type revision_version: int
        """
        self.interface = interface_version
        self.revision = revision_version
        self.quality = quality_version
        self.__checkVersion()

    def __checkVersion(self):
        """
        check a version to make sure it is valid"""

        if not isinstance(self.interface, (int, long)):
            raise(VersionError("Versions are int or long"))
        if not isinstance(self.quality, (int, long)):
            raise(VersionError("Versions are int or long"))
        if not isinstance(self.revision, (int, long)):
            raise(VersionError("Versions are int or long"))
        if self.interface == 0:
            raise(VersionError("interface_version starts at 1"))


    def __repr__(self):
        return str(self.interface) + '.' + str(self.quality) + '.' + str(self.revision)

    __str__ = __repr__

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

