#!/usr/bin/env python2.6

"""String handling for dbprocessing

Contains functions/objects useful in handling strings related to
dbprocessing: parsing, formatting, etc.
"""

__author__ = 'Jonathan Niehof <jniehof@lanl.gov>'
__version__ = '0.0'

import string


class DBFormatter(string.Formatter):
    """String formatter extended/modified for DBUtils"""

    def get_value(self, key, *args, **kwargs):
        """Gets a field value

        Extends base behaviour by silently ignoring parameters in format
        string with no match in the arguments.
        """
        try:
            return super(DBFormatter, self).get_value(key, *args, **kwargs)
        except KeyError:
            return '{' + key + '}'
