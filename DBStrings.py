#!/usr/bin/env python2.6

"""String handling for dbprocessing

Contains functions/objects useful in handling strings related to
dbprocessing: parsing, formatting, etc.
"""

__author__ = 'Jonathan Niehof <jniehof@lanl.gov>'
__version__ = '0.0'

import string


class _UnfoundField(object):
    """Information on a field specified in format string, but not in arguments

    @ivar key: key which was requested, but not found
    @type key: hashable
    @ivar lookups: sequence of lookups attempted on the not-found object
    @type lookups: str
    @ivar conversion: requested conversion on final value
    @type conversion: str
    """

    def __init__(self, key):
        """Initialize

        @param key: the key which was requested but not found
        @type key: hashable
        """
        self.key = key
        self.lookups = ''
        self.conversion = None

    def __getattr__(self, key):
        """Record an attempted attribute lookup on the not-found object

        Stores information on the lookup for later reconstruction.
        """
        self.lookups += ('.' + key)
        return self

    def __getitem__(self, key):
        """Record an attempted item lookup on the not-found object

        Stores information on the lookup for later reconstruction.
        """
        self.lookups += ('[' + repr(key) + ']')
        return self

    def field_spec(self, format_spec):
        """Reproduce the complete field spec that was requested for this item

        @param format_spec: format specification for this field
        @type format_spec: str
        """
        fs = '{' + self.key + self.lookups
        if self.conversion is not None:
            fs += ('!' + self.conversion)
        if format_spec:
            fs += (':' + format_spec)
        fs += '}'
        return fs

    
class DBFormatter(string.Formatter):
    """String formatter extended/modified for DBUtils"""

    def get_field(self, field_name, args, kwargs):
        """Find object referenced by a field_name

        If field_name is not found, return an L{_UnknownField} describing
        the field.
        """
        obj, first = \
             super(DBFormatter, self).get_field(field_name, args, kwargs)
        if isinstance(obj, _UnfoundField):
            return obj, None
        else:
            return obj, first

    def get_value(self, key, *args, **kwargs):
        """Gets a field value

        Extends base behaviour by returning information on field lookups
        which don't exist, rather than throwing an exception.
        """
        try:
            return super(DBFormatter, self).get_value(key, *args, **kwargs)
        except KeyError:
            return _UnfoundField(key)

    def convert_field(self, value, conversion):
        """Applies a str/repr conversion on result from get_value

        Extends base: if L{value} indicates the flag was not found,
        record the requested conversion and perform no additional changes.
        """
        if isinstance(value, _UnfoundField):
            value.conversion = conversion
            return value
        else:
            return super(DBFormatter, self).convert_field(value, conversion)

    def format_field(self, value, format_spec):
        """Creates final formatted field

        Extends base: if field was not found, reproduce the original field spec
        """
        if isinstance(value, _UnfoundField):
            return value.field_spec(format_spec)
        else:
            return super(DBFormatter, self).format_field(value, format_spec)
