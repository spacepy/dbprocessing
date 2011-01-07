#!/usr/bin/env python2.6

"""String handling for dbprocessing

Contains functions/objects useful in handling strings related to
dbprocessing: parsing, formatting, etc.
"""

__author__ = 'Jonathan Niehof <jniehof@lanl.gov>'
__version__ = '0.0'

import string


class AssemblingFormatter(string.Formatter):
    """String formatter extended with a method to reassemble field specs"""

    @staticmethod
    def assemble(literal, field, format, conversion):
        """Assembles components of a field specification

        Converse of parse. Takes literal text, field name, format spec,
        and conversion and assembles into a full field spec.

        @param literal: any literal text preceding the field definition
        @type literal: str
        @param field: name of the field
        @type field: str
        @param format: format specification to apply to L{field}
        @type format: str
        @param conversion: conversion to apply to L{field}
        @type conversion: str
        @return: a full format spec that will parse into L{literal},
                 L{field}, L{format}, L{conversion}
        @rtype: str
        """
        fs = literal + '{' + field
        if conversion:
            fs += ('!' + conversion)
        if format:
            if literal or field or conversion:
                fs += ':'
            fs += format
        fs += '}'
        return fs


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
        return QuietFormatter.assemble('', self.key + self.lookups,
                                       format_spec, self.conversion)


class QuietFormatter(AssemblingFormatter):
    """String formatter extended to silently leave unfilled fields alone.

    Normal formatter throws an exception if a field cannot be filled from
    the provided argument; this one simply retains the original field spec.
    """
    #Follow four methods extend base formatter class
    def get_field(self, field_name, args, kwargs):
        """Find object referenced by a field_name

        If field_name is not found, return an L{_UnknownField} describing
        the field.
        """
        obj, first = \
             super(QuietFormatter, self).get_field(field_name, args, kwargs)
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
            return super(QuietFormatter, self).get_value(key, *args, **kwargs)
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
            return super(QuietFormatter, self).convert_field(value, conversion)

    def format_field(self, value, format_spec):
        """Creates final formatted field

        Extends base: if field was not found, reproduce the original field spec
        """
        if isinstance(value, _UnfoundField):
            return value.field_spec(format_spec)
        else:
            return super(QuietFormatter, self).format_field(value, format_spec)


class DBFormatter(QuietFormatter):
    """String formatter extended/modified for DBUtils

    @cvar SPECIAL_FIELDS: indexed by field name; each element contains
                          a fully-formatted representation of the field
                          and a regular expression that should match it.
    @type SPECIAL_FIELDS: dict
    """
    SPECIAL_FIELDS = {
        'Y': ('{Y:04d}', '(19|2\d)\d\d'),
        'm': ('{m:02d}', '(0\d|1[0-2])'),
        'd': ('{d:02d}', '[0-3]\d'),
        'y': ('{y:02d}', '\d\d'),
        'j': ('{j:03d}', '[0-3]\d\d'),
        'H': ('{H:02d}', '[0-2]\d'),
        'M': ('{M:02d}', '[0-6]\d'),
        'MILLI': ('{MILLI:03d}', '\d{3}'),
        'MICRO': ('{MICRO:03d}', '\d{3}'),
        'QACODE': ('{QACODE}', '(ok|ignore|problem)'),
        }

    #Following methods provide new functionality for this class
    def expand_format(self, format_string):
        """Adds formatting codes to 'special' fields in format string

        For every field defined in L{SPECIAL_FIELDS}, if there is no format
        spec nor conversion specified, replace it on the output with the
        full format spec in L{SPECIAL_FIELDS}. Everything else is returned
        verbatim.

        @param format_string: the format string to convert
        @type format_string: str
        @return: L{format_string} with the fields defined in L{SPECIAL_FIELDS}
                 expanded to full format specifiers.
        @rtype: str
        """

        result = []
        for literal, field, format, conversion in self.parse(format_string):
            if field in self.SPECIAL_FIELDS and (not format) and (not conversion):
                result.append(literal)
                result.append(self.SPECIAL_FIELDS[field][0])
            else:
                result.append(self.assemble(literal, field, format, conversion))
        return ''.join(result)

    def regex(self, format_string):
        """Convert 'special' fields in format string to regex

        For every field defined in L{SPECIAL_FIELDS}, if there is no
        forma spec/conversion specified OR it matches that in
        L{SPECIAL_FIELDS}, replace with the regular expression from
        L{SPECIAL_FIELDS}. Everything else returned verbatim.

  
        @param format_string: the format string to convert
        @type format_string: str
        @return: L{format_string} with the fields defined in L{SPECIAL_FIELDS}
                 replaced with matching regular expressions
        @rtype: str
        """

        result = []
        for literal, field, format, conversion in self.parse(format_string):
            if field in self.SPECIAL_FIELDS:
                result.append(literal)
                orig = self.assemble('', field, format, conversion)
                if (not format and not conversion) \
                   or self.SPECIAL_FIELDS[field][0] == orig:
                    result.append(self.SPECIAL_FIELDS[field][1])
                else:
                    result.append(orig)
            else:
                result.append(self.assemble(literal, field, format, conversion))
        return ''.join(result)      
