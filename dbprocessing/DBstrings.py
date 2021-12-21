#!/usr/bin/env python
"""String handling for dbprocessing.

Contains functions/objects useful in handling strings related to
dbprocessing: parsing, formatting, etc.
"""

from __future__ import print_function

__author__ = 'Jonathan Niehof <jniehof@lanl.gov>'

import string


class DBformatter(string.Formatter):
    """String formatter extended/modified for DButils

    Notes
    -----
    As this is currently implemented, :meth:`re` may not handle
    ``{{`` and ``}}`` properly, since regex expansion and basic formatting
    occur in two different steps, and thus ``{{`` and ``}}`` are already
    replaced with ``{`` and ``}`` in the second step.
    """

    SPECIAL_FIELDS = {
        'Y': ('{Y:04d}', '(19|2\d)\d\d'),
        'm': ('{m:02d}', '(0\d|1[0-2])'),
        'b': ('{b}', 'Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec'),
        'd': ('{d:02d}', '[0-3]\d'),
        'y': ('{y:02d}', '\d\d'),
        'j': ('{j:03d}', '[0-3]\d\d'),
        'H': ('{H:02d}', '[0-2]\d'),
        'M': ('{M:02d}', '[0-6]\d'),
        'S': ('{S:02d}', '[0-6]\d'),
        'MILLI': ('{MILLI:03d}', '\d{3}'),
        'MICRO': ('{MICRO:03d}', '\d{3}'),
        'QACODE': ('{QACODE}', 'ok|ignore|problem'),
        'VERSION': ('{VERSION}', '\d+\.\d+\.\d+'),
        'DATE': ('{DATE}', '(19|2\d)\d\d(0\d|1[0-2])[0-3]\d'),
        'datetime': ('{datetime}', '(19|2\d)\d\d(0\d|1[0-2])[0-3]\d'),
        'mday': ('{mday:d}', '-?\d+'),
        'APID': ('{APID:x}', '[\da-fA-F]+'),
        '??': ('{??}', '..'),
        '???': ('{???}', '...'),
        '????': ('{????}', '....'),
        'nn': ('{nn}', '\d\d'),
        'nnn': ('{nnn}', '\d\d\d'),
        'nnnn': ('{nnnn}', '\d\d\d\d'),
    }
    """indexed by field name; each element contains a fully-formatted
       representation of the field and a regular expression that should match
       it. (:class:`dict`)"""

    def format(self, format_string, *args, **kwargs):
        """Expand base format to handle datetime and special dbp keywords

        This is the top-level function to call.

        Parameters
        ----------
        format_string : :class:`str`
            String with format specifiers.

        Returns
        -------
        :class:`str`
            The formatted string.
        """
        self.expand_datetime(kwargs)
        return super(DBformatter, self).format(
            self.expand_format(format_string), *args, **kwargs)

    def re(self, format_string, *args, **kwargs):
        """Format with regexp for unspecified fields.

        Similar to :meth:`format`, but any fields which are not specified
        are replaced wth regular expressions according to
        :data:`SPECIAL_FIELDS`.

        Parameters
        ----------
        format_string : :class:`str`
            String with format specifiers.

        Returns
        -------
        :class:`str`
            The formatted string.
        """
        self.expand_datetime(kwargs)
        return super(DBformatter, self).format(
            self.expand_format(format_string, kwargs), *args, **kwargs)

    def expand_datetime(self, kwargs):
        """Expands datetime keyword into special keywords. Helper function!

        A single datetime keyword may be provided to :meth:`format`; this
        function expands that datetime keyword into all the fields that
        may be provided by the datetime object and inserts those keywords
        into ``kwargs``.

        Parameters
        ----------
        kwargs : :class:`dict`
            keywords passed to :meth:`format`. Updated in place.

        Examples
        --------
        >>> kwargs = { 'datetime': datetime.datetime(2010, 1, 1) }
        >>> dbprocessing.DBstrings.DBformatter().expand_datetime(kwargs)
        >>> kwargs
        {'DATE': '20100101',
         'H': 0,
         'M': 0,
         'MICRO': 0,
         'MILLI': 0,
         'S': 0,
         'Y': 2010,
         'b': 'Jan',
         'd': 1,
         'datetime': datetime.datetime(2010, 1, 1, 0, 0),
         'j': 1,
         'm': 1,
         'y': 10}
        """
        if 'datetime' in kwargs:
            dt = kwargs['datetime']
            if hasattr(dt, 'year'):
                if not 'Y' in kwargs:
                    kwargs['Y'] = dt.year
                if not 'm' in kwargs:
                    kwargs['m'] = dt.month
                if not 'd' in kwargs:
                    kwargs['d'] = dt.day
                if not 'y' in kwargs:
                    kwargs['y'] = dt.year % 100
                if not 'j' in kwargs:
                    kwargs['j'] = int(dt.strftime('%j'))
                if not 'DATE' in kwargs:
                    kwargs['DATE'] = dt.strftime('%Y%m%d')
                if not 'b' in kwargs:
                    kwargs['b'] = dt.strftime('%b')
            if hasattr(dt, 'hour'):
                if not 'H' in kwargs:
                    kwargs['H'] = dt.hour
                if not 'M' in kwargs:
                    kwargs['M'] = dt.minute
                if not 'S' in kwargs:
                    kwargs['S'] = dt.second
                if not 'MILLI' in kwargs:
                    kwargs['MILLI'] = int(dt.microsecond / 1000)
                if not 'MICRO' in kwargs:
                    kwargs['MICRO'] = dt.microsecond % 1000

    def expand_format(self, format_string, kwargs=None):
        """Add formatting codes to 'special' fields in format string.

        Helper function!

        For every field defined in :data:`SPECIAL_FIELDS`, if there is no
        format spec nor conversion specified, replace it on the output
        with the full format spec in :data:`SPECIAL_FIELDS`.

        If the format spec/conversion is not provided or matches that in
        :data:`SPECIAL_FIELDS`, and the field is not found in ``kwargs``,
        replace with the regular expression from :data:`SPECIAL_FIELDS`.

        Everything else is returned verbatim.

        Parameters
        ----------
        format_string : :class:`str`
            The format string to convert.
        kwargs : :class:`dict`
            Provided keywords to check for existence. If not supplied, do
            no regex substitution.

        Returns
        -------
        :class:`str`
            ``format_string`` with the fields defined in :data:`SPECIAL_FIELDS`
            expanded to full format specifiers and replaced by regular
            expressions, as desired.
        """
        result = []
        for literal, field, format, conversion in self.parse(format_string):
            result.append(literal)
            orig = self.assemble('', field, format, conversion)
            if field in self.SPECIAL_FIELDS:
                if kwargs == None or field in kwargs:
                    # assume field is provided
                    if (not format) and (not conversion):
                        result.append(self.SPECIAL_FIELDS[field][0])
                    else:
                        result.append(orig)
                else:
                    # field not provided, put in regex instead
                    if (not format and not conversion) \
                            or self.SPECIAL_FIELDS[field][0] == orig:
                        new_re = '(' + self.SPECIAL_FIELDS[field][1].replace(
                            '{', '{{').replace('}', '}}') + ')'
                        result.append(new_re)
                    else:
                        result.append(orig)
            else:
                result.append(orig)
        return ''.join(result)

    def assemble(self, literal, field, format, conversion):
        """Assembles components of a field specification

        Converse of parse. Takes literal text, field name, format spec,
        and conversion and assembles into a full field spec.

        Parameters
        ----------
        literal : :class:`str`
            any literal text preceding the field definition
        field : :class:`str`
            name of the field
        format : :class:`str`
            format specification to apply to ``field``
        conversion : :class:`str`
            conversion to apply to ``field``

        Returns
        -------
        :class:`str`
            A full format spec that will parse into ``literal``, ``field``,
            ``format``, ``conversion``

        Examples
        --------
        >>> f = dbprocessing.DBstrings.DBformatter()
        >>> parsed = list(f.parse('The year is {Y}'))
        >>> parsed[0]
        ('The year is ', 'Y', '', None)
        >>> f.assemble(*parsed[0])
        'The year is {Y}'
        """
        if not field and not conversion and not format:
            return literal
        fs = literal + '{'
        if field:
            fs += field
        if conversion:
            fs += ('!' + conversion)
        if format:
            if literal or field or conversion:
                fs += ':'
            fs += format
        fs += '}'
        return fs
