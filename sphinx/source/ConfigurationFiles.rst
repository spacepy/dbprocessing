Configuration Files
===================

Basics
------
The configuration file consists of sections, led by a [section] header and followed by
name: value entries, name=value is also accepted.

Note that leading whitespace is removed from values.
The optional values can contain format strings which refer to other values in the same section,
or values in a special DEFAULT section. Additional defaults can be provided on initialization
and retrieval. Lines beginning with '#' or ';' are ignored and may be used to provide comments.

Configuration files may include comments, prefixed by specific characters (# and ;).
Comments may appear on their own in an otherwise empty line, or may be entered in lines
holding values or section names. In the latter case, they need to be preceded by a whitespace
character to be recognized as a comment. (For backwards compatibility, only ; starts an inline
comment, while # does not.)

TODO
====
Add in an example and describe it




