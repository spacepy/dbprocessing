# -*- coding: utf-8 -*-
"""
Class to hold random utilities of use throughout this code
"""

import collections

def flatten(l):
    """
    flatten an irregualrly nested list of lists
    thanks SO: http://stackoverflow.com/questions/2158395/flatten-an-irregular-list-of-lists-in-python
    """
    for el in l:
        if isinstance(el, collections.Iterable) and not isinstance(el, basestring):
            for sub in flatten(el):
                yield sub
        else:
            yield el

def toBool(value):
    if value in ['True', 'true', True, 1, 'Yes', 'yes']:
        return True
    else:
        return False

def toNone(value):
    if value in ['', 'None']:
        return None
    else:
        return value

