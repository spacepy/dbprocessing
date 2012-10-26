# -*- coding: utf-8 -*-
"""
Class to hold random utilities of use throughout this code
"""

import collections

def flatten(l):
    """
    flatten an irregularly nested list of lists
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

def strargs_to_args(strargs):
    """
    read in the arguments string from the db and change to a dict
    """
    if strargs is None:
        return None
    kwargs = {}
    if isinstance(strargs, (list, tuple)): # we have multiple to deal with
        # TODO why was this needed?
        if len(strargs) == 1:
            kwargs = strargs_to_args(strargs[0])
            return kwargs
        for val in strargs:
            tmp = strargs_to_args(val)
            for key in tmp:
                kwargs[key] = tmp[key]
        return kwargs
    try:
        for val in strargs.split():
            tmp = val.split('=')
            kwargs[tmp[0]] = tmp[1]
    except (AttributeError, KeyError): # it was None
        pass
    return kwargs