# -*- coding: utf-8 -*-

""" 
inspector requirements:
    - one product per inspector file
    - must implement inspect(filename, **kwargs)
    - inspect() must return version instance or False (no other or exceptions allowed)
    
inspector suggestions:
    - please use positive tests not negative tests 
         - e.g. pass all positive tests to be True.
         - do not default to True
"""









#==============================================================================
# Helper routines
#==============================================================================
import datetime

def valid_YYYYMMDD(inval):
    """
    if inval is valid YYYYMMDD return True, False otherwise
    """
    try:
        ans = datetime.datetime.strptime(inval, "%Y%m%d")
    except ValueError:
        return False
    if isinstance(ans, datetime.datetime):
        return True




    
    