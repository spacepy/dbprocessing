# -*- coding: utf-8 -*-
"""
Created on Tue Apr 17 14:10:27 2012

@author: balarsen
"""
import imp # fix thisafter an install
import os

inspector = imp.load_source("inspector", '/Users/balarsen/svnhome/RBSPSOC/dbprocessing/inspectors/inspector.py')
import Version

def inspect(filename, **kwargs):
    """
    look for a filename that matches this product
    and example is "Test-Test_R0_evinst_20020602_v1.0.0.cdf"
    """
    fname = os.path.basename(filename)
    # {MISSION}-{SPACECRAFT}_{PRODUCT}_{Y}{m}{d}_v{VERSION}.cdf
    if fname[0:19] != 'Test-Test_R0_evinst':
        return False
    if not inspector.valid_YYYYMMDD(fname[20:28]):
        return False
    if fname.split(os.extsep)[-1] != 'cdf':
        return False
    ver = fname.split('v')[-1]
    try:
        version = Version.Version(ver.split('.')[0], ver.split('.')[1], ver.split('.')[2])
    except:
        return False
    return version
        
