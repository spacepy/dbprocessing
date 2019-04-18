#!/usr/bin/env python

"""Add a product process line to the dbprocessing database."""

import argparse

import dbprocessing.DButils as DButils


def parse_args(argv=None):
    """Parse command line arguments

    :param list argv: command line arguments, default sys.argv
    :returns: positional arguments and keyword arguments
    :rtype: tuple of list
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--prod', dest='product', action='store',
                        help='Product string', required=True)
    parser.add_argument('-c', '--proc', dest='process', action='store',
                        help='Process string', required=True)
    parser.add_argument('-n', '--name', dest='name', action='store',
                        help='Path to database', required=True)
    parser.add_argument('-o', '--opt', dest='optional', action='store_false',
                        help='Set to optional file. Default is true.', required=False)
    parser.add_argument('-y', '--yday', dest='yesterday', action='store_true',
                        help='Want yesterday. Default is false.', required=False)
    parser.add_argument('-t', '--tmrw', dest='tomorrow', action='store_true',
                        help='Want tomorrow. Default is false.', required=False)
    args = parser.parse_args(argv)
    kwargs = vars(args)
    return [], kwargs

if __name__ == '__main__':
    args, kwargs = parse_args()
    db=DButils.DButils(kwargs['name'])
    try:
        proc_id = db.getProcessID(kwargs['process'])
    except:
        raise ValueError('Could not find process in database: {}'.
                         format(kwargs['process']))
        
    try:
        prod_id = db.getProductID(kwargs['product'])
    except:
        raise ValueError('Could not find product in database: {}'.
                         format(kwargs['product']))
        
    if kwargs['yesterday']:
        yesterday = 1
    else:
        yesterday = 0
        
    if kwargs['tomorrow']:
        tomorrow = 1
    else:
        tomorrow = 0

    if proc_id in db.getProcessFromInputProduct(prod_id):
        print('Link between {} and {} already in database.'.\
              format(kwargs['process'], kwargs['process']))
    else:
        db.addproductprocesslink(prod_id, proc_id, kwargs['optional'], yesterday, tomorrow)
        
    db.commitDB()
    db.closeDB()
    
