#!/usr/bin/env python

"""Product Process links to add for filled fields files for L2."""

import argparse
import pdb
import subprocess

import dbprocessing


def parse_args(argv=None):
    """Parse command line arguments

    :param list argv: command line arguments, default sys.argv
    :returns: positional arguments and keyword arguments
    :rtype: tuple of list
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--name', dest='name', action='store',
                        help='Path to database', required=True)
    args = parser.parse_args(argv)
    kwargs = vars(args)
    return [], kwargs


def build_command(dbname, inst, str1, str2):
    """Build command string based on strings needed for file names.
    Issues command.

    :param str dbname: path to database
    :param str inst: 'epihi' or 'epilo'
    :param str str1: first variable (eg. 'let1' for epihi, 'ic' for epilo)
    :param str str2: second varibale (eg. '3600' for epihi, '4b3' for epilo)
    """
    if inst == 'epihi':
        prod = 'psp_fields_l1-isois-{}-{}-rates{}_filled'.format(inst,str1,str2)
        proc = 'psp_isois-{}_l1-l2-{}-rates{}'.format(inst,str1,str2)
    else:
        prod = 'psp_fields_l1-isois-{}-{}_filled'.format(inst,str2)
        proc = 'psp_isois-{}_l1-l2-{}'.format(inst,str1)
        
    cmd_str = 'python addProductProcessLink.py -d {} -c {} -n {}'.\
              format(prod, proc, dbname)
    subprocess.call(cmd_str, shell=True)


if __name__ == '__main__':
    args, kwargs = parse_args()
    instruments  = ['epihi', 'epilo']
    modes        = ['ic','ie','pc','pe']
    # second-rates is it's own case
    rates        = ['let1','let2','het']
    times        = ['10','60','300','3600']
    # skip Basic Rates, Diagnostic Rates, PHScan and Eff.
    apids = {'ic': ['4b3','4b4','4b5','4b6','4b8'],
             'ie': ['4bb'],
             'pc': ['4bf','4c0','4c1'],
             'pe': ['4c4','4c5','4c6','4c7']}

    for inst in instruments:
        if inst == 'epihi':
            # first second rates
            rate = 'second'
            time = ''
            build_command(kwargs['name'], inst, rate, time)
            # now the rest
            for rate in rates:
                for time in times:
                    build_command(kwargs['name'], inst, rate, time)
        # now epilo
        else:
            for mode in modes:
                for apid in apids[mode]:
                    build_command(kwargs['name'], inst, mode, apid)
