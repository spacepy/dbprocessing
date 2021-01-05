#!/usr/bin/env python


#==============================================================================
# INPUTS
#==============================================================================
# mission name
# satellite name
# product name
## prod name
## product rel path
## prod filename format
## prod level
# <- add the prod
# <- create the inst_prod link
from __future__ import print_function

import ConfigParser
import datetime
import sys

from dbprocessing import DButils
from dbprocessing.Utils import toBool, toNone

sections = ['base', 'product', 'inspector',]


def readconfig(config_filepath):
    """
    read in a config file and return a dictionary with the sections as keys and
    the options and values as dictionaries
    """
    # Create a ConfigParser object, to read the config file
    cfg = ConfigParser.SafeConfigParser()
    cfg.read(config_filepath)
    ## lest not use this since it is undocumented magic and might change with versions
    # return cfg._sections # this is an ordered dict of the contents of the conf file
    ans = {}
    for section in cfg.sections():
        ans[section] = {}
        for val in cfg.items(section):
            try:
                ans[section][val[0]] = float(val[1])
            except ValueError:
                ans[section][val[0]] = val[1]
            if ans[section][val[0]] == 'None':
                ans[section][val[0]] = None
            if ans[section][val[0]] == 'True':
                ans[section][val[0]] = True
            if ans[section][val[0]] == 'False':
                ans[section][val[0]] = False
            try:
                ans[section][val[0]] = datetime.datetime.strptime(ans[section][val[0]], '%Y-%m-%d').date()
            except (ValueError, TypeError):
                pass
    return ans

def _updateSections(conf):
    """
    go through each section and update the db is there is a change
    """
    dbu = DButils.DButils('rbsp') # TODO don't assume RBSP later
    dbu.openDB()
    dbu._createTableObjects()

    sections = ['mission', 'satellite', 'instrument', 'product',
                'instrumentproductlink', 'inspector']
    succ = 0
    for section in sections:
        # get the object for the section
        try:
            obj = dbu.session.query(dbu.__getattribute__(section.title())).get(conf[section][section + '_id'])
            succ += 1
        except KeyError: # happens for instrumentproductlink where there is not an id
            continue
        # get the attributes
        attrs = [v for v in dir(obj) if v[0] != '_']
        # check if everything is the same or not
        same = [conf[section][v1] == obj.__getattribute__(v1) for v1 in attrs]
        # print out what is different  DB --- file
        if sum(same) != len(same): # means they are different
            for i, v1 in enumerate(same):
                if not v1:
                    print('{0}[{1}]  {2} ==> {3}'.format(section, attrs[i], obj.__getattribute__(attrs[i]), conf[section][attrs[i]]))
                    obj.__setattr__(attrs[i], conf[section][attrs[i]])
                    dbu.session.add(obj)
            dbu.commitDB()
    if succ == 0:
        raise(ValueError('using {0} on a product that is not in the DB'.format(sys.argv[0])))


def usage():
    """
    print the usage message out
    """
    print("Usage: {0} <filename>".format(sys.argv[0]))
    print("   -> config file to update")
    return


if __name__ == "__main__":
    if len(sys.argv) != 2:
        usage()
        sys.exit(2)
    conf = readconfig(sys.argv[-1])
    _updateSections(conf)
