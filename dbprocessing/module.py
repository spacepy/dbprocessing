"""
Wrapper for environment modules script (http://modules.sourceforge.net)

as used on the LANL scheme
"""
from __future__ import print_function

import os
import re
import subprocess

class module(object):
    """Support for using/loading environment modules"""

    def __init__(self, *args):
        """
        Commands are entered as args to this class then parsed

        :keyword args : Arguments passed straight through to module
        
        :examples:

        mod = module('load', 'icy')
        """
        self.env = os.environ.copy()
        command = "modulecmd python "+' '.join(args)
        p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
        retval = p.communicate()
        self._parse(retval)

    def _parse(self, inval):
        """
        Parse the module call and return an environment
        """
        regex = re.compile(r'^os\.environ\[(.*)\]$')
        for val in inval:
            if val is None:
                continue
            # split on \n
            cmd = val.split('\n')
            for v2 in cmd:
                if not v2:
                    continue
                dict_call, pth = v2.split(' = ')
                m = re.match(regex, dict_call)
                if m:
                    key = m.groups()[0]
                    self.env[key] = pth

    @classmethod
    def get_env(self, *args):
        """
        Return a complete environment suitable for using in subprocess.call()

        :keyward args : Arguments passed straight through to module

        :return:dictionary containg the request environment
        :rtype: dict
        """
        m = module(*args)
        return m.env




