"""
class is meant as a wrapper for the modules script (http://modules.sourceforge.net)
as used on the LANL scheme
"""

import os
import re
import subprocess

class module(object):
    def __init__(self, *args):
        """
        commands are entered as args to this class then parsed

        Parameters
        ==========
        args : arguments
            arguments pass straight through to module

        Examples
        ========
        mod = module('load', 'icy')
        """
        self.env = os.environ.copy()
        command = "modulecmd python "+' '.join(args)
        p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
        retval = p.communicate()
        self._parse(retval)

    def _parse(self, inval):
        """
        parse the module call and return an environment
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
        return a complete envrionment suitable for passing through to
        subprocess.call()

        Parameters
        ==========
        args : arguments
            arguments pass straight through to module

        Returns
        =======
        dictionary containg the request environment
        """
        m = module(*args)
        return m.env




