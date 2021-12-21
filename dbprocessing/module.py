"""
Wrapper for environment modules script (http://modules.sourceforge.net).

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

        Parameters
        ----------
        args : :class:`list` of :class:`str`
            Arguments passed straight through to module
        
        Examples
        --------
        >>> mod = module('load', 'icy')
        """
        self.env = os.environ.copy()
        """Environment variables (:class:`dict`)"""
        command = "modulecmd python "+' '.join(args)
        p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
        retval = p.communicate()
        self._parse(retval)

    def _parse(self, inval):
        """
        Parse the module call and updates environment.

        Makes changes to :data:`env`.

        Parameters
        ----------
        inval : :class:`str`
            Output of module call
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
        Return a complete environment.

        Parameters
        ----------
        args : :class:`list` of :class:`str`
           Arguments to pass direction to module command.

        Returns
        -------
        :class:`dict`
            Environment variables, starting with current environment and
            modified by the module commands. Suitable as ``env`` argument
            in :func:`subprocess.call`.
        """
        m = module(*args)
        return m.env




