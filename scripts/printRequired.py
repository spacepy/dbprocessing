#!/usr/bin/env python
"""Given one or more process names, print product IDs of required inputs"""

import argparse

import dbprocessing.DButils


class PrintRequired(object):
    """Get required input products for a process"""

    def main(self):
        self.get_args()
        self.dbu = dbprocessing.DButils.DButils(self.mission)
        self.required = {}
        try:
            for pro in self.processes:
                self.required[pro] = self.get_required(pro)
        finally:
            self.dbu.closeDB()
        self.print_info()

    def get_args(self):
        """Parse arguments"""
        parser = argparse.ArgumentParser(
            description="Print required input products")
        parser.add_argument('-m', '--mission', dest='mission', action='store',
                            help='Mission file (sqlite)', required=True)
        parser.add_argument('processes', nargs='+', action='store',
                            metavar='process', help='Process names or IDs')
        args = parser.parse_args()
        self.mission = args.mission
        self.processes = args.processes

    def get_required(self, process):
        """Get required input product ID and name by process"""
        pid = self.dbu.getProcessID(process)
        return [(input_prod, self.dbu.session.query(self.dbu.Product)
                 .get(input_prod).product_name)
                for input_prod, optional in self.dbu.getInputProductID(pid)
                if not optional
        ]
        return [(input_prod, dbu.session.query(dbu.Product)
                 .get(input_prod).product_name)
                for input_prod, optional in dbu.getInputProductID(pid)
                if not optional
        ]

    def print_info(self):
        for k in sorted(list(self.required.keys())):
            print(k)
            for pid, name in sorted(self.required[k]):
                print('\t{:5d} {}'.format(pid, name))
        print('')
        print(' '.join([str(p) for p in sorted(list(set([
            pid for req in self.required.values() for pid, name in req])))]))


if __name__ == '__main__':
    PrintRequired().main()
