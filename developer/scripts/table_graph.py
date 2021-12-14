#!/usr/bin/env python

"""Create the diagram of the database layout

Using the tables module, prints to stdout a GraphViz dot file showing the
table relationships."""

import collections

import dbprocessing.tables
import sqlalchemy.schema
import sqlalchemy.types


tablestart = '''
"{tablename}" [
    shape=none
    label=<
        <table border="0" cellpadding="5" cellspacing="0" cellborder="1">
            <tr><td colspan="3" bgcolor="#E0B010" valign="BOTTOM"><font point-size="18" face="bold">{tablename}</font></td></tr>'''
tableend = '''
        </table>
>];'''
column = '            <tr><td align="center" port="{columnname}_in" sides="ltb"  bgcolor="#FFF0B0">{annotation}</td><td align="left" sides="rtb" bgcolor="#FFF0B0"><b>{columnname}</b></td><td bgcolor="#FFE080" align="left" port="{columnname}_out">{columntype}</td></tr>'''

if __name__ == '__main__':
    print('digraph g { graph [ rankdir = "LR" ];\nsplines=compound;')
    names = sorted(dbprocessing.tables.names)
    foreign_keys = collections.defaultdict(list)
    for n in names:
        print(tablestart.format(tablename=n))
        for c in dbprocessing.tables.definition(n):
            if not isinstance(c, sqlalchemy.schema.Column):
                continue
            print(column.format(
                annotation='&#9919;' if c.primary_key else '',
                columnname=c.name,
                columntype=repr(c.type).split('(')[0]))
            qualname = '{}.{}'.format(n, c.name)
            if c.primary_key:
                pass
            if not c.nullable:
                pass
            for fk in c.foreign_keys:
                foreign_keys[(n, c.name)].append(fk.target_fullname.split('.'))
        print(tableend)
    for s in sorted(foreign_keys):
        for t in sorted(foreign_keys[s]):
            print('{}:{}_out:e -> {}:{}_in:w'.format(s[0], s[1], t[0], t[1]))
    print('}')
