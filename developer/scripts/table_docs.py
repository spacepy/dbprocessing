#!/usr/bin/env python

"""Create the skeleton of the rst documentation for tables

Using the tables module, prints to stdout the table names, column names,
and SQL types for each column."""

import dbprocessing.tables
import sqlalchemy.schema
import sqlalchemy.types


if __name__ == '__main__':
    names = sorted(dbprocessing.tables.names)
    namelen = max([len(n) for n in names])
    namecolumnlen = namelen + len(':sql:table:``')
    overline = '=' * namecolumnlen + ' ' + '=' * (79 - namecolumnlen)
    print(overline)
    for n in names:
        print(':sql:table:`{name}`{spaces} {name} summary'.format(
            spaces = ' ' * (namelen - len(n)), name=n))
    print(overline)
    for n in names:
        print('\n.. sql:table:: {}'.format(n))
        for c in dbprocessing.tables.definition(n):
            if not isinstance(c, sqlalchemy.schema.Column):
                continue
            print('\n.. sql:column:: {}\n'.format(c.name))
            info = [':py:class:`~sqlalchemy.types.{}`'.format(
                repr(c.type).split('(')[0])]
            if c.primary_key:
                info.append(
                    ':py:class:`PK <sqlalchemy.schema.PrimaryKeyConstraint>`')
            if not c.nullable:
                # This doesn't actually link, but does render the way I want
                # and provides a hint in the source at least...
                info.append(':py:obj:`NOT NULL <sqlalchemy.schema.Column'
                            '.params.nullable>`')
            for fk in c.foreign_keys:
                info.append(':py:class:`FK <sqlalchemy.schema'
                            '.ForeignKeyConstraint>`\n   :sql:column:`{}`'
                            .format(fk.target_fullname))
            print('   ({})'.format(',\n   '.join(info)))

