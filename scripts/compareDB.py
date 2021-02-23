#!/usr/bin/env python

"""Compare two databases for similarities in products, files, etc."""

import argparse
import collections

import dbprocessing.DButils


def main(mission1, mission2):
    """Compare contents of two missions

    Will open both missions and check if have same products, processes,
    codes, and files. Output printed to screen. Comparison is based on
    names, not IDs.

    Parameters
    ----------
    mission1 : str
        Name of first mission, e.g. database name or path to sqlite database
    mission2 : str
        Name of second mission
    """
    dbu1, dbu2 = None, None
    try:
        dbu1 = dbprocessing.DButils.DButils(mission1)
        dbu2 = dbprocessing.DButils.DButils(mission2)
        for t in ('product', 'process', 'code', 'file',):
            res = check_tables(t, dbu1, dbu2)
            if res:
                print('\n'.join(res))
        res = check_links(dbu1, dbu2)
        if res:
            print('\n'.join(res))
    finally:
        if dbu1 is not None:
            dbu1.closeDB()
        if dbu2 is not None:
            dbu2.closeDB()


def check_tables(table, dbu1, dbu2):
    """Compare tables present in each database

    Parameters
    ----------
    table : {'code', 'file', 'process', 'product'}
        table name
    dbu1, dbu2 : dbprocessing.DButils.DButils
        database utils instances

    Returns
    -------
    list
        Any discrepancies found between databases (empty list if none)
    """
    out = []
    table = table.lower()
    assert table in ('code', 'file', 'process', 'product')
    nameattr = '{}_name'.format(table)
    # Get all records for this table.
    gettername = 'getAll{}{}s'.format(table.title(),
                                      'e' if table.endswith('s') else '')
    if table == 'file': # There is no getAllFiles
        entries1 = [dbu1.getEntry('File', i) for i in dbu1.getAllFileIds()]
        entries2 = [dbu2.getEntry('File', i) for i in dbu2.getAllFileIds()]
    else:
        entries1 = getattr(dbu1, gettername)()
        entries2 = getattr(dbu2, gettername)()
    # E.g. getAllCodes returns dicts, of which only one part is the code record
    # So recover just that part
    if entries1 and isinstance(entries1[0], collections.Mapping):
        entries1 = [e[table] for e in entries1]
        entries2 = [e[table] for e in entries2]
    # Codes also have a description not a name, and files have no _
    for name_suffix in ('_name', '_description', 'name'):
        nameattr = '{}{}'.format(table, name_suffix)
        if entries1 and hasattr(entries1[0], nameattr):
            break
    # Code description is often not unique, so make something
    # that might be
    if table == 'code':
        entries1 = { '{} {} {}'.format(
            p.code_description, p.filename, p.arguments): p for p in entries1 }
        entries2 = { '{} {} {}'.format(
            p.code_description, p.filename, p.arguments): p for p in entries2 }
    else:
        entries1 = { getattr(p, nameattr): p for p in entries1 }
        entries2 = { getattr(p, nameattr): p for p in entries2 }
    # Check what's in only one database
    names1 = set(entries1.keys())
    names2 = set(entries2.keys())
    for n in sorted(names1.difference(names2)):
        out.append('{} {} is in mission1 but not mission2.'.format(
            table.title(), n))
    for n in sorted(names2.difference(names1)):
        out.append('{} {} is in mission2 but not mission1.'.format(
            table.title(), n))
    # For every column in the record, compare across databases
    for n in sorted(names1.intersection(names2)):
        e1, e2 = entries1[n], entries2[n]
        for a in list(dir(e1)):
            if a.startswith('_'):
                # Skip the non-public
                continue
            if a in (nameattr, '{}_id'.format(table)):
                # Don't compare IDs for this table
                continue
            if a in ('verbose_provenance', 'shasum', 'file_create_date'):
                # Don't compare stuff that's likely to be different
                continue
            if a.endswith('_id') or a in ('output_product'):
                # Reference to another table, compare *names*
                othertbl = 'product' if a == 'output_product' \
                           else a.split('_')[0]
                id1 = getattr(e1, a) #IDs into the other table
                id2 = getattr(e2, a)
                # Allow for not having a reference (e.g. no output product)
                if id1 in (None, ''):
                    n1 = ''
                else:
                    n1 = getattr(dbu1.getEntry(othertbl.title(), id1),
                                 '{}_name'.format(othertbl))
                if id2 in (None, ''):
                    n2 = ''
                else:
                    n2 = getattr(dbu2.getEntry(othertbl.title(), id2),
                                 '{}_name'.format(othertbl))
                if n1 != n2:
                    out.append('{} {} {} is {} in mission 1 but {} in mission 2'
                               .format(table.title(), n, a, n1, n2))
            elif getattr(e1, a) != getattr(e2, a):
                # Something else. Compare values
                out.append(
                    '{} {} {} is {} in mission 1 but {} in mission2'.format(
                        table.title(), n, a, getattr(e1, a), getattr(e2, a)))
    return out


def check_links(dbu1, dbu2):
    """Compare file-file and file-code links across databases

    Parameters
    ----------
    dbu1, dbu2 : dbprocessing.DButils.DButils
        database utils instances

    Returns
    -------
    list
        Any discrepancies found between databases (empty list if none)
    """
    out = []
    entries1 = [dbu1.getEntry('File', i) for i in dbu1.getAllFileIds()]
    entries2 = [dbu2.getEntry('File', i) for i in dbu2.getAllFileIds()]
    entries1 = { e.filename: e.file_id for e in entries1 }
    entries2 = { e.filename: e.file_id for e in entries2 }
    for f in sorted(entries1.keys()):
        if f not in entries2: # Handled in the table check above
            continue
        # Check for same input files
        infiles1 = set([e.filename for e in dbu1.getFileParents(entries1[f])])
        infiles2 = set([e.filename for e in dbu2.getFileParents(entries2[f])])
        for n in sorted(infiles1.difference(infiles2)):
            out.append('{} input file {} is in mission1 but not mission2.'
                       .format(f, n))
        for n in sorted(infiles2.difference(infiles1)):
            out.append('{} input file {} is in mission2 but not mission1.'
                       .format(f, n))
        # Now check for same input codes
        incode1 = dbu1.session.query(dbu1.Filecodelink)\
                       .filter_by(resulting_file=entries1[f]).all()
        incode2 = dbu2.session.query(dbu2.Filecodelink)\
                       .filter_by(resulting_file=entries2[f]).all()
        if len(incode1) != len(incode2):
            out.append('{} has {} codes in mission1 but {} in mission2.'
                       .format(f, len(incode1), len(incode2)))
            continue
        if len(incode1) == 0:
            continue
        if len(incode1) != 1:
            out.append('{} has {} codes, should be 0 or 1.'
                       .format(f, len(incode1)))
            continue
        incode1, incode2 = incode1[0], incode2[0]
        code1 = dbu1.getEntry('Code', incode1.source_code)
        code2 = dbu2.getEntry('Code', incode2.source_code)
        for attr in ('code_description', 'filename', 'relative_path',
                     'arguments'):
            e1, e2 = getattr(code1, attr), getattr(code2, attr)
            if e1 != e2:
                out.append('{} code has {} {} in mission1 but {} in mission2.'
                           .format(f, attr, e1, e2))
    return out


def parse_args(argv=None):
    """Parse arguments for this script

    Parameters
    ----------
    argv : list
        Argument list, default from sys.argv

    Returns
    -------
    options : argparse.Values
        Arguments from command line, from flags and non-flag arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mission", required=True, action='append',
                        help="Mission databases to compare, specify twice")

    options = parser.parse_args(argv)
    if len(options.mission) != 2:
        parser.error('Must specify two missions for comparison.')
    return options


if __name__ == "__main__":
    options = parse_args()
    main(options.mission[0], options.mission[1])
