#!/usr/bin/env python

import datetime
import itertools
import os
from optparse import OptionParser

import networkx
from dbprocessing import DButils, Version


def build_graph(dbu):
    """Loads the file records and their parent-child relationships into a networkX graph
    
    Arguments:
        dbu {DButils} -- The DButils instance for the mission
    
    Returns:
        networkx.Graph -- A Graph of file records and their parent-child relationships
    """

    G = networkx.DiGraph()
    # This way is WAY slower, by about 3x - Myles 5/29/18
    # G.add_nodes_from([(f.file_id, {
    #     'filename': f.filename,
    #     'product_id': f.product_id,
    #     'utc_file_date': f.utc_file_date,
    #     'exists_on_disk': f.exists_on_disk,
    #     'version': dbu.getFileVersion(f)
    # }) for f in dbu.session.query(dbu.File).all()])

    G.add_nodes_from([(i, {
        'filename': a,
        'product_id': b,
        'utc_file_date': c,
        'exists_on_disk': d,
        'version': Version.Version(e, f, g)
    }) for i, a, b, c, d, e, f, g in dbu.session.query(
        dbu.File.file_id, dbu.File.filename, dbu.File.product_id, dbu.File.
        utc_file_date, dbu.File.exists_on_disk, dbu.File.interface_version,
        dbu.File.quality_version, dbu.File.revision_version).all()])

    G.add_edges_from(
        dbu.session.query(dbu.Filefilelink.source_file,
                          dbu.Filefilelink.resulting_file).all())
    return G


def get_fastdata_participants(graph, cutoff):
    """Get all file records that meet the fast data participant requirements
    
    Arguments:
        graph {networkx.Graph} -- A graph of file records and their parent-child relationships
        cutoff {datetime.date} -- Date to use as the fast data cut off
    
    Returns:
        set(dict) -- Set of the file records
    """
    print(type(graph.nodes[1]['utc_file_date']))
    l0 = [n for n, d in graph.in_degree() if d == 0]
    fast0 = set([x for x in l0 if graph.nodes[x]['utc_file_date'] > cutoff])
    fast0_children_lists = [networkx.descendants(graph, x) for x in fast0]
    fast0_children = set(itertools.chain.from_iterable(fast0_children_lists))

    return fast0_children | fast0


def reap_files(graph, participants):
    nodes = [graph.nodes[x] for x in participants]
    nodes.sort(key=lambda file: (
        file['product_id'], file['utc_file_date'], file['version']))

    for node in reversed(nodes):
        print(node)


if __name__ == '__main__':
    usage = "usage: %prog -m database"
    parser = OptionParser(usage=usage)
    parser.add_option(
        "-m",
        "--mission",
        dest="mission",
        help="selected mission database",
        default=None)
    parser.add_option(
        "-d",
        "--cutoff",
        dest="cutoff",
        help="What date to use as the fast data cut off",
        default=None)
    parser.add_option(
        "--reap-files",
        dest="files",
        action='store_true',
        help="Removes all Level0 files, and all of their children, that are not the newest version and are older than the cut off date. It will still keep the records of the files in the dbprocessing database, but sets exists_on_disk to false.",
        default=False)
    parser.add_option(
        "--reap-records",
        dest="records",
        action='store_true',
        help="Removes any file records that are marked as not exists_on_disk, and will sanity check that none of its children are still on disk. It also removes the corresponding file_file_links and file_code_links.",
        default=False)

    (options, args) = parser.parse_args()

    dbu = DButils.DButils(options.mission)
    G = build_graph(dbu)

    fd = get_fastdata_participants(G, datetime.date(2015, 1, 1))
    
    if options.files:
        reap_files(G, fd)
    
    # if options.records:
    #     reap_records(G, fd)

    dbu.closeDB()
