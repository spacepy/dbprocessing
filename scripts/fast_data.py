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
    # G.add_nodes_from([(f.file_id, f)
    #                   for f in dbu.session.query(dbu.File).all()])
    G.add_nodes_from([(i, {
        'file_id': i,
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
    l0 = [n for n, d in graph.in_degree() if d == 0]
    fast0 = set([x for x in l0 if graph.nodes[x]['utc_file_date'] > cutoff])
    fast0_children_lists = [networkx.descendants(graph, x) for x in fast0]
    fast0_children = set(itertools.chain.from_iterable(fast0_children_lists))

    return fast0_children | fast0


def reap_files(graph, participants):
    nodes = [graph.nodes[x] for x in participants]
    nodes.sort(key=lambda file: (
        file['product_id'], file['utc_file_date'], file['version']))

    last_node = nodes[0]
    for node in reversed(nodes):
        if (node['product_id'] == last_node['product_id'] and
                node['utc_file_date'] == last_node['utc_file_date'] and
                node['version'] < last_node['version']):
            os.remove(dbu.getFileFullPath(node['file_id']))
            # This is slow, and we(I.E. not me) can fix it later if its too slow. - Myles 6/5/2018
            dbu.getEntry('File', node['file_id']).exists_on_disk = False

        last_node = node


def reap_records(graph, participants):
    nodes = [graph.nodes[x] for x in participants]
    nodes.sort(key=lambda file: (
        file.product_id, file.utc_file_date, file.version))

    last_node = nodes[0]
    for node in reversed(nodes):
        if (node['product_id'] == last_node['product_id'] and
                node['utc_file_date'] == last_node['utc_file_date'] and
                node['version'] < last_node['version']):
            if not node.exists_on_disk:
                dbu._purgeFileFromDB(node['file_id'], trust_id=True)
            
        last_node = node


if __name__ == '__main__':
    usage = "usage: %prog -m database -d YYYY-MM-DD --reap-files --reap-records"
    parser = OptionParser(usage=usage)
    parser.add_option(
        "-m",
        "--mission",
        dest="mission",
        help="selected mission database",)
    parser.add_option(
        "-d",
        "--cutoff",
        dest="cutoff",
        help="What date to use as the fast data cut off",)
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

    if options.mission is None:
        parser.error("Mission needs to be specified")

    try:
        cut_date = datetime.datetime.strptime(
            options.cutoff, "%Y-%m-%d").date()
    except TypeError as err:
        parser.error("Must pass a cutoff date")
    except ValueError as err:
        parser.error(err.message)

    if not (options.files or options.records):
        parser.error("Must use --reap-files and/or --reap-records")

    dbu = DButils.DButils(options.mission)
    G = build_graph(dbu)

    fd = get_fastdata_participants(G, cut_date)

    if options.files:
        reap_files(G, fd)

    if options.records:
        reap_records(G, fd)

    dbu.closeDB()
