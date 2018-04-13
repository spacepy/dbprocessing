#!/usr/bin/env python

import datetime
from optparse import OptionParser

import networkx
from dbprocessing import DButils
from dbprocessing import Version

def build_graph(dbu):
    G = networkx.DiGraph()
    
    G.add_nodes_from([(i, {
        'filename': a,
        'product_id': b,
        'utc_file_date': c,
        'exists_on_disk': d,
        'version': Version.Version(e, f, g)
    }) for i, a, b, c, d, e, f, g in dbu.session.query(
        dbu.File.file_id, dbu.File.filename, dbu.File.product_id,
        dbu.File.utc_file_date, dbu.File.exists_on_disk, 
        dbu.File.interface_version, dbu.File.quality_version,
        dbu.File.revision_version).all()])

    G.add_edges_from(
        dbu.session.query(dbu.Filefilelink.source_file,
                          dbu.Filefilelink.resulting_file).all())
    return G


def get_fastdata_participants(graph, cutoff):
    l0 = [n for n, d in graph.in_degree() if d == 0]
    fast0 = set([x for x in l0 if graph.nodes[x]['utc_file_date'] < cutoff])
    fast0_children_lists = [networkx.descendants(graph, x) for x in fast0]
    
    fast0_children = set()
    for sublist in fast0_children_lists:
        for item in sublist:
            fast0_children.add(item)

    return fast0_children | fast0

def do_things(graph, participants):
    # Punch Myles if this comment exists for naming this function so poorly
    nodes = [graph.nodes[x] for x in participants]
    nodes.sort(key = lambda file: (file['product_id'], file['utc_file_date'], file['version']))
    
    for node in reversed(nodes):
        pass

if __name__ == '__main__':
    usage = "usage: %prog -m database"
    parser = OptionParser(usage=usage)
    parser.add_option(
        "-m",
        "--mission",
        dest="mission",
        help="selected mission database",
        default=None)

    (options, args) = parser.parse_args()

    dbu = DButils.DButils(options.mission)
    G = build_graph(dbu)

    fd = get_fastdata_participants(G, datetime.datetime(2015, 1, 1))
    do_things(G, fd)

    dbu.closeDB()
