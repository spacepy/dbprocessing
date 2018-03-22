#!/usr/bin/env python

from optparse import OptionParser

import networkx as nx

from dbprocessing import DButils

def build_graph(dbu):
    G = nx.DiGraph()
    G.add_nodes_from((r for r, in
        dbu.session.query(dbu.File.file_id).all()))
    G.add_edges_from(dbu.session.query(dbu.Filefilelink.source_file, dbu.Filefilelink.resulting_file).all())
    return G

if __name__ == '__main__':
    usage = "usage: %prog -m database"
    parser = OptionParser(usage=usage)
    parser.add_option("-m", "--mission", dest="mission",
                      help="selected mission database", default=None)
    
    (options, args) = parser.parse_args()

    dbu = DButils.DButils(options.mission)
    G = build_graph(dbu)

    #print(len([n for n,d in G.in_degree().items() if d==0]))
    #print(nx.has_path(G, 1, 321022))
    dbu.closeDB()
