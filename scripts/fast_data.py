#!/usr/bin/env python

from optparse import OptionParser

import networkx as nx
import matplotlib.pyplot as plt

from dbprocessing import DButils

if __name__ == '__main__':
    usage = "usage: %prog -m database"
    parser = OptionParser(usage=usage)
    parser.add_option("-m", "--mission", dest="mission",
                      help="selected mission database", default=None)
    
    (options, args) = parser.parse_args()

    dbu = DButils.DButils(options.mission)
    G = nx.Graph()

    files = dbu.getFiles()
    for f in files:
        G.add_node(f.file_id)
    
    edges = dbu.session.query(dbu.Filefilelink).all()
    for e in edges:
        G.add_edge(e.source_file, e.resulting_file)
    
    #nx.draw(G, with_labels=True, font_weight='bold')
    #plt.savefig("graph.png")
    dbu.closeDB()