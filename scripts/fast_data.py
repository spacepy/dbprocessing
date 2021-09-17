#!/usr/bin/env python

import argparse
import datetime
import itertools
import os
import os.path
import shutil
import warnings

import networkx
import spacepy.datamanager

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
        'in_release': False, #Assume false, correct below
        'newest': False, #Assume false, correct below
        'product_id': b,
        'utc_file_date': c,
        'exists_on_disk': d,
        'version': Version.Version(e, f, g)
    }) for i, a, b, c, d, e, f, g in dbu.session.query(
        dbu.File.file_id, dbu.File.filename, dbu.File.product_id, dbu.File.
        utc_file_date, dbu.File.exists_on_disk, dbu.File.interface_version,
        dbu.File.quality_version, dbu.File.revision_version).all()])

    for f in dbu.getFiles(newest_version=True):
        G.node[f.file_id]['newest'] = True
    for f in dbu.session.query(dbu.Release.file_id):
        G.node[f[0]]['in_release'] = True

    G.add_edges_from(
        dbu.session.query(dbu.Filefilelink.source_file,
                          dbu.Filefilelink.resulting_file).all())
    return G


def filter_graph(graph, cutoff):
    """Filter files from the graph that we don't want to delete

    All files that are the newest version, or in a release, and any
    of their inputs should be kept (i.e. removed from consideration
    for deletion.)

    Also any level0 files that are older than our cutoff (and thus
    out of the "fast" regime), and their descendents.

    Why not outputs for newest/release? Because one file might be the
    latest and have multiple outputs for same day/product due to
    reprocessing, and we only want the latest of those, and it will be
    retained in its own right. Similarly for the release...just
    because something was built from a file in the release doesn't
    mean it needs to be kept.

    Note this will keep inputs to "latest" even if the inputs themselves
    aren't the latest (i.e. the database is in an inconsistent state).
    Earlier version of fast_data would not keep those inputs, so this
    is a little more paranoid.
    """
    removenodes = set()
    for i in graph:
        if graph.node[i]['newest'] or graph.node[i]['in_release']:
            removenodes.add(i)
            removenodes.update(networkx.ancestors(graph, i))
        if graph.in_degree(i) == 0 and graph.node[i]['utc_file_date'] <= cutoff:
            removenodes.add(i)
            removenodes.update(networkx.descendants(graph, i))
    graph.remove_nodes_from(removenodes)


def reap(dbu, graph, participants, dofiles=False, dorecords=False, verbose=False,
         archive=None):
    """Reap files and/or records from fileids in a set

    Arguments:
        graph: the full graph of file relationships
        participants: Nodes (file IDs) that meet the fast data criteria
                      (from get_fastdata_participants). Set.
        dofiles: remove files from disc
        dorecords: remove records from db (only if the files do not exist)
        verbose: print matching files (ones to delete)
        archive: directory to copy files to instead of deleting, requires dofiles.
    """
    if dofiles and dorecords:
        #This is in theory possible but for now probably safer not to
        raise ValueError('Cannot reap files and records in one call')
    if archive is not None and not dofiles:
        raise ValueError(
            'Cannot specify archive directory without reaping files')
    missiondir = spacepy.datamanager.RePath.path_split(
        dbu.getMissionDirectory())
    # Path index of "leading" directory information
    leading_idx = len(missiondir)
    if missiondir[-1] == '':
        leading_idx -= 1
    nodes = [graph.node[x] for x in participants]
    nodes.sort(key=lambda file: (
        file['product_id'], file['utc_file_date'], file['version']))
    for node in reversed(nodes[:-1]):
        if verbose:
            print(node['filename'])
        if node['exists_on_disk'] and dofiles:
            fullpath = dbu.getFileFullPath(node['file_id'])
            if archive is None:
                os.remove(fullpath)
            else:
                targetdir = os.path.join(
                    archive, spacepy.datamanager.RePath.path_slice(
                        fullpath, leading_idx, -1))
                if not os.path.isdir(targetdir):
                    os.makedirs(targetdir)
                shutil.move(fullpath, targetdir)
            # This is slow, and we(I.E. not me) can fix it later if its too slow. - Myles 6/5/2018
            dbu.getEntry('File', node['file_id']).exists_on_disk = False
        if dorecords:
            if not node['exists_on_disk']:
                dbu._purgeFileFromDB(node['file_id'], trust_id=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-m",
        "--mission",
        required=True,
        help="selected mission database",)
    parser.add_argument(
        "-a", "--archive", default=None,
        help="Directory to archive files instead of deleting",)
    parser.add_argument(
        "--cutoff",
        required=True,
        help="What date to use as the fast data cut off",)
    parser.add_argument(
        "--reap-files",
        dest="files",
        action='store_true',
        help="Removes all Level0 files, and all of their children, that are not the newest version and are newer than the cut off date. It will still keep the records of the files in the dbprocessing database, but sets exists_on_disk to false.",
        default=False)
    parser.add_argument(
        "--reap-records",
        dest="records",
        action='store_true',
        help="Removes any file records that are marked as not exists_on_disk, and will sanity check that none of its children are still on disk. It also removes the corresponding file_file_links and file_code_links.",
        default=False)
    parser.add_argument(
        "--verbose",
        action='store_true',
        help="Prints names of files that will be deleted",
        default=False)

    options = parser.parse_args()

    if options.mission is None:
        parser.error("Mission needs to be specified")

    try:
        cut_date = datetime.datetime.strptime(
            options.cutoff, "%Y-%m-%d").date()
    except TypeError as err:
        parser.error("Must pass a cutoff date in form YYYY-MM-DD")
    except ValueError as err:
        parser.error(str(err))

    if not (options.files or options.records):
        warnings.warn("Will not take any action without --reap-files or"
                      " --reap-records")

    dbu = DButils.DButils(options.mission)
    G = build_graph(dbu)

    filter_graph(G, cut_date)
    fd = set(G)

    if fd:
        reap(dbu, G, fd, dofiles=options.files, dorecords=options.records,
             verbose=options.verbose, archive=options.archive)

    dbu.commitDB()
    dbu.closeDB()
