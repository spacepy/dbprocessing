"""
UNFINISHED
TODO: For all files before date, check the graph and see 
      if they can be deleted without conflict.
"""

import sys
import os
import sqlite3
import re
import Graph

from dbprocessing import DButils

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
 
    return None

def delete_before_date(conn, date):
    """
    Delete all files before certain date
    :param conn:  Connection to the SQLite database
    :param date: Date before which files are to be deleted
    :return:
    """
    numDel = 0
    d = ''.join(date)
    d = re.sub('-', '', d)
    d = int(d)              # Converts date string into int.
    
    graph = create_graph(conn)
    
    sql = 'DELETE FROM file WHERE utc_file_date=?'
    cur = conn.cursor()
    cur.execute("SELECT file_id,utc_file_date  FROM file")
    files = cur.fetchall()
    count = 0
    filesToBeDeleted = []
    for file in files:           #Creates list of all files before date
        f = ''.join(file[1])
        f = re.sub('-', '', f)
        f = int(f)
        if f <= d:
            #cur.execute(sql, file[1])
            filesToBeDeleted.append(f)
            count += 1
    """
    TODO: Check to see if files in ftbd have connections in graph.
          If it has neighbors, delete only if neighbors are also in ftbd,
          otherwise if no neighbors just delete & update tables.

    for f in filesToBeDeleted:        
        node = graph.getNode(str(f[0]))
        neighbors = graph.neighbors(node)
        if neighbors == {}:
            cur.execute(sql, f[1])
            numDel += 1
        else:
            for n in neighbors:
                if n in filesToBeDeleted:
                    cur.execute(sql, f[i])
                    cur.execute(sql, n)
                    numDel += 2
    """
        
    print('{0} files before {1}, {2} deleted.'.format(count,date,numDel))

def delete_all_files(conn):
    """
    Delete all rows in the tasks table
    :param conn: Connection to the SQLite database
    :return:
    """
    sql = 'DELETE FROM file'
    cur = conn.cursor()
    cur.execute(sql)

def create_graph(conn):
    """
    Creates a graph with directed edge from all source files
    to the result files within filefilelink.
    :param conn: Connection to the SQLite database
    :return: Directed Graph.
    """
    c = conn.cursor()
    c.execute("SELECT source_file, resulting_file FROM filefilelink")
    rows = c.fetchall()
    count = 0
    graph = Graph.Graph()
    
    for r in rows:
        if not graph.contains(r[0]):
            graph.add_node(r[0])
            
        if not graph.contains(r[1]):
            graph.add_node(r[1])
            
        graph.add_edge(r[0], r[1])
        verts = graph.nodes()

    for a in verts:
        print(a)
        print(graph.neighbors(a))
        print("____________")
        
    return graph
            
def adjust_links(conn, graph):
    """
    Checks the file links to make sure deleted file
    doesn't affect other files, and adjusts
    the table if files are to be deleted.
    :param conn: Connection to the SQLite database
    :param fileNum: Number of the file in question
    :return: boolean: Whether file can be deleted.
    """
    graph = self.create_graph(conn)

def main():
    
    database = '/home/natejm/dbprocessing/tests/testDB/testDB2.sqlite'
    # create a database connection
    conn = create_connection(database)
    with conn:
        delete_before_date(conn, '2016-01-01');
        #adjust_links(conn, 0)
        create_graph(conn)
 
if __name__ == '__main__':
    main()



