This file describes the script sin this directory

Processing Chain
================
ProcessQueue.py - main file to run for the processing chain
 - 2 modes, ingest and process
   - ingest mode brings files form incoming into the db
   - process mode runs processing on files in the db table processqueue
Usage: ProcessQueue [-i] [-p] [-m Test]
   -i -> import
   -p -> process
   -m -> selects mission



Database interaction
====================
addProducts.py - add products to the database per a product configuration file
Usage: addProducts.py <filename>
   -> config file to read

updateProducts.py - update a product via a changes configuration file as written by writeProductsConf.py
Usage: updateProducts.py <filename>
   -> config file to update

writeProcessConf.py - write our a process configuration file based on existing DB entries
 - This works but the read does not follow this template, no real need to use this
Usage: writeProcessConf.py <process name> <filename>
   -> process name (or number) to write to config file

addProcess.py - add a process to the db via a configuration file
Usage: addProcess.py <filename>
   -> config file to read

deleteAllDBFiles.py - remove all file entries for the DB
 - this does not remove them from disk but does remove the DB entries
 - this has no confirmation, use sparingly

writeDBhtml.py - write out the database contents into html pages, a handy dump
Usage: writeDBhtml.py <mission> <filename>
   -> mission name to write to html

writeProductsConf.py - write out a config file for an existing product
 - all will write out config files for all products, this is the normal usage
Usage: writeProductsConf.py <product name> <filename>
   -> product name (or number) to write to config file














Versions:
8Aug2012 BAL
 - initial revision


