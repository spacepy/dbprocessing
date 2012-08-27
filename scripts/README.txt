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

weeklyReport.py - write out an html suitable for a weekly report type use of what the chain has done
 - somewhat limited not but a good start
Usage: scripts/weeklyReport.py <input directory> <startTime> <stopTime> <filename>
   -> directory with the dbprocessing_log.log files (automatically grabs all)
   -> start date e.g. 2000-03-12
   -> stop date e.g. 2000-03-17
   -> filename to write out the report
Example:
~/dbUtils/weeklyReport.py ~/tmp 2012-08-08 2012-08-09 weeklyReport.html

qualityControlFileDates.py - write out a text file with the dates of non QC checked files for a given product
Usage: qualityControlFileDates.py [-f, --file= filename] product_name
        -f output filename (default QC_dates.txt)
        product name (or ID)
Example:
~/dbUtils/qualityControlFileDates.py rbspb_pre_ect-rept-sci-L2


Other info
==========
Totally clean out the DB:  (leaves mission, instrument, satellite)
run these commands (done this way so it is hard to do)
from dbprocessing import DBUtils2
a = DBUtils2.DBUtils2('rbsp')
a._openDB()
a._createTableObjects()
a.deleteAllEntries()











Versions:
8Aug2012 BAL
 - initial revision


