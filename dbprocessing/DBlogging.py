from __future__ import print_function

import datetime
import logging
import logging.handlers
import os
import time

"""
Setup routines to log information from the dbprocessing chain

"""

utctoday = datetime.datetime.utcnow().date().strftime('%Y-%m-%d')

try:
    logname
except NameError:
    logname = None

# TODO this should be setup by a config file
if('DBPROCESSING_LOG_DIR' in os.environ):
    log_dir = os.environ['DBPROCESSING_LOG_DIR']
else:
    log_dir = os.path.join('~', 'dbprocessing_logs')
log_dir = os.path.expanduser(log_dir)
if not os.path.isdir(log_dir):
    os.makedirs(log_dir)
basename = 'dbprocessing_{0}'.format(logname if logname else 'log')
LOG_FILENAME = os.path.expanduser(os.path.join(log_dir, '{0}.log.{1}'.format(
    basename, utctoday)))

# Set up a specific logger with our desired output level
dblogger = logging.getLogger('DBLogger')
dblogger.setLevel(logging.INFO)

# Add the log message handler to the logger
# handler = logging.handlers.TimedRotatingFileHandler(
#              LOG_FILENAME, maxBytes=20000000, backupCount=0) # keep them all
## TODO this doesn't work so hardcode the name above, so break the rotation here
# Without explicit encoding on the handler, logging during interpreter shutdown
# (e.g. DButils.closeDB(), from __del__) will fail.
# https://stackoverflow.com/questions/42372981/filehandler-encoding-producing-exception
handler = logging.handlers.TimedRotatingFileHandler(
    LOG_FILENAME, when='midnight', interval=1, backupCount=0, # keep them all
    utc=True, encoding='ascii')

LEVELS = { 'debug': logging.DEBUG,
           'info': logging.INFO,
           'warning': logging.WARNING,
           'error': logging.ERROR,
           'critical': logging.CRITICAL }

# create formatter
formatter = \
    logging.Formatter("%(asctime)s - %(module)s:%(lineno)d - %(levelname)s" +
                      " - %(message)s")
logging.Formatter.converter = time.gmtime

# add formatter to ch
handler.setFormatter(formatter)

# add ch to logger
dblogger.addHandler(handler)

dblogger.info("DBLogger initialized")


def change_logfile(logname=None):
    global LOG_FILENAME, handler, formatter, dblogger
    basename = 'dbprocessing_{0}'.format(logname if logname else 'log')
    old_filename = LOG_FILENAME
    LOG_FILENAME = os.path.expanduser(os.path.join(log_dir, '{0}.log.{1}'.format(
        basename, utctoday)))
    dblogger.info("Logging file switched from {0} to {1}".format(old_filename, LOG_FILENAME))
    new_handler = logging.handlers.TimedRotatingFileHandler(
        LOG_FILENAME, when='midnight', interval=1, backupCount=0, # keep them all
        utc=True, encoding='ascii')
    new_handler.setFormatter(formatter)
    dblogger.removeHandler(handler)
    handler.close()
    dblogger.addHandler(new_handler)
    handler = new_handler
    dblogger.info("Switching logging file from {0} to {1}".format(old_filename, LOG_FILENAME))
