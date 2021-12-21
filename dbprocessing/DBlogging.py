"""Support for logging information from the dbprocessing chain."""

from __future__ import print_function

import datetime
import logging
import logging.handlers
import os
import time


utctoday = datetime.datetime.utcnow().date().strftime('%Y-%m-%d')
"""Current UTC date as YYYY-MM-DD (:class:`str`)"""

try:
    logname
except NameError:
    logname = None
    """Base name of the log file (``dbprocessing_`` is prepended and the date
       appended) (:class:`str`)"""

# TODO this should be setup by a config file
log_dir = os.environ.get('DBPROCESSING_LOG_DIR',
                         os.path.join('~', 'dbprocessing_logs'))
"""Directory to contain all dbprocessing log files (:class:`str`)"""
log_dir = os.path.expanduser(log_dir)
if not os.path.isdir(log_dir):
    os.makedirs(log_dir)
basename = 'dbprocessing_{0}'.format(logname if logname else 'log')
"""Name of log file without date (:class:`str`)"""
LOG_FILENAME = os.path.expanduser(os.path.join(log_dir, '{0}.log.{1}'.format(
    basename, utctoday)))
"""Full name of the log file (:class:`str`)"""

# Set up a specific logger with our desired output level
dblogger = logging.getLogger('DBLogger')
"""Logger instance for all dbprocessing code (:class:`~logging.Logger`)"""
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
"""Handler instance for all dbprocessing code
   (:class:`~logging.handlers.TimedRotatingFileHandler`)"""

LEVELS = { 'debug': logging.DEBUG,
           'info': logging.INFO,
           'warning': logging.WARNING,
           'error': logging.ERROR,
           'critical': logging.CRITICAL }
"""Map name of logging level names to numbers (:class:`dict`)"""

# create formatter
formatter = \
    logging.Formatter("%(asctime)s - %(module)s:%(lineno)d - %(levelname)s" +
                      " - %(message)s")
# """Log message formatter (:class:`~logging.Formatter`)"""
# Autodoc erroneously grabs the Formatter docstring, which
# isn't validly formatted for Sphinx, so leaving this docstring commented out.
logging.Formatter.converter = time.gmtime

# add formatter to ch
handler.setFormatter(formatter)

# add ch to logger
dblogger.addHandler(handler)

dblogger.info("DBLogger initialized")


def change_logfile(logname=None):
    """Switch to a new log file

    This implements switching default logging from one filename to another,
    usually used to indicate the mission being processed.
    The filename will always start with ``dbprocessing_`` and include
    a date string for the day.

    Parameters
    ----------
    logname : :class:`str`, default "log"
        Name to include in full log filename.
    """
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
