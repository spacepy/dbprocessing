
import datetime
import logging
import logging.handlers
import os
import time

__version__ = '2.0.3'



"""
Setup routines to log information from the dbprocessing chain

"""

utctoday = datetime.datetime.utcnow().date().strftime('%Y-%m-%d')

# TODO this should be setup by a config file
LOG_FILENAME = os.path.expanduser(os.path.join('~', 'dbprocessing_logs',
                                               'dbprocessing_log.log.{0}'.format(utctoday)))

# Set up a specific logger with our desired output level
dblogger = logging.getLogger('DBLogger')
dblogger.setLevel(logging.DEBUG)

# Add the log message handler to the logger
#handler = logging.handlers.TimedRotatingFileHandler(
#              LOG_FILENAME, maxBytes=20000000, backupCount=0) # keep them all
## TODO this doesn't work so hardcode the name above, so break the rotation here
handler = logging.handlers.TimedRotatingFileHandler(
              LOG_FILENAME, when='midnight', interval=1000000, backupCount=0, utc=True) # keep them all

LEVELS = {'debug': logging.DEBUG,
          'info': logging.INFO,
          'warning': logging.WARNING,
          'error': logging.ERROR,
          'critical': logging.CRITICAL}


# create formatter
formatter = \
    logging.Formatter("%(asctime)s - %(module)s:%(lineno)d - %(levelname)s" +
                      " - %(message)s")
logging.Formatter.converter = time.gmtime

# add formatter to ch
handler.setFormatter(formatter)

# add ch to logger
dblogger.addHandler(handler)

# test and do the rollover if needed:
if dblogger.handlers[0].shouldRollover(dblogger.info('test rollover')):
    dblogger.handlers[0].doRollover()


dblogger.info("DBLogger initialized")
