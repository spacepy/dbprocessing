
import logging
import logging.handlers

__version__ = '2.0.2'



"""
Setup routines to log information from the dbprocessing chain

"""


# TODO this should be setup by a config file
LOG_FILENAME = 'dbprocessing_log.log'

# Set up a specific logger with our desired output level
dblogger = logging.getLogger('DBLogger')
dblogger.setLevel(logging.DEBUG)

# Add the log message handler to the logger
handler = logging.handlers.RotatingFileHandler(
              LOG_FILENAME, maxBytes=2000000, backupCount=5)


LEVELS = {'debug': logging.DEBUG,
          'info': logging.INFO,
          'warning': logging.WARNING,
          'error': logging.ERROR,
          'critical': logging.CRITICAL}


# create formatter
formatter = \
    logging.Formatter("%(asctime)s - %(module)s:%(lineno)d - %(levelname)s" +
                      " - %(message)s")

# add formatter to ch
handler.setFormatter(formatter)

# add ch to logger
dblogger.addHandler(handler)


dblogger.info("DBLogger initialized")
