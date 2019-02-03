# HANDLE LOGGING
import logging
import os
import sys


def mylogger(__file__):
    # create a custom logger handler

    logfile = 'logs/' + os.path.splitext(os.path.basename(__file__))[0] + '.log'
    '''
    logger = logging.getLogger(logfile)
    handler = logging.FileHandler(logfile)
    handler.setLevel(logging.WARNING)
    l_format = logging.Formatter('%(asctime)s - [%(name)s:%(lineno)d]=> %(message)s')
    handler.setFormatter(l_format)
    logger.addHandler(handler)
    logger.warning(logfile)
    '''

    # console handler
    logger = logging.getLogger(logfile)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.WARNING)
    FORMAT = "%(asctime)s,%(filename)s:%(lineno)s ]=> %(message)s"
    formatter = logging.Formatter(FORMAT)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger