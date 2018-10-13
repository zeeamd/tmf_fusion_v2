import unittest
import time
import sys
import os
import logging

from library.src.main.utils.configure_logger import configure_logger, StreamToLogger

from recoding.src.test.nol_test import TestNOLRecode
from recoding.src.test.npm_test import TestNPMRecode
from recoding.src.test.dcr_test import TestDCRRecode

if __name__ == '__main__':
    time_str = time.strftime("%Y%m%d_%H%M%S")

    LOG_PATH = input('Define your log path: ')

    if (str(LOG_PATH) == 'None') | (len(str(LOG_PATH)) == 0):
        LOG_PATH = 'logs/recoding/'

    if not os.path.exists(LOG_PATH):
        os.makedirs(LOG_PATH)

    log_flnm = LOG_PATH + 'recoding_unit_test_{}.log'.format(time_str)

    main_logger = configure_logger(__name__, 'recoding', log_flnm)

    stdout_logger = logging.getLogger('STDOUT')
    sl = StreamToLogger(stdout_logger)
    sys.stdout = sl

    stderr_logger = logging.getLogger('STDERR')
    sl = StreamToLogger(stderr_logger)
    sys.stderr = sl

    unittest.main()
