# import unittest
# import os
# import sys
# import time
# import logging
#
# from library.src.main.utils.configure_logger import configure_logger, StreamToLogger
#
# from cph_fusion.src.test.donor_recip_test import CPHDonorRecipTest
#
#
# if __name__ == '__main__':
#     time_str = time.strftime("%Y%m%d_%H%M%S")
#
#     LOG_PATH = input('Define your log path: ')
#
#     if (str(LOG_PATH) == 'None') | (len(str(LOG_PATH)) == 0):
#         LOG_PATH = 'logs/cph/'
#
#     if not os.path.exists(LOG_PATH):
#         os.makedirs(LOG_PATH)
#
#     log_flnm = LOG_PATH + 'cph_unit_test_{}.log'.format(time_str)
#
#     main_logger = configure_logger(__name__, 'cph_fusion', log_flnm)
#
#     stdout_logger = logging.getLogger('STDOUT')
#     sl = StreamToLogger(stdout_logger)
#     sys.stdout = sl
#
#     stderr_logger = logging.getLogger('STDERR')
#     sl = StreamToLogger(stderr_logger)
#     sys.stderr = sl
#
#     unittest.main()

import random
try:
    import unittest2 as unittest
except ImportError:
    import unittest

class SimpleTest(unittest.TestCase):
    @unittest.skip("demonstrating skipping")
    def test_skipped(self):
        self.fail("shouldn't happen")

    def test_pass(self):
        self.assertEqual(10, 7 + 3)

    # def test_fail(self):
    #     self.assertEqual(11, 7 + 3)