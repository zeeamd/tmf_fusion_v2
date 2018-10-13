import unittest
import logging
from .utils.configure_spark_session import configure_spark_session

from pandas.testing import assert_frame_equal


class PySparkTest(unittest.TestCase):

    @classmethod
    def suppress_py4j_logging(cls):
        logger = logging.getLogger('py4j')
        logger.setLevel(logging.WARN)

    @classmethod
    def create_testing_pyspark_session(cls):
        return configure_spark_session('my - local - testing - pyspark - context')

    @classmethod
    def setUpClass(cls):
        cls.suppress_py4j_logging()
        cls.spark = cls.create_testing_pyspark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    @staticmethod
    def assert_frame_equal_with_sort(results, expected, keycolumns):
        results_sorted = results.sort_values(by=keycolumns).reset_index(drop=True)
        expected_sorted = expected.sort_values(by=keycolumns).reset_index(drop=True)
        assert_frame_equal(results_sorted, expected_sorted)