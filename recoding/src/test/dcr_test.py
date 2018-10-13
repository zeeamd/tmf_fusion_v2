import unittest
import pandas as pd
from pandas.testing import assert_frame_equal

import logging

from library.src.main.utils.io import force_decode
from recoding.src.main.dcr.recoding import build_dcr_brand_by_type_data_pandas, build_dcr_sub_brand_by_type_data_pandas

test_dir = '/Users/tued7001/Documents/Test_Files/total_media_fusion/201805'


class TestDCRRecode(unittest.TestCase):

    @staticmethod
    def assert_frame_equal_with_sort(results, expected, keycolumns):
        results_sorted = results.sort_values(by=keycolumns).reset_index(drop=True)
        expected_sorted = expected.sort_values(by=keycolumns).reset_index(drop=True)
        assert_frame_equal(results_sorted, expected_sorted)

    def _test_build_dcr_brand_by_type(self, input_df, test_df, dcr_type):
        logger = logging.getLogger(__name__)

        logger.info('Testing with DCR Brand type {}'.format(dcr_type))

        try:

            res_df = build_dcr_brand_by_type_data_pandas(input_df, dcr_type)

        except Exception as e:
            logger.fatal(e, exc_info=True)
            res_df = pd.DataFrame()

        cols_to_test = test_df.columns

        test_df['site_id'] = test_df['site_id'].astype(int)
        test_df['name'] = test_df['name'].apply(force_decode)
        test_df['computer'] = test_df['computer'].astype(float)
        test_df['unique_audience'] = test_df['unique_audience'].astype(float)

        if dcr_type == 'Text':
            test_df['duration'] = test_df['duration'].astype(float) * 60.0
        else:
            test_df['impressions'] = test_df['impressions'].astype(float)

        logger.info('Columns to test: {}'.format(cols_to_test))

        logger.info('Shape of test data {}'.format(test_df.shape[0]))
        logger.info('Shape of result data {}'.format(res_df.shape[0]))

        self.assert_frame_equal_with_sort(res_df[cols_to_test], test_df, 'site_id')
        logger.info("SUCCESS")

    def _test_build_dcr_sub_brand_by_type(self, input_df, test_df, dcr_type, subname_list):
        logger = logging.getLogger(__name__)

        logger.info('Testing with DCR Sub-brand type {}'.format(dcr_type))

        try:

            res_df = build_dcr_sub_brand_by_type_data_pandas(input_df, dcr_type, subname_list)

        except Exception as e:
            logger.fatal(e, exc_info=True)
            res_df = pd.DataFrame()

        cols_to_test = test_df.columns

        test_df['site_id'] = test_df['site_id'].astype(int)
        test_df['name'] = test_df['name'].apply(force_decode)
        test_df['computer'] = test_df['computer'].astype(float)
        test_df['unique_audience'] = test_df['unique_audience'].astype(float)

        if dcr_type == 'Text':
            test_df['duration'] = test_df['duration'].astype(float)
        else:
            test_df['impressions'] = test_df['impressions'].astype(float)

        logger.info('Columns to test: {}'.format(cols_to_test))

        logger.info('Shape of test data {}'.format(test_df.shape[0]))
        logger.info('Shape of result data {}'.format(res_df.shape[0]))

        self.assert_frame_equal_with_sort(res_df[cols_to_test], test_df, 'site_id')
        logger.info("SUCCESS")

    def test_build_dcr_brand_text(self):

        self._test_build_dcr_brand_by_type(pd.read_csv('/'.join([test_dir, 'query_result_desktop_text.csv'])),
                                           pd.read_csv('/'.join([test_dir, 'all_dcr_brands_pc_text.csv'])),
                                                       'Text')

    def test_build_dcr_brand_video(self):
        self._test_build_dcr_brand_by_type(pd.read_csv('/'.join([test_dir, 'query_result_desktop_video.csv'])),
                                           pd.read_csv('/'.join([test_dir, 'All_dcr_brands_pc_video.csv'])),
                                                       'Video')

    def test_build_dcr_sub_brand_text(self):

        self._test_build_dcr_sub_brand_by_type(pd.read_csv('/'.join([test_dir, 'query_result_desktop_text_sub.csv'])),
                                               pd.read_csv('/'.join([test_dir, 'All_dcr_brands_pc_txt_s.csv'])),
                                                           'Text', ['Preview'])

    def test_build_dcr_sub_brand_video(self):
        self._test_build_dcr_sub_brand_by_type(pd.read_csv('/'.join([test_dir, 'query_result_desktop_video_sub.csv'])),
                                               pd.read_csv('/'.join([test_dir, 'All_dcr_brands_pc_video_sub.csv'])),
                                                           'Video', ['Preview', 'Internal'])



