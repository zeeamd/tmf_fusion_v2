import pandas as pd
import logging

from library.src.main.pyspark_test_framework import PySparkTest

from recoding.src.main.nol.recoding_spark import recode_nol_site_usage_type, recode_nol_demos


class TestNOLRecode(PySparkTest):

    def test_recode_demo(self):
        logger = logging.getLogger(__name__)

        logger.info('Creating test data for Testing Recoding NOL Demo')

        nol_demo_test_df = pd.DataFrame(columns=['rn_id', 'surf_location_id', 'weight', 'age', 'gender_id',
                                                 'race_id', 'web_access_locations', 'education_id', 'income_id',
                                                 'occupation_id', 'members_2_11_count', 'members_12_17_count',
                                                 'zip_code',
                                                 'county_size_id', 'hispanic_origin_id', 'working_status_id',
                                                 'web_conn_speed_id'])

        nol_sample_test_df = pd.DataFrame(columns=['rn_id', 'surf_location_id'])

        nol_demo_test_df['rn_id'] = [1, 2, 3, 4, 5]
        nol_demo_test_df['surf_location_id'] = [1, 1, 1, 2, 1]
        nol_demo_test_df['weight'] = [100] * 5
        nol_demo_test_df['age'] = [2, 10, 16, 25, 50]
        nol_demo_test_df['gender_id'] = [1, 1, 1, 2, 1]
        nol_demo_test_df['race_id'] = [3, 2, 1, 1, 1]
        nol_demo_test_df['income_id'] = [-1, 2, 3, -1, 1]
        nol_demo_test_df['web_access_locations'] = [2, 3, 6, -1, 11]
        nol_demo_test_df['working_status_id'] = [-2, 1, 8, 1, 11]
        nol_demo_test_df['members_2_11_count'] = [0, 4, 8, 0, 11]
        nol_demo_test_df['members_12_17_count'] = [1, 0, 0, 0, 2]
        nol_demo_test_df['education_id'] = [5, 8, 7, 0, 2]
        nol_demo_test_df['hispanic_origin_id'] = [-1, 1, 1, 2, 2]
        nol_demo_test_df['occupation_id'] = [3, 4, 1, 8, 2]
        nol_demo_test_df['web_conn_speed_id'] = [1, 6, 1, 4, 1]

        nol_sample_test_df['rn_id'] = [1, 2, 3]
        nol_sample_test_df['surf_location_id'] = [1, 1, 9]

        test_df = pd.DataFrame(columns=['rn_id', 'surf_location_id', 'work_access', 'edu', 'race1', 'race2', 'inc',
                                        'hispanic', 'occ', 'children1', 'children2', 'speed', 'cs', 'gender'])

        test_df['rn_id'] = [1, 2]
        test_df['surf_location_id'] = [1, 1]
        test_df['work_access'] = [2, 1]
        test_df['edu'] = [1, 2]
        test_df['occ'] = [1, 1]
        test_df['race1'] = [2, 1]
        test_df['race2'] = [1, 2]
        test_df['inc'] = [4, 2]
        test_df['hispanic'] = [1, 1]
        test_df['children1'] = [1, 2]
        test_df['children2'] = [2, 1]
        test_df['speed'] = [1, 2]
        test_df['cs'] = [1, 1]
        test_df['gender'] = [1, 1]

        nol_demo_test_sdf = self.spark.createDataFrame(nol_demo_test_df)
        nol_sample_test_sdf = self.spark.createDataFrame(nol_sample_test_df)

        logger.info('Running recoding')

        res_sdf = recode_nol_demos(nol_demo_test_sdf, nol_sample_test_sdf)

        res_df = res_sdf.toPandas()

        test_demos = ['work_access', 'edu', 'race1', 'race2', 'inc',
                      'hispanic', 'occ', 'children1', 'children2', 'speed', 'cs', 'gender']

        cols_to_test = ['rn_id'] + test_demos

        for col in cols_to_test:
            res_df[col] = res_df[col].astype(int)
            test_df[col] = test_df[col].astype(int)

        self.assert_frame_equal_with_sort(res_df[cols_to_test], test_df[cols_to_test],
                                          'rn_id')

        logger.info('SUCCESS')

    def test_recode_site_usage(self):
        logger = logging.getLogger(__name__)

        logger.info('Creating test data for Testing Recoding NOL Site Usage')

        df = pd.DataFrame(columns=['rn_id', 'dummy_id', 'dummy_name', 'duration'])

        df['rn_id'] = [1, 2, 3, 4, 1, 2, 5, 3, 4]
        df['dummy_id'] = [1, 1, 1, 1, 3, 2, 1, 2, 2]
        df['dummy_name'] = 'site_' + df['dummy_id'].apply(str)
        df['duration'] = [0, 1, 1, 1, 2, 2, 2, 2, 2]

        test_df = pd.DataFrame(columns=['rn_id'])

        test_df['rn_id'] = [1, 2, 3, 4, 5]
        test_df['dummy_2'] = [0, 1, 1, 1, 2]
        test_df['dummy_1'] = [0, 2, 2, 2, 0]
        test_df['dummy_3'] = [2, 0, 0, 0, 0]

        test_df['dummy_yn002'] = [0, 1, 1, 1, 1]
        test_df['dummy_yn001'] = [0, 1, 1, 1, 0]
        test_df['dummy_yn003'] = [1, 0, 0, 0, 0]

        sdf = self.spark.createDataFrame(df)

        logger.info('Running recoding')

        res_sdf = recode_nol_site_usage_type(sdf, 'dummy')

        res_df = res_sdf.toPandas()

        cols_to_test = list(test_df.columns)

        for col in cols_to_test:
            res_df[col] = res_df[col].astype(int)
            test_df[col] = test_df[col].astype(int)

        logger.info('Asserting Results and Expectations are the same')

        self.assert_frame_equal_with_sort(res_df[cols_to_test], test_df[cols_to_test], 'rn_id')

        logger.info('SUCCESS')
