import pandas as pd
import numpy as np
import logging

from library.src.main.pyspark_test_framework import PySparkTest

from recoding.src.main.npm.recoding_spark import main_npm_demos_recode, main_npm_usage_recode


class TestNPMRecode(PySparkTest):

    def test_recode_demo(self):
        logger = logging.getLogger(__name__)

        logger.info('Creating test data for Testing Recoding NPM Demo')

        test_demos = ['hispanic', 'gender', 'race_back', 'dvr', 'cableplus', 'video_game', 'internet', 'paycable',
                      'hdtv',
                      'satellite', 'race_asian', 'occupation1', 'employment1', 'county_size', 'spanish_language1',
                      'kids_0to5', 'kids_6to11', 'kids_12to17', 'hh_size1', 'number_of_tvs1', 'education2',
                      'education7',
                      'income1', 'income9']

        #we should use small csv files
        npm_person_raw_df = pd.DataFrame(
            columns=['origin_code', 'gender_code', 'race_code', 'dvr_flag', 'cable_plus_flag',
                     'video_game_owner_flag', 'internet_access_flag', 'pay_cable_flag',
                     'television_high_definition_display_capability_flag',
                     'alternative_delivery_flag', 'nielsen_occupation_code',
                     'education_level_number', 'working_hours_number', 'language_class_code',
                     'avgwt', 'cph_daily_avgwt', 'countintab', 'zip_code', 'household_id', 'person_id',
                     'age', 'income_amt'])

        npm_hh_raw_df = pd.DataFrame(columns=['county_size_code', 'number_of_tv_sets', 'number_of_kids_less_than_6',
                                              'number_of_kids_less_than_12', 'number_of_kids_less_than_18',
                                              'avgwt', 'cph_daily_avgwt', 'countintab', 'zip_code', 'household_id'])

        npm_person_raw_df['household_id'] = np.array([1, 1, 2, 2, 3, 3, 3, 3, 3, 3])
        npm_person_raw_df['person_id'] = np.array([1, 2, 1, 2, 1, 2, 3, 4, 5, 6])
        npm_person_raw_df['origin_code'] = np.array([1, 1, 1, 1, 1, 1, 1, 1, 2, 2])
        npm_person_raw_df['gender_code'] = np.array([1, 1, 1, 1, 1, 1, 1, 1, 2, 2])
        npm_person_raw_df['race_code'] = np.array([3, 4, 5, 6, 7, 9, 1, 3, 2, 2])
        npm_person_raw_df['dvr_flag'] = np.array(['Y', 'Y', 'Y', 'Y', 'N', 'Y', 'N', 'Y', 'Y', 'Y'])
        npm_person_raw_df['cable_plus_flag'] = npm_person_raw_df['dvr_flag']
        npm_person_raw_df['video_game_owner_flag'] = npm_person_raw_df['dvr_flag']
        npm_person_raw_df['internet_access_flag'] = npm_person_raw_df['dvr_flag']
        npm_person_raw_df['pay_cable_flag'] = npm_person_raw_df['dvr_flag']
        npm_person_raw_df['television_high_definition_display_capability_flag'] = npm_person_raw_df['dvr_flag']
        npm_person_raw_df['alternative_delivery_flag'] = npm_person_raw_df['dvr_flag']
        npm_person_raw_df['avgwt'] = 1000 * np.random.random((npm_person_raw_df.shape[0],))
        npm_person_raw_df['cph_daily_avgwt'] = 1000 * np.random.random((npm_person_raw_df.shape[0],))
        npm_person_raw_df['zip_code'] = 0
        npm_person_raw_df['countintab'] = 1
        npm_person_raw_df['age'] = [5] * npm_person_raw_df.shape[0]
        npm_person_raw_df['income_amt'] = np.array([30, 40, 50, 60, 70, 90, 10, 30, 20, 20])
        npm_person_raw_df['education_level_number'] = np.array([0, 8, 9, 10, 11, 12, 13, 14, 15, 20])
        npm_person_raw_df['working_hours_number'] = np.array([0, 5, 10, 11, 50, 40, 44, 1, 100, 30])
        npm_person_raw_df['nielsen_occupation_code'] = [0, 1, 0, 1, 3, 4, 5, 4, 8, 8]
        npm_person_raw_df['language_class_code'] = np.array([1, 2, 1, 2, 1, 2, 3, 2, 4, 5])

        npm_hh_raw_df['household_id'] = np.array([1, 2, 3])
        npm_hh_raw_df['county_size_code'] = np.array(['A', 'A', 'B'])
        npm_hh_raw_df['number_of_tv_sets'] = np.array([5, 4, 6])
        npm_hh_raw_df['number_of_kids_less_than_6'] = np.ones((3,))
        npm_hh_raw_df['number_of_kids_less_than_12'] = [2, 2, 1]
        npm_hh_raw_df['number_of_kids_less_than_18'] = [3, 2, 1]
        npm_hh_raw_df['avgwt'] = 1000 * np.random.random((npm_hh_raw_df.shape[0],))
        npm_hh_raw_df['cph_daily_avgwt'] = 1000 * np.random.random((npm_hh_raw_df.shape[0],))
        npm_hh_raw_df['zip_code'] = 0
        npm_hh_raw_df['countintab'] = 1

        npm_test_data_values = np.array(
            [[1001, 1, 1, 2, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 6, 1, 1, 1, 2, 5, 2, 1, 2, 2],
             [1002, 1, 2, 2, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 6, 1, 1, 1, 2, 5, 2, 1, 2, 3],
             [2001, 1, 2, 2, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 6, 1, 1, 2, 2, 4, 2, 1, 3, 4],
             [2002, 1, 2, 2, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 6, 1, 1, 2, 2, 4, 2, 1, 3, 4],
             [3001, 1, 3, 2, 1, 2, 2, 2, 2, 2, 2, 2, 2, 1, 3, 3, 2, 6, 1, 2, 2, 5, 5, 2, 1, 3, 4],
             [3002, 2, 3, 2, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 3, 3, 2, 6, 1, 2, 2, 5, 5, 2, 1, 4, 5],
             [3003, 3, 3, 2, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 2, 6, 1, 2, 2, 5, 5, 2, 2, 1, 1],
             [3004, 4, 3, 2, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 3, 2, 2, 6, 1, 2, 2, 5, 5, 2, 2, 2, 2],
             [3005, 5, 3, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 2, 4, 1, 2, 4, 1, 2, 2, 5, 5, 2, 2, 1, 1],
             [3006, 6, 3, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 2, 4, 1, 2, 3, 1, 2, 2, 5, 5, 1, 4, 1, 1]])

        npm_recode_test_df = pd.DataFrame(data=npm_test_data_values,
                                          columns=['respondentid', 'household_id', 'person_id'] + test_demos)

        npm_hh_raw_sdf = self.spark.createDataFrame(npm_hh_raw_df)
        npm_person_raw_sdf = self.spark.createDataFrame(npm_person_raw_df)

        logger.info('Running recoding')

        npm_recode_res_sdf = main_npm_demos_recode(npm_hh_raw_sdf, npm_person_raw_sdf)

        npm_recode_res_df = npm_recode_res_sdf.toPandas()

        cols_to_test = ['respondentid'] + test_demos

        for col in cols_to_test:
            npm_recode_res_df[col] = npm_recode_res_df[col].astype(int)
            npm_recode_test_df[col] = npm_recode_test_df[col].astype(int)

        logger.info('Asserting Results and Expectations are the same')

        self.assert_frame_equal_with_sort(npm_recode_res_df[cols_to_test], npm_recode_test_df[cols_to_test],
                                          'respondentid')

        logger.info('SUCCESS')

    def test_recode_usage(self):
        logger = logging.getLogger(__name__)

        logger.info('Creating test data for Testing Recoding NPM TV Usage')

        test_demos = ['hispanic', 'gender', 'race_back', 'dvr', 'cableplus', 'video_game', 'internet', 'paycable',
                      'hdtv', 'satellite', 'race_asian', 'occupation1', 'employment1', 'county_size',
                      'spanish_language1', 'kids_0to5', 'kids_6to11', 'kids_12to17', 'hh_size1', 'number_of_tvs1',
                      'education2', 'education7', 'income1', 'income9']

        npm_test_data_values = np.array(
            [[1001, 1, 1, 2, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 6, 1, 1, 1, 2, 5, 2, 1, 2, 2],
             [1002, 1, 2, 2, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 6, 1, 1, 1, 2, 5, 2, 1, 2, 3],
             [2001, 1, 2, 2, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 6, 1, 1, 2, 2, 4, 2, 1, 3, 4],
             [2002, 1, 2, 2, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 6, 1, 1, 2, 2, 4, 2, 1, 3, 4],
             [3001, 1, 3, 2, 1, 2, 2, 2, 2, 2, 2, 2, 2, 1, 3, 3, 2, 6, 1, 2, 2, 5, 5, 2, 1, 3, 4],
             [3002, 2, 3, 2, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 3, 3, 2, 6, 1, 2, 2, 5, 5, 2, 1, 4, 5],
             [3003, 3, 3, 2, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 2, 6, 1, 2, 2, 5, 5, 2, 2, 1, 1],
             [3004, 4, 3, 2, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 3, 2, 2, 6, 1, 2, 2, 5, 5, 2, 2, 2, 2],
             [3005, 5, 3, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 2, 4, 1, 2, 4, 1, 2, 2, 5, 5, 2, 2, 1, 1],
             [3006, 6, 3, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 2, 4, 1, 2, 3, 1, 2, 2, 5, 5, 1, 4, 1, 1]])

        npm_recode_test_df = pd.DataFrame(data=npm_test_data_values,
                                          columns=['respondentid', 'household_id', 'person_id'] + test_demos)

        npm_recode_test_df['age1'] = 3

        npm_tv_view_usage_df = pd.DataFrame(columns=['respondentid', 'pc', 'min_viewed'])

        npm_tv_view_usage_df['household_id'] = np.array([1, 1, 1, 1, 2, 2, 2, 2, 3])
        npm_tv_view_usage_df['person_id'] = np.array([1, 1, 2, 2, 1, 1, 2, 2, 1])
        npm_tv_view_usage_df['reportable_network_name'] = ['a&', 'b', 'a!', 'c/', '&a', 'd', 'b', 'c', 'a&']
        npm_tv_view_usage_df['daypart'] = 1
        npm_tv_view_usage_df['minutes_viewed'] = [0, 1, 1, 1, 1, 0, 0, 2, 50]

        npm_tv_test_df = pd.DataFrame(columns=['respondentid', 'a1', 'b1', 'c1', 'd1'])
        npm_tv_test_df['respondentid'] = np.array([1001, 1002, 2001, 2002])
        npm_tv_test_df['a1'] = [0, 1, 1, 0]
        npm_tv_test_df['b1'] = [1, 0, 0, 0]
        npm_tv_test_df['c1'] = [0, 1, 0, 2]
        npm_tv_test_df['d1'] = [0, 0, 0, 0]

        npm_recode_sdf = self.spark.createDataFrame(npm_recode_test_df)
        npm_tv_view_usage_sdf = self.spark.createDataFrame(npm_tv_view_usage_df)

        logger.info('Running recoding')

        npm_tv_500_res_sdf, npm_tv_100_res_sdf = main_npm_usage_recode(npm_recode_sdf, npm_tv_view_usage_sdf)

        npm_tv_500_res_df = npm_tv_500_res_sdf.toPandas()

        logger.info('Asserting Results and Expectations are the same')

        self.assert_frame_equal_with_sort(npm_tv_500_res_df, npm_tv_test_df, 'respondentid')

        logger.info('SUCCESS')