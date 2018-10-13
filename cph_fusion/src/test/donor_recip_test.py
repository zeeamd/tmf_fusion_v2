import logging
from time import time

import pandas as pd
import numpy as np

from cph_fusion.src.main.donors_recips import donors_recips_spark, donors_recips_pandas

from library.src.main.pyspark_test_framework import PySparkTest


class CPHDonorRecipTest(PySparkTest):

    def test_donor_recip_spark(self):

        logger = logging.getLogger(__name__)

        test_top_200_nol_site_df = pd.DataFrame(columns=['rn_id'])

        test_top_200_nol_site_df['rn_id'] = [1, 2, 3, 4, 5]
        test_top_200_nol_site_df['dummy_2'] = [0, 1, 1, 1, 2]
        test_top_200_nol_site_df['dummy_1'] = [0, 2, 2, 2, 0]
        test_top_200_nol_site_df['dummy_3'] = [2, 0, 0, 0, 0]

        test_top_200_nol_site_df['dummy_yn002'] = [0, 1, 1, 1, 1]
        test_top_200_nol_site_df['dummy_yn001'] = [0, 1, 1, 1, 0]
        test_top_200_nol_site_df['dummy_yn003'] = [1, 0, 0, 0, 0]

        test_npm_tv_usage_df = pd.DataFrame(columns=['respondentid', 'a1', 'b1', 'c1', 'd1'])
        test_npm_tv_usage_df['respondentid'] = np.array([1001, 1002, 2001, 2002])
        test_npm_tv_usage_df['a1'] = [0, 1, 1, 0]
        test_npm_tv_usage_df['b1'] = [1, 0, 0, 0]
        test_npm_tv_usage_df['c1'] = [0, 1, 0, 2]
        test_npm_tv_usage_df['d1'] = [0, 0, 0, 0]

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

        test_demos = ['hispanic', 'gender', 'race_back', 'dvr', 'cableplus', 'video_game', 'internet', 'paycable',
                      'hdtv', 'satellite', 'race_asian', 'occupation1', 'employment1', 'county_size',
                      'spanish_language1', 'kids_0to5', 'kids_6to11', 'kids_12to17', 'hh_size1', 'number_of_tvs1',
                      'education2', 'education7', 'income1', 'income9']

        test_npm_recode_df = pd.DataFrame(data=npm_test_data_values,
                                          columns=['respondentid', 'household_id', 'person_id'] + test_demos)

        test_npm_recode_df['weight'] = np.ones((test_npm_recode_df.shape[0],))
        test_npm_recode_df['age'] = 3

        test_nol_demo_df = pd.DataFrame(columns=['rn_id', 'surf_location_id', 'weight', 'age', 'gender_id',
                                                 'race_id', 'web_access_locations', 'education_id', 'income_id',
                                                 'occupation_id', 'members_2_11_count', 'members_12_17_count',
                                                 'zip_code',
                                                 'county_size_id', 'hispanic_origin_id', 'working_status_id',
                                                 'web_conn_speed_id'])

        test_nol_demo_df['rn_id'] = [1, 2, 3, 4, 5]
        test_nol_demo_df['surf_location_id'] = [1, 1, 1, 2, 1]
        test_nol_demo_df['weight'] = [0, 5, 2, 4, 6]
        test_nol_demo_df['age'] = [2, 10, 16, 25, 50]
        test_nol_demo_df['gender_id'] = [1, 1, 1, 2, 1]
        test_nol_demo_df['race_id'] = [3, 2, 1, 1, 1]
        test_nol_demo_df['income_id'] = [-1, 2, 3, -1, 1]
        test_nol_demo_df['web_access_locations'] = [2, 3, 6, -1, 11]
        test_nol_demo_df['working_status_id'] = [-2, 1, 8, 1, 11]
        test_nol_demo_df['members_2_11_count'] = [0, 4, 8, 0, 11]
        test_nol_demo_df['members_12_17_count'] = [1, 0, 0, 0, 2]
        test_nol_demo_df['education_id'] = [5, 8, 7, 0, 2]
        test_nol_demo_df['hispanic_origin_id'] = [-1, 1, 1, 2, 2]
        test_nol_demo_df['occupation_id'] = [3, 4, 1, 8, 2]
        test_nol_demo_df['web_conn_speed_id'] = [1, 6, 1, 4, 1]

        test_hhp_nol_df = pd.DataFrame(data=[[1001, 1],
                                             [1002, 2],
                                             [3001, 3],
                                             [3002, 4],
                                             [3003, 5]], columns=['respondentid', 'rn_id'])

        test_donors_df, test_recips_df = donors_recips_pandas(test_npm_recode_df, test_nol_demo_df, test_hhp_nol_df,
                                                              test_npm_tv_usage_df, test_top_200_nol_site_df)

#        test_donors_df = pd.DataFrame(columns=['respondentid', 'rn_id'])
#        test_recips_df = pd.DataFrame(columns=['respondentid'])

#        test_donors_df['respondentid'] = []

        logger.info('Converting all necessary datasets into Spark Dataframes')

        npm_sdf = self.spark.createDataFrame(test_npm_recode_df)
        top_500_tv_sdf = self.spark.createDataFrame(test_npm_tv_usage_df)
        nol_sdf = self.spark.createDataFrame(test_nol_demo_df)
        top_200_nol_site_sdf = self.spark.createDataFrame(test_top_200_nol_site_df)
        hhp_nol_sdf = self.spark.createDataFrame(test_hhp_nol_df)

        start = time()

        res_donors_sdf, res_recips_sdf = donors_recips_spark(npm_sdf, nol_sdf, hhp_nol_sdf, top_500_tv_sdf,
                                                             top_200_nol_site_sdf)

        res_donors_df = res_donors_sdf.toPandas()
        res_recips_df = res_recips_sdf.toPandas()

        end = time() - start

        logger.info('It took {} seconds'.format(end))

        donors_cols_to_test = test_donors_df.columns
        recips_cols_to_test = test_recips_df.columns

        logger.info('Changing numeric types to float')

        for col in donors_cols_to_test:
            res_donors_df[col] = res_donors_df[col].astype(float)
            test_donors_df[col] = test_donors_df[col].astype(float)

        for col in recips_cols_to_test:
            res_recips_df[col] = res_recips_df[col].astype(float)
            test_recips_df[col] = test_recips_df[col].astype(float)

        self.assert_frame_equal_with_sort(res_donors_df[donors_cols_to_test], test_donors_df[donors_cols_to_test],
                                          'respondentid')
        self.assert_frame_equal_with_sort(res_recips_df[recips_cols_to_test], test_recips_df[recips_cols_to_test],
                                          'respondentid')