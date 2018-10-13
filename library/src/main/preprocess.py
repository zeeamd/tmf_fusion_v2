# -*- coding: utf-8 -*-
"""
Created on Thu Aug  9 22:26:29 2018

@author: tued7001
"""
import logging
from abc import ABCMeta, abstractmethod

import pandas as pd

from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession

from .calibration.soft_calibration import soft_calibrate
from .dcr_functions import create_dcr_targets_pandas, create_demo_var_by_site_id_pandas, dcr_use_level


class Preprocess(object, metaclass=ABCMeta):
    
    def __init__(self, spark, loc_adj_fact, tot_adj_fact, estimate_huber):

        self._logger = logging.getLogger(__name__)

        self.spark = spark
        self.donors = spark.createDataFrame(pd.DataFrame())
        self.recips = spark.createDataFrame(pd.DataFrame())
        self.old_links = pd.DataFrame()
        self.linking_variables_master = []
        self.linking_variables_pc = []
        self.linking_variables_se_init = []
        self.linking_variables_se_final = []
        self.linking_variables_iw = []
        self.iw_rec_vars = []
        self.iw_don_vars = []
        self.soft_cal_targets = pd.DataFrame()
        self.soft_cal_impress = pd.DataFrame()

        assert(isinstance(loc_adj_fact, int) | isinstance(loc_adj_fact, float))
        assert(isinstance(tot_adj_fact, int) | isinstance(tot_adj_fact, float))
        assert(isinstance(estimate_huber, bool))

        self.soft_cal_params = {'loc_adj_fact': loc_adj_fact, 'tot_adj_fact': tot_adj_fact,
                                'estimate_huber': estimate_huber}
        return None

    @property
    def spark(self):
        return self._spark

    @spark.setter
    def spark(self, spark_sess):
        assert isinstance(spark_sess, SparkSession)

        self._spark = spark_sess

    @property
    def donors(self):
        return self._donors

    @donors.setter
    def donors(self, sdf):
        assert isinstance(sdf, DataFrame) | isinstance(sdf, pd.DataFrame)

        self._donors = sdf

    @property
    def recips(self):
        return self._recips

    @recips.setter
    def recips(self, sdf):
        assert isinstance(sdf, DataFrame) | isinstance(sdf, pd.DataFrame)

        self._recips = sdf

    @property
    def soft_cal_targets(self):
        return self._soft_cal_targets

    @soft_cal_targets.setter
    def soft_cal_targets(self, sdf):
        assert isinstance(sdf, DataFrame) | isinstance(sdf, pd.DataFrame)

        self._soft_cal_targets = sdf

    @property
    def soft_cal_impress(self):
        return self._soft_cal_impress

    @soft_cal_impress.setter
    def soft_cal_impress(self, sdf):
        assert isinstance(sdf, DataFrame) | isinstance(sdf, pd.DataFrame)

        self._soft_cal_impress = sdf

    @property
    def old_links(self):
        return self._old_links

    @old_links.setter
    def old_links(self, sdf):
        assert isinstance(sdf, DataFrame) | isinstance(sdf, pd.DataFrame)

        self._old_links = sdf

    @property
    def linking_variables_master(self):
        return self._linking_vars_mas

    @linking_variables_master.setter
    def linking_variables_master(self, val):
        assert (isinstance(val, list))

        self._linking_vars_mas = val

    @property
    def linking_variables_pc(self):
        return self._linking_vars_pc

    @linking_variables_pc.setter
    def linking_variables_pc(self, val):
        assert (isinstance(val, list))

        self._linking_vars_pc = val

    @property
    def linking_variables_se_init(self):
        return self._linking_vars_se_init

    @linking_variables_se_init.setter
    def linking_variables_se_init(self, val):
        assert (isinstance(val, list))

        self._linking_vars_se_init = val

    @property
    def linking_variables_se_final(self):
        return self._linking_vars_se_final

    @linking_variables_se_final.setter
    def linking_variables_se_final(self, val):
        assert (isinstance(val, list))

        self._linking_vars_se_final = val

    @property
    def linking_variables_iw(self):
        return self._linking_vars_iw

    @linking_variables_iw.setter
    def linking_variables_iw(self, val):
        assert (isinstance(val, list))

        self._linking_vars_iw = val

    @property
    def iw_rec_vars(self):
        return self._iw_rec_vars

    @iw_rec_vars.setter
    def iw_rec_vars(self, val):
        assert isinstance(val, list)

        self._iw_rec_vars = val

    @property
    def iw_don_vars(self):
        return self._iw_don_vars

    @iw_don_vars.setter
    def iw_don_vars(self, val):
        assert isinstance(val, list)

        self._iw_don_vars = val

    @abstractmethod
    def create_donors_recips(self, **kwargs):
        pass

    @abstractmethod
    def create_soft_cal_targets(self, **kwargs):
        pass

    @abstractmethod
    def update_donor_recips(self, **kwargs):
        return self

    @staticmethod
    @abstractmethod
    def setup_dcr_demos():
        pass

    @staticmethod
    @abstractmethod
    def create_cc_entries(df):
        pass

    @staticmethod
    def create_age_gender_demos(demos):
        """
        :param demos:
        :return:
        """
        assert isinstance(demos, pd.DataFrame)

        p_lower_bounds = ['02', '02', '13', '18']
        p_upper_bounds = ['12', '17', '99', '99']

        g_lower_bounds_1 = ['13', '18', '21', '25', '30', '35', '40', '45', '50', '55', '65']
        g_upper_bounds_1 = ['17', '20', '24', '29', '34', '39', '44', '49', '54', '64', '99']

        g_lower_bounds_2 = ['13', '18', '18', '18', '18', '25', '35', '35', '45', '45', '55']
        g_upper_bounds_2 = ['99', '24', '34', '44', '99', '34', '44', '54', '54', '99', '99']

        g_lower_bounds = g_lower_bounds_1 + g_lower_bounds_2
        g_upper_bounds = g_upper_bounds_1 + g_upper_bounds_2

        n = len(g_lower_bounds)

        demo_males = ['demo_m' + g_lower_bounds[i] + g_upper_bounds[i] for i in range(n)]
        demo_females = ['demo_f' + g_lower_bounds[i] + g_upper_bounds[i] for i in range(n)]
        demo_person = ['demo_p' + p_lower_bounds[i] + p_upper_bounds[i] for i in range(4)]

        for i in range(4):
            demos[demo_person[i]] = 0

        demos['demo_male'] = 0
        demos['demo_female'] = 0
        demos['demo_total'] = 1

        # now time to deal with male values only
        indices = demos['gender'] == 1

        demos.loc[indices, 'demo_male'] = 1

        for i in range(n):
            # creates the new columns and initializes it with the value 0
            demos[demo_males[i]] = 0

            l_bound = int(g_lower_bounds[i])
            u_bound = int(g_upper_bounds[i])

            demos.loc[indices & ((demos['age'] <= u_bound) & (l_bound <= demos['age'])),
                      demo_males[i]] = 1

        # now time to deal with female values only
        indices = demos['gender'] == 2

        demos.loc[indices, 'demo_female'] = 1

        for i in range(n):
            # creates the new columns and initializes it with the value 0
            demos[demo_females[i]] = 0

            l_bound = int(g_lower_bounds[i])
            u_bound = int(g_upper_bounds[i])

            demos.loc[indices & ((demos['age'] <= u_bound) & (l_bound <= demos['age'])),
                      demo_females[i]] = 1

        # now we just look at person in this grouping
        for i in range(4):
            demos[demo_person[i]] = 0

            l_bound = int(p_lower_bounds[i])
            u_bound = int(p_upper_bounds[i])

            demos.loc[((demos['age'] <= u_bound) & (l_bound <= demos['age'])),
                      demo_person[i]] = 1

        return demos

    @staticmethod
    def create_dcr_targets(dcr_pc_df, dcr_pcv_df, dcr_pc_combine_targets, dcr_pcv_combine_targets, tot_univ, dcr_target,
                           dcr_target_sub, dcr_vid):
        """
        :param dcr_pc_df:
        :param dcr_pcv_df:
        :param dcr_pc_combine_targets:
        :param dcr_pcv_combine_targets:
        :param tot_univ:
        :param dcr_target:
        :param dcr_target_sub:
        :param dcr_vid:
        :return:
        """

        assert isinstance(dcr_pc_df, pd.DataFrame)
        assert isinstance(dcr_pcv_df, pd.DataFrame)
        assert isinstance(dcr_pc_combine_targets, pd.DataFrame)
        assert isinstance(dcr_pcv_combine_targets, pd.DataFrame)
        assert isinstance(dcr_target, pd.DataFrame)
        assert isinstance(dcr_target_sub, pd.DataFrame)
        assert isinstance(dcr_vid, pd.DataFrame)
        assert isinstance(tot_univ, float)

        return create_dcr_targets_pandas(dcr_pc_df, dcr_pcv_df, dcr_pc_combine_targets, dcr_pcv_combine_targets,
                                         tot_univ, dcr_target, dcr_target_sub, dcr_vid)

    def use_level_by_site_id(self, media_usage_df, min_lvl_cnt_c):
        """
        :param media_usage_df:
        :param min_lvl_cnt_c:
        :return:
        """
        logger = self._logger

        logger.debug('Setting up DCR Demos')

        assert isinstance(media_usage_df, pd.DataFrame)
        assert isinstance(min_lvl_cnt_c, int) | isinstance(min_lvl_cnt_c, float)

        # we get the DCR Demos
        dcr_demos = self.setup_dcr_demos()

        return dcr_use_level(dcr_demos, media_usage_df, min_lvl_cnt_c)

    def create_demo_var_by_site_id(self, input_df, level, name, media_type, site_sfx):

        assert isinstance(input_df, pd.DataFrame)
        assert isinstance(level, int)
        assert isinstance(name, str)
        assert isinstance(media_type, str)
        assert isinstance(site_sfx, str)

        logger = self._logger

        if level > 8:
            logger.warning('Usage level is higher than tolerance')
            return pd.DataFrame(columns=['rn_id'])

        else:
            return create_demo_var_by_site_id_pandas(input_df, level, name, media_type, site_sfx)

    def create_dcr_impressions_by_site_id(self, dcr_usage_df, dcr_site_name, media_type, min_lvl_cnt_c,
                                          site_sfx, use_level=None):
        """
        :param dcr_usage_df:
        :param dcr_site_name:
        :param media_type:
        :param min_lvl_cnt_c:
        :param site_sfx:
        :param use_level:
        :return:
        """
        logger = self._logger

        if use_level is None:
            use_level = self.use_level_by_site_id(dcr_usage_df, min_lvl_cnt_c)
            logger.info('Usage Level is: {}'.format(use_level))
            dcr_imps = self.create_demo_var_by_site_id(dcr_usage_df, use_level, dcr_site_name, media_type, site_sfx)
            return dcr_imps, use_level
        else:
            dcr_imps = self.create_demo_var_by_site_id(dcr_usage_df, use_level, dcr_site_name, media_type, site_sfx)
            return dcr_imps

    def calibrate_weights(self, df, id_col):
        """
        :param df:
        :param id_col:
        :return:
        """
        assert(isinstance(id_col, str))

        base_logger = self._logger

        base_logger.info("Performing soft calibration")

        soft_cal_res = soft_calibrate(self.soft_cal_targets, self.soft_cal_impress,
                                      loc_adj_fact=self.soft_cal_params.get('loc_adj_fact'),
                                      tot_adj_fact=self.soft_cal_params.get('tot_adj_fact'),
                                      estimate_huber=self.soft_cal_params.get('estimate_huber'))
    
        base_logger.info('Updating weights')
        df['unitLabel'] = df[id_col].apply(lambda x: 'r' + str(x))
        
        df = pd.merge(df, soft_cal_res[['unitLabel', 'xf']] .rename(columns={'xf': 'new_weight'}), on='unitLabel',
                      how='left')

        no_new_weight = df['new_weight'].isnull()
        
        df.loc[~no_new_weight, 'weight'] = df.loc[~no_new_weight, 'new_weight']
        
        df.drop(['new_weight', 'unitLabel'], axis=1, inplace=True)
        
        return df
    
    def calibrate_donor_weights(self):
        """
        :return:
        """

        base_logger = self._logger

        base_logger.info("Calibrating our donor weights")

        self.donors = self.calibrate_weights(self.donors, 'respondentid')
        
        return self
    
    def calibrate_recip_weights(self):
        """
        :return:
        """

        base_logger = self._logger

        base_logger.info("Calibrating our recipient weights")

        self.recips = self.calibrate_weights(self.recips, 'rn_id')
        
        return self

    def determine_old_linkage(self, prev_donors, prev_recips, prev_linkages):
        """
        :param prev_donors:
        :param prev_recips:
        :param prev_linkages:
        :return:
        """
        
        assert(isinstance(prev_donors, pd.DataFrame))
        assert(isinstance(prev_recips, pd.DataFrame))
        assert(isinstance(prev_linkages, pd.DataFrame))

        donors = self.donors
        recips = self.recips
        
        old_donors = prev_donors.rename(columns={'respondentid': 'donorid', 'cc': 'cc_donor_prev'})
    
        old_recips = prev_recips.rename(columns={'respondentid': 'recipientid', 'cc': 'cc_recip_prev'})
    
        cur_donors = donors.rename(columns = {'respondentid': 'donorid', 'cc': 'cc_donor'})
    
        cur_recips = recips.rename(columns = {'respondentid': 'recipientid', 'cc': 'cc_recip'})

        old_links = pd.merge(pd.merge(prev_linkages, old_donors, on='donorid'), old_recips, on='recipientid')
    
        old_links = pd.merge(pd.merge( old_links, cur_donors, on='donorid'), cur_recips, on='recipientid' )

        same_donor_indices = old_links['cc_donor'] == old_links['cc_donor_prev']
        same_recip_indices = old_links['cc_recip'] == old_links['cc_recip_prev']

        indices = same_donor_indices & same_recip_indices
    
        self.old_links = old_links.loc[indices, :].reset_index(drop=True).drop(['cc_donor', 'cc_recip', 'cc_donor_prev',
                                                                                'cc_recip_prev'], axis=1)

        return self
