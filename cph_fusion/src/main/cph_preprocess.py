# -*- coding: utf-8 -*-
"""
Created on Fri Aug 10 12:48:15 2018

@author: tued7001
"""
#python standard library
import logging

#data management
import pandas as pd

from pyspark.sql.dataframe import DataFrame
from pyspark.ml.feature import Bucketizer
from pyspark.ml import Pipeline

#in-house methods
from library.src.main.preprocess import Preprocess
from library.src.main.core import CCTransformer
from .donors_recips import donors_recips_pandas, donors_recips_spark
from .soft_cal_respondent import create_internet_data_pandas, create_dcr_respondents_by_type_pandas, \
    create_dcr_respondents_pandas


class CPHPreprocess(Preprocess):

    def __init__(self, spark):

        Preprocess.__init__(self, spark, 4, 20, True)

        self._logger = logging.getLogger(__name__)

        self.prev_cc_master_donor = pd.DataFrame()
        self.prev_cc_master_recip = pd.DataFrame()
        self.impressions_home = pd.DataFrame()
        self.impressions_work = pd.DataFrame()
        self.top_100_tv_network_dp = {}
        self.dcr_targets_dict = {}

    @property
    def top_100_tv_network_dp(self):
        return self._top_100_tv_network_dp

    @top_100_tv_network_dp.setter
    def top_100_tv_network_dp(self, val_dict):

        assert(isinstance(val_dict, dict))

        self._top_100_tv_network_dp=val_dict

    @property
    def prev_cc_master_donor(self):
        return self._prev_cc_master_donor

    @prev_cc_master_donor.setter
    def prev_cc_master_donor(self, df):
        assert isinstance(df, pd.DataFrame) | isinstance(df, DataFrame)
        self._prev_cc_master_donor = df

    @property
    def prev_cc_master_recip(self):
        return self._prev_cc_master_recip

    @prev_cc_master_recip.setter
    def prev_cc_master_recip(self, df):
        assert isinstance(df, pd.DataFrame)
        self._prev_cc_master_recip=df


    @property
    def dcr_targets_dict( self ):
        return self._dcr_targ_dict
    
    @dcr_targets_dict.setter
    def dcr_targets_dict( self, df_dict):

        assert isinstance(df_dict, dict)

        self._dcr_targ_dict = df_dict
        
    @property
    def impressions_home( self ):
        return self.impress_home

    @impressions_home.setter
    def impressions_home(self, df):
        assert isinstance(df, pd.DataFrame) | isinstance(df, DataFrame)
        self.impress_home = df
    
    @property
    def impressions_work( self ):
        return self.impress_work

    @impressions_work.setter
    def impressions_work(self, df):
        assert isinstance(df, pd.DataFrame) | isinstance(df, DataFrame)

        self.impress_work = df

    @staticmethod
    def create_cc_entries(df):
        """
        :param df:
        :return:
        """

        assert isinstance(df, DataFrame)

        pipeline = Pipeline(stages=[
            Bucketizer(splits=[0, 2, 6, 13, 18, 25, 35, 45, 55, 65, 150], inputCol='age', outputCol="age_bin"),
            CCTransformer(cc_iter=9, inputCols=['age_bin', 'gender', 'hispanic', 'race_black', 'spanish_language1'],
                          outputCol='cc')
        ])

        model = pipeline.fit(df)

        return model.transform(df).drop('age_bin')

    @staticmethod
    def setup_dcr_demos():
        data=[['0', 'demo_total', '1'], ['1', 'demo_p0212', '0'], ['1', 'demo_p1399', '1'],
                ['2', 'demo_p0217', '0'], ['2', 'demo_p1899', '1'], ['3', 'demo_p0212', '0'],
                ['3', 'demo_m1399', '1'], ['3', 'demo_f1399', '1'], ['4', 'demo_p0217', '0'],
                ['4', 'demo_m1899', '1'], ['4', 'demo_f1899', '1'], ['5', 'demo_p0217', '0'],
                ['5', 'demo_m1844', '1'], ['5', 'demo_m4599', '1'], ['5', 'demo_f1844', '1'],
                ['5', 'demo_f4599', '1'], ['6', 'demo_p0217', '0'], ['6', 'demo_m1834', '1'],
                ['6', 'demo_m3554', '1'], ['6', 'demo_m5599', '1'], ['6', 'demo_f1834', '1'],
                ['6', 'demo_f3554', '1'], ['6', 'demo_f5599', '1'], ['7', 'demo_p0217', '0'],
                ['7', 'demo_m1824', '1'], ['7', 'demo_m2534', '1'], ['7', 'demo_m3544', '1'],
                ['7', 'demo_m4554', '1'], ['7', 'demo_m5564', '1'], ['7', 'demo_m6599', '1'],
                ['7', 'demo_f1824', '1'], ['7', 'demo_f2534', '1'], ['7', 'demo_f4554', '1'],
                ['7', 'demo_f5564', '1'], ['7', 'demo_f6599', '1'], ['8', 'demo_p0212', '1'],
                ['8', 'demo_m1317', '1'], ['8', 'demo_m1820', '1'], ['8', 'demo_m2124', '1'],
                ['8', 'demo_m2529', '1'], ['8', 'demo_m3034', '1'], ['8', 'demo_m3539', '1'],
                ['8', 'demo_m4044', '1'], ['8', 'demo_m4549', '1'], ['8', 'demo_m5054', '1'],
                ['8', 'demo_m5564', '1'], ['8', 'demo_m6599', '1'], ['8', 'demo_f1317', '1'],
                ['8', 'demo_f1820', '1'], ['8', 'demo_f2124', '1'], ['8', 'demo_f2529', '1'],
                ['8', 'demo_f3034', '1'], ['8', 'demo_f3539', '1'], ['8', 'demo_f4044', '1'],
                ['8', 'demo_f4549', '1'], ['8', 'demo_f5054', '1'], ['8', 'demo_f5564', '1'],
                ['8', 'demo_f6599', '1']]

        df=pd.DataFrame(data=data, columns=['level', 'name', 'check'])

        df['level'] = df['level'].astype(int)
        df['check'] = df['check'].astype(int)

        return df

    def _create_dcr_respondents_by_type(self, dcr_df, aggr_df, nol_df, internet_cph,  internet_home, internet_work,
                                        site_sfx, min_cnt_dcr_h=5, min_cnt_dcr_w=5,  min_lvl_cnt_c=10):
        """
        :param dcr_df:
        :param aggr_df:
        :param nol_df:
        :param internet_cph:
        :param internet_home:
        :param internet_work:
        :param site_sfx:
        :param min_cnt_dcr_h:
        :param min_cnt_dcr_w:
        :param min_lvl_cnt_c:
        :return:
        """
        logger = self._logger

        cph_media_space_df = pd.merge(internet_cph[['rn_id', 'weight']], nol_df, on='rn_id')
        home_media_space_df = pd.merge(internet_home[['rn_id', 'weight']], nol_df, on='rn_id')
        work_media_space_df = pd.merge(internet_work[['rn_id', 'weight']], nol_df, on='rn_id')

        if site_sfx in ['vd', 'br']:
            aggr_df = aggr_df.rename(columns={'brandid': 'site_id'})
        else:
            aggr_df = aggr_df.rename(columns={'channelid': 'site_id'})

        if site_sfx in ['vd', 'sv']:
            aggr_df['minutes'] = aggr_df['viewduration'] / 60.0
            dcr_cols = ['rn_id', 'site_id', 'minutes']
            filter_col = 'minutes'
        else:
            dcr_cols = ['rn_id', 'site_id', 'pageviews', 'viewduration']
            filter_col = 'pageviews'

        dcr_df['site_id'] = dcr_df['site_id'].astype(int)
        aggr_df['site_id'] = aggr_df['site_id'].astype(int)

        site_lists = list(set(dcr_df['site_id'].unique().tolist()) & set(aggr_df['site_id'].unique().tolist()))

        dcr_imps_cph = pd.DataFrame(columns=['rn_id'])
        dcr_imps_h = pd.DataFrame(columns=['rn_id'])
        dcr_imps_w = pd.DataFrame(columns=['rn_id'])
        level_c = pd.DataFrame()
        target_d = pd.DataFrame()

        for site in site_lists:
            dcr_site = dcr_df.loc[dcr_df['site_id'] == site, :].reset_index(drop=True)
            aggr_site = aggr_df.loc[aggr_df['site_id'] == site, :].reset_index(drop=True)
            dcr_site_name = dcr_site.loc[0, 'name']

            logger.info('Creating impressions for site: {}'.format(dcr_site_name))

            if aggr_site['surf_location_id'].nunique() < 2:
                logger.warning('Not enough respondent data')
                pass

            else:
                aggr_surf_tots = aggr_df['surf_location_id'].value_counts()

                if (aggr_surf_tots.loc[1] > min_cnt_dcr_h) & (aggr_surf_tots.loc[2] > min_cnt_dcr_w):
                    dcr_imps_cph, dcr_imps_h, dcr_imps_w, target_d, \
                    level_c = create_dcr_respondents_by_type_pandas(dcr_imps_cph, dcr_imps_h, dcr_imps_w, target_d,
                                                                    level_c, cph_media_space_df, home_media_space_df,
                                                                    work_media_space_df, aggr_site, dcr_site, dcr_cols,
                                                                    filter_col, site, site_sfx, dcr_site_name,
                                                                    min_lvl_cnt_c,
                                                                    self.create_dcr_impressions_by_site_id)
                else:
                    pass

        return dcr_imps_cph, dcr_imps_h, dcr_imps_w, level_c, target_d

    def create_donors_recips(self, npm_df, nol_df, hhp_nol_df, top_tv_500_sdf, top_200_df, tv_100):
        """
        :param npm_df:
        :param nol_df:
        :param hhp_nol_df:
        :param top_tv_500_sdf:
        :param top_200_df:
        :param tv_100:
        :return:
        """

        assert isinstance(npm_df, pd.DataFrame) | isinstance(npm_df, DataFrame)
        assert isinstance(nol_df, pd.DataFrame) | isinstance(nol_df, DataFrame)
        assert isinstance(top_tv_500_sdf, pd.DataFrame) | isinstance(top_tv_500_sdf, DataFrame)
        assert isinstance(top_200_df, pd.DataFrame) | isinstance(top_200_df, DataFrame)
        assert isinstance(tv_100, list)

        cph_logger = self._logger

        cph_logger.info('Adding critical cell information to NPM Sample')

        if (isinstance(npm_df, DataFrame) & isinstance(nol_df, DataFrame)
                & isinstance(top_tv_500_sdf, DataFrame) & isinstance(top_200_df, DataFrame)):

            npm_cc_df = self.create_cc_entries(npm_df)

            cph_donors_df, cph_recips_df = donors_recips_spark(npm_cc_df, nol_df, hhp_nol_df, top_tv_500_sdf,
                                                               top_200_df)

            self.donors = cph_donors_df
            self.recips = cph_recips_df

        elif (isinstance(npm_df, pd.DataFrame) & isinstance(nol_df, pd.DataFrame)
              & isinstance(top_tv_500_sdf, pd.DataFrame) & isinstance(top_200_df, pd.DataFrame)):

            cph_logger.info('Adding critical cell information to NPM Sample')

            npm_cc_df = self.create_cc_entries(self.spark.createDataFrame(npm_df)).toPandas()

            cph_donors_df, cph_recips_df = donors_recips_pandas(npm_cc_df, nol_df, hhp_nol_df, top_tv_500_sdf,
                                                                top_200_df)

            self.donors = cph_donors_df
            self.recips = cph_recips_df

        else:
            cph_logger.critical('Not all the inputs are the same type')
            cph_logger.critical('Setting all donors and recips as empty')

            self.donors = pd.DataFrame()
            self.recips = pd.DataFrame()

            return self

        cph_logger.info('Setting up linking variables for configuration workbook for PyFusion Application')

        self.linking_variables_pc = [col.lower() for col in top_tv_500_sdf.columns if not col == 'respondentid']

        self.linking_variables_master = list(map(str.lower, ['Age_bydob', 'Age0', 'Age1', 'cableplus', 'dvr',
                                                             'education7', 'employment1', 'gender', 'HDTV', 'hh_size1',
                                                             'hispanic', 'income9', 'kids_0to5', 'kids_12to17',
                                                             'kids_6to11', 'number_of_tvs1', 'Occupation1', 'Paycable',
                                                             'race_asian', 'race_black', 'RegionB', 'Video_Game',
                                                             'Satellite', 'Spanish_language1']))

        self.linking_variables_master += self.linking_variables_pc

        self.linking_variables_se_init = list(map(str.lower, ['Age1', 'cableplus', 'dvr', 'education7', 'employment1',
                                                              'gender', 'HDTV', 'hh_size1', 'hispanic', 'income9',
                                                              'kids_0to5', 'kids_12to17', 'kids_6to11',
                                                              'number_of_tvs1', 'Occupation1', 'Paycable', 'race_asian',
                                                              'race_black', 'RegionB', 'Video_Game', 'Satellite',
                                                              'Spanish_language1']))

        self.linking_variables_se_final = list(map(str.lower, ['Age1', 'cableplus', 'dvr', 'education7', 'employment1',
                                                               'HDTV', 'hh_size1', 'income9', 'kids_0to5',
                                                               'kids_12to17', 'kids_6to11', 'number_of_tvs1',
                                                               'Occupation1', 'Paycable', 'race_asian', 'RegionB',
                                                               'Video_Game', 'Satellite', 'Spanish_language1']))

        self.linking_variables_pc = ['age0'] + self.linking_variables_se_final

        self.iw_rec_vars = tv_100
        self.iw_don_vars = ['_'.join(['parent', str(i)]) for i in range(1, 101)]

        return self
    
    def create_soft_cal_targets(self, nol_df, cph_daily_weights, dcr_text, dcr_vid, dcr_sub_text, dcr_sub_vid,
                                dcr_target, dcr_target_sub, dcr_vid_target, strm_brand_df, strm_sub_brand_df,
                                surf_brand_df, surf_sub_brand_df):
        """
        :param nol_df:
        :param cph_daily_weights:
        :param dcr_text:
        :param dcr_vid:
        :param dcr_sub_text:
        :param dcr_sub_vid:
        :param dcr_target:
        :param dcr_target_sub:
        :param dcr_vid_target:
        :param strm_brand_df:
        :param strm_sub_brand_df:
        :param surf_brand_df:
        :param surf_sub_brand_df:
        :return:
        """

        cph_donors_df = self.donors
        
        assert(isinstance(nol_df, pd.DataFrame))
        assert(isinstance(cph_daily_weights, pd.DataFrame))
        assert(isinstance(dcr_text, pd.DataFrame))
        assert(isinstance(dcr_vid, pd.DataFrame))
        assert(isinstance(dcr_sub_text, pd.DataFrame))
        assert(isinstance(dcr_sub_vid, pd.DataFrame))
        assert(isinstance(dcr_target, pd.DataFrame))
        assert(isinstance(dcr_target_sub, pd.DataFrame))
        assert(isinstance(dcr_vid_target, pd.DataFrame))
        assert(isinstance(strm_brand_df, pd.DataFrame))
        assert(isinstance(strm_sub_brand_df, pd.DataFrame))
        assert(isinstance(surf_brand_df, pd.DataFrame))
        assert(isinstance(surf_sub_brand_df, pd.DataFrame))

        cph_logger = self._logger

        strm_brand_df['surf_location_id'] = strm_brand_df['surf_location_id'].astype(int)
        strm_sub_brand_df['surf_location_id'] = strm_sub_brand_df['surf_location_id'].astype(int)
        surf_brand_df['surf_location_id'] = surf_brand_df['surf_location_id'].astype(int)
        surf_sub_brand_df['surf_location_id'] = surf_sub_brand_df['surf_location_id'].astype(int)

        strm_brand_df['parentid'] = strm_brand_df['parentid'].astype(int)
        strm_sub_brand_df['parentid'] = strm_sub_brand_df['parentid'].astype(int)
        surf_brand_df['parentid'] = surf_brand_df['parentid'].astype(int)
        surf_sub_brand_df['parentid'] = surf_sub_brand_df['parentid'].astype(int)

        strm_brand_df['brandid'] = strm_brand_df['brandid'].astype(int)
        strm_sub_brand_df['brandid'] = strm_sub_brand_df['brandid'].astype(int)
        surf_brand_df['brandid'] = surf_brand_df['brandid'].astype(int)
        surf_sub_brand_df['brandid'] = surf_sub_brand_df['brandid'].astype(int)

        strm_sub_brand_df['channelid'] = strm_sub_brand_df['channelid'].astype(int)
        surf_sub_brand_df['channelid'] = surf_sub_brand_df['channelid'].astype(int)

        cph_logger.info('Determining internet usage by type and critical cell')

        internet_home, internet_work, internet_cph = create_internet_data_pandas(cph_donors_df, nol_df,
                                                                                 cph_daily_weights,
                                                                                 self.create_cc_entries)

        cph_logger.info('Number of donor respondents as soft calibrated candidates: {}'.format(internet_cph.shape[0]))

        cph_logger.info("Creating age-gender demographics variables")

        nol_age_gender_df = self.create_age_gender_demos(nol_df[['rn_id', 'age', 'gender_id']]\
                                                 .rename(columns={'gender_id': 'gender'}).copy())
    
        cph_logger.info("Determining the soft calibration targets")

        donors = pd.merge(cph_donors_df[['respondentid', 'cc']], internet_cph[['respondentid', 'weight']],
                          on='respondentid', how='left').fillna(0.0)

        cph_logger.info("Creating cc targets")

        cc_targets = donors.groupby('cc')['weight'].sum().reset_index() \
            .rename(columns={'weight': 'reach', 'cc': 'code'})

        cc_targets['var'] = 'cc'
        cc_targets['imp'] = 0

        tot_univ = internet_cph['weight'].sum()

        softcal_resp, impressions_home, impressions_work, dcr_pc_df, dcr_pcv_df, dcr_pc_combine_targets, \
        dcr_pcv_combine_targets = create_dcr_respondents_pandas(internet_home, internet_work, internet_cph, dcr_text,
                                                                dcr_vid, dcr_sub_text, dcr_sub_vid, strm_brand_df,
                                                                strm_sub_brand_df, surf_brand_df, surf_sub_brand_df,
                                                                nol_age_gender_df, self._create_dcr_respondents_by_type)

        cph_logger.info("Determining the DCR targets")

        dcr_pc_combine_targets_temp = dcr_pc_combine_targets.rename(columns={'home_prop_reach': 'prop_reach',
                                                                             'home_prop_dur': 'prop_dur',
                                                                             'home_prop_pv': 'prop_pv'})

        dcr_pcv_combine_targets_temp = dcr_pcv_combine_targets.rename(columns={'home_prop_reach': 'prop_reach',
                                                                               'home_prop_dur': 'prop_dur'})

        dcr_targets = self.create_dcr_targets(dcr_pc_df, dcr_pcv_df, dcr_pc_combine_targets_temp,
                                              dcr_pcv_combine_targets_temp, tot_univ, dcr_target, dcr_target_sub,
                                              dcr_vid_target)

        cols = ['var', 'imp', 'reach', 'code']

        softcal_targ = pd.concat([dcr_targets[cols], cc_targets[cols]], axis=0, sort=True).reset_index(drop=True)

        softcal_targ['code'] = softcal_targ['code'].astype(int)

        self.soft_cal_targets = softcal_targ
        self.soft_cal_impress = softcal_resp
        
        self.impressions_home = impressions_home
        self.impressions_work = impressions_work
        
        self.dcr_targets_dict = {'dcr_pc': dcr_pc_df, 'dcr_pcv': dcr_pcv_df, 'dcr_combine_pc': dcr_pc_combine_targets,
                                 'dcr_combine_pcv': dcr_pcv_combine_targets,  'dcr_target': dcr_targets,
                                 'dcr_target_sub': dcr_target_sub}
        
        return self

    def update_donor_recips(self, prev_donors, prev_recips, prev_linkage):
        """
        :param prev_donors:
        :param prev_recips:
        :param prev_linkage:
        :return:
        """
        self.determine_old_linkage(prev_donors, prev_recips, prev_linkage)

        donors = self.donors
        recips = self.recips

        self.prev_cc_master_donor = donors.loc[donors['respondentid'].isin(self.old_links['donorid']),:].reset_index(drop=True)
        self.prev_cc_master_recip = recips.loc[recips['respondentid'].isin(self.old_links['recipientid']), :]\
            .reset_index(drop=True)

        return self
