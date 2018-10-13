import pandas as pd
import logging
from functools import reduce
from gc import collect


def create_internet_data_pandas(cph_donors_df, nol_demo, cph_daily_weights_df, funct_cc):
        """
        :param cph_donors_df:
        :param nol_demo:
        :param cph_daily_weights_df:
        :param funct_cc:
        :return:
        """

        nol_demo['surf_location_id'] = nol_demo['surf_location_id'].astype(int)
        nol_demo['gender_id'] = nol_demo['gender_id'].astype(int)
        nol_demo['hispanic_origin_id'] = nol_demo['hispanic_origin_id'].astype(int)
        nol_demo['race_id'] = nol_demo['race_id'].astype(int)
        cph_daily_weights_df['respondentid'] = cph_daily_weights_df['respondentid'].astype(int)
        cph_daily_weights_df['weight'] = cph_daily_weights_df['weight'].astype(float)

        nol_demo = nol_demo.rename(columns={'gender_id': 'gender', 'spanish_lang_dominance': 'spanish_language1'})
        nol_demo['hispanic'] = nol_demo['hispanic_origin_id'].isin([1, -1]).astype(int) + 1
        nol_demo['race_black'] = 2 - (nol_demo['race_id'] == 2).astype(int)

        nol_demo = funct_cc(nol_demo)

        indices = nol_demo['surf_location_id'] == 1

        internet_home = nol_demo.loc[indices, ['rn_id', 'cc', 'weight']].reset_index(drop=True)

        internet_work = nol_demo.loc[~indices, ['rn_id', 'cc', 'weight'] ].reset_index(drop=True)

        internet_cph = pd.merge(cph_donors_df[['respondentid', 'rn_id', 'cc']],
                                cph_daily_weights_df[['respondentid', 'weight']], on='respondentid',
                                how='left').fillna(0)

        ih_cc = internet_home.groupby('cc')['weight'].sum()
        cph_cc = internet_cph.groupby('cc')['weight'].sum()

        cph_adj = ih_cc.to_frame().join(cph_cc.to_frame(), lsuffix='_l', rsuffix='_r')

        cph_adj['adj'] = cph_adj['weight_l'].astype(float) / cph_adj['weight_r'].astype(float)

        internet_cph = internet_cph.set_index('cc').join(cph_adj['adj'].to_frame(),
                                                         how='inner').reset_index()

        internet_cph['weight'] = internet_cph['weight'].astype(float) * internet_cph['adj']
        internet_cph.drop('adj', axis=1, inplace=True)

        return internet_home, internet_work, internet_cph


def create_dcr_respondents_by_type_pandas(dcr_imps_cph, dcr_imps_h, dcr_imps_w, target_d, level_c, cph_media_space_df,
                                          home_media_space_df, work_media_space_df, aggr_site, dcr_site,
                                          dcr_cols, filter_col, site, site_sfx, dcr_site_name, min_lvl_cnt_c,
                                          create_dcr_impressions_by_site_id_funct):
    """
    :param dcr_imps_cph:
    :param dcr_imps_h:
    :param dcr_imps_w:
    :param target_d:
    :param level_c:
    :param cph_media_space_df:
    :param home_media_space_df:
    :param work_media_space_df:
    :param aggr_site:
    :param dcr_site:
    :param dcr_cols:
    :param filter_col:
    :param site:
    :param site_sfx:
    :param dcr_site_name:
    :param min_lvl_cnt_c:
    :param create_dcr_impressions_by_site_id_funct:
    :return:
    """

    dcr_cph_usage_df = pd.merge(cph_media_space_df, aggr_site[dcr_cols], on='rn_id')
    dcr_home_usage_df = pd.merge(home_media_space_df, aggr_site[dcr_cols], on='rn_id')
    dcr_work_usage_df = pd.merge(work_media_space_df, aggr_site[dcr_cols], on='rn_id')

    home_aggr_df = dcr_home_usage_df.loc[dcr_home_usage_df[filter_col] > 0, :].reset_index(drop=True)
    work_aggr_df = dcr_work_usage_df.loc[dcr_work_usage_df[filter_col] > 0, :].reset_index(drop=True)

    target_d_site = dcr_site.rename(columns={'computer': 'pc_target_imp',
                                             'unique_audience': 'pc_target_reach',
                                             'duration': 'pc_target_dur'})

    if site_sfx in ['vd', 'sv']:
        target_d_site['nv_h_reach'] = home_aggr_df['weight'].sum()
        target_d_site['nv_h_dur'] = home_aggr_df[['weight', 'minutes']].prod(axis=1).sum()
        target_d_site['nv_w_reach'] = work_aggr_df['weight'].sum()
        target_d_site['nv_w_dur'] = work_aggr_df[['weight', 'minutes']].prod(axis=1).sum()

        target_d_site['nv_panel_dur'] = target_d_site['nv_h_dur'] + target_d_site['nv_w_dur']
        target_d_site['dcr_dur_adj'] = target_d_site['pc_target_dur'] / target_d_site['nv_panel_dur']
        target_d_site['home_prop_reach'] = target_d_site['nv_h_reach'] / (
                target_d_site['nv_h_reach'] + target_d_site['nv_w_reach'])
        target_d_site['home_prop_dur'] = target_d_site['nv_h_dur'] / (target_d_site['nv_w_dur'] +
                                                                      target_d_site['nv_h_dur'])

        target_d_site = target_d_site.rename(columns={'nv_h_reach': 'nv_home_panel',
                                                      'nv_w_reach': 'nv_work_panel'})

        target_d_site = target_d_site[['name', 'site_id', 'pc_target_imp', 'pc_target_reach',
                                       'pc_target_dur', 'nv_home_panel', 'nv_work_panel', 'nv_panel_dur',
                                       'dcr_dur_adj', 'home_prop_reach', 'home_prop_dur']]

    else:
        target_d_site['nv_h_reach'] = home_aggr_df['weight'].sum()
        target_d_site['nv_h_pv'] = home_aggr_df[['weight', 'pageviews']].prod(axis=1).sum()
        target_d_site['nv_h_dur'] = home_aggr_df[['weight', 'viewduration']].prod(axis=1).sum()
        target_d_site['nv_w_reach'] = work_aggr_df['weight'].sum()
        target_d_site['nv_w_pv'] = work_aggr_df[['weight', 'pageviews']].prod(axis=1).sum()
        target_d_site['nv_w_dur'] = work_aggr_df[['weight', 'viewduration']].prod(axis=1).sum()

        target_d_site['nv_pc_panel'] = target_d_site['nv_h_pv'] + target_d_site['nv_w_pv']
        target_d_site['nv_pc_panel_dur'] = target_d_site['nv_h_dur'] + target_d_site['nv_w_dur']
        target_d_site['dcr_pv_adj'] = target_d_site['pc_target_imp'] / target_d_site['nv_pc_panel']
        target_d_site['dcr_dur_adj'] = target_d_site['pc_target_dur'] / target_d_site['nv_pc_panel_dur']
        target_d_site['home_prop_pv'] = target_d_site['nv_h_pv'] / (target_d_site['nv_h_pv'] +
                                                                    target_d_site['nv_w_pv'])
        target_d_site['home_prop_dur'] = target_d_site['nv_h_dur'] / (target_d_site['nv_w_dur'] +
                                                                      target_d_site['nv_h_dur'])
        target_d_site['home_prop_reach'] = target_d_site['nv_h_reach'] / (
                target_d_site['nv_w_reach'] + target_d_site['nv_h_reach'])

        target_d_site = target_d_site.rename(columns={'nv_h_pv': 'nv_pc_h_reach',
                                                      'nv_w_pv': 'nv_pc_w_reach'})

        target_d_site = target_d_site[['name', 'site_id', 'pc_target_imp', 'pc_target_reach',
                                       'pc_target_dur', 'nv_pc_h_reach', 'nv_pc_w_reach', 'nv_pc_panel',
                                       'nv_pc_panel_dur', 'dcr_pv_adj', 'dcr_dur_adj', 'home_prop_pv',
                                       'home_prop_dur', 'home_prop_reach']]

    dcr_cph, use_level = create_dcr_impressions_by_site_id_funct(dcr_cph_usage_df, dcr_site_name, 'pc', min_lvl_cnt_c,
                                                                 site_sfx)

    dcr_home = create_dcr_impressions_by_site_id_funct(home_aggr_df, dcr_site_name, 'pc', min_lvl_cnt_c, site_sfx,
                                                       use_level=use_level)

    dcr_work = create_dcr_impressions_by_site_id_funct(work_aggr_df, dcr_site_name, 'pc', min_lvl_cnt_c, site_sfx,
                                                       use_level=use_level)

    dcr_imps_cph = pd.merge(dcr_imps_cph, dcr_cph, on='rn_id', how='outer').fillna(0.0)
    dcr_imps_h = pd.merge(dcr_imps_h, dcr_home, on='rn_id', how='outer').fillna(0.0)
    dcr_imps_w = pd.merge(dcr_imps_w, dcr_work, on='rn_id', how='outer').fillna(0.0)
    level_c_site = pd.DataFrame(data=[use_level], columns=['use_level'])
    level_c_site['name'] = dcr_site['name']
    level_c_site['site_id'] = site

    target_d = pd.concat([target_d_site, target_d], sort=True, ignore_index=True, axis=0)
    level_c = pd.concat([level_c_site, level_c], sort=True, ignore_index=True, axis=0)

    return dcr_imps_cph, dcr_imps_h, dcr_imps_w, target_d, level_c


def create_dcr_respondents_pandas(internet_home, internet_work, internet_cph, dcr_brands_pc_text, dcr_brands_video,
                                  dcr_sub_brands_pc_text, dcr_sub_brands_video, strm_aggr_brand, strm_aggr_sub_brand,
                                  surf_aggr_brand, surf_aggr_sub_brand, nol_age_gender_df, create_dcr_resp_by_type_funct):
    """
    :param internet_home:
    :param internet_work:
    :param internet_cph:
    :param dcr_brands_pc_text:
    :param dcr_brands_video:
    :param dcr_sub_brands_pc_text:
    :param dcr_sub_brands_video:
    :param strm_aggr_brand:
    :param strm_aggr_sub_brand:
    :param surf_aggr_brand:
    :param surf_aggr_sub_brand:
    :param nol_age_gender_df:
    :param create_dcr_resp_by_type_funct:
    :return:
    """

    logger = logging.getLogger(__name__)

    logger.info('Building DCR impressions')

    logger.info('Build DCR PC By Brand')

    pc_br_imps_cph, pc_br_imps_h, pc_br_imps_w, \
    pc_level_br, pc_target_d_br = create_dcr_resp_by_type_funct(dcr_brands_pc_text, surf_aggr_brand,nol_age_gender_df,
                                                                internet_cph, internet_home, internet_work, 'br')
    collect()

    logger.info('Build DCR PC By SubBrand')

    pc_sb_imps_cph, pc_sb_imps_h, pc_sb_imps_w, \
    pc_level_sb, pc_target_d_sb = create_dcr_resp_by_type_funct(dcr_sub_brands_pc_text, surf_aggr_sub_brand,
                                                                nol_age_gender_df, internet_cph, internet_home,
                                                                internet_work, 'sb')

    collect()

    logger.info('Build DCR PCV By Brand')

    pcv_br_imps_cph, pcv_br_imps_h, pcv_br_imps_w, \
    pcv_level_br, pcv_target_d_br = create_dcr_resp_by_type_funct(dcr_brands_video, strm_aggr_brand,
                                                                  nol_age_gender_df, internet_cph,
                                                                  internet_home, internet_work, 'vd')

    collect()

    logger.info('Build DCR PCV By SubBrand')

    pcv_sb_imps_cph, pcv_sb_imps_h, pcv_sb_imps_w, \
    pcv_level_sb, pcv_target_d_sb = create_dcr_resp_by_type_funct(dcr_sub_brands_video, strm_aggr_sub_brand,
                                                                  nol_age_gender_df, internet_cph,
                                                                  internet_home, internet_work, 'sv')

    collect()

    logger.info('Merging the impressions together')

    imps_list = [pc_br_imps_cph, pc_sb_imps_cph, pcv_br_imps_cph, pcv_sb_imps_cph]

    ih_imps_list = [pc_br_imps_h, pc_sb_imps_h, pcv_br_imps_h, pcv_sb_imps_h]

    iw_imps_list = [pc_br_imps_w, pc_sb_imps_w, pcv_br_imps_w, pcv_sb_imps_w]

    impressions = reduce(lambda left, right: pd.merge(left, right, on='rn_id', how='outer'),
                         imps_list).fillna(0.0)

    impressions_home = reduce(lambda left, right: pd.merge(left, right, on='rn_id', how='outer'),
                              ih_imps_list).fillna(0.0)

    impressions_work = reduce(lambda left, right: pd.merge(left, right, on='rn_id', how='outer'),
                              iw_imps_list).fillna(0.0)

    logger.info('Setting up the DCR combined targets for PC and Video')

    levels_use_pc = pd.concat([pc_level_br, pc_level_sb], axis=0, sort=True).reset_index(drop=True)
    levels_use_pcv = pd.concat([pcv_level_br, pcv_level_sb], axis=0, sort=True).reset_index(drop=True)

    dcr_combine_targets_pc = pd.concat([pc_target_d_br, pc_target_d_sb], axis=0, sort=True).reset_index(drop=True)
    dcr_combine_targets_pcv = pd.concat([pcv_target_d_br, pcv_target_d_sb], axis=0, sort=True).reset_index(drop=True)

    max_dcr_adj = 4
    min_dcr_adj = 0.5

    indices = dcr_combine_targets_pc['dcr_pv_adj'] > max_dcr_adj

    dcr_combine_targets_pc.loc[indices, 'dcr_pv_adj'] = max_dcr_adj
    dcr_combine_targets_pc.loc[indices, 'pc_target_imp'] = dcr_combine_targets_pc.loc[
                                                               indices, 'nv_pc_panel'] * max_dcr_adj

    indices = dcr_combine_targets_pc['dcr_dur_adj'] > max_dcr_adj

    dcr_combine_targets_pc.loc[indices, 'dcr_dur_adj'] = max_dcr_adj
    dcr_combine_targets_pc.loc[indices, 'pc_target_dur'] = dcr_combine_targets_pc.loc[
                                                               indices, 'nv_pc_panel_dur'] * max_dcr_adj

    indices = (dcr_combine_targets_pc['dcr_dur_adj'] > min_dcr_adj) & (
                dcr_combine_targets_pc['dcr_pv_adj'] > min_dcr_adj)

    dcr_pc_df = pd.merge(pd.concat([dcr_brands_pc_text, dcr_sub_brands_pc_text], axis=0, sort=True).reset_index(
        drop=True), pd.merge(dcr_combine_targets_pc.loc[indices, :].drop('name', axis=1).reset_index(drop=True),
                             levels_use_pc, on='site_id').drop('name', axis=1), on='site_id')

    indices = dcr_combine_targets_pcv['dcr_dur_adj'] > max_dcr_adj

    dcr_combine_targets_pcv.loc[indices, 'dcr_dur_adj'] = max_dcr_adj
    dcr_combine_targets_pcv.loc[indices, 'pc_target_dur'] = dcr_combine_targets_pcv.loc[
                                                                indices, 'nv_panel_dur'] * max_dcr_adj

    indices = dcr_combine_targets_pcv['dcr_dur_adj'] > min_dcr_adj

    dcr_pcv_df = pd.merge(pd.concat([dcr_brands_video, dcr_sub_brands_video], axis=0, sort=True) \
                          .reset_index(drop=True),
                          pd.merge(dcr_combine_targets_pcv.loc[indices, :] \
                                   .drop('name', axis=1).reset_index(drop=True),
                                   levels_use_pcv, on='site_id').drop('name', axis=1),
                          on='site_id')

    dcr_pc_df = dcr_pc_df.rename(columns={'name': 'site'})
    dcr_pcv_df = dcr_pcv_df.rename(columns={'name': 'site'})

    logger.info('Sample size of impressions: {}'.format(impressions.shape[0]))

    softcal_reps = pd.merge(internet_cph[['respondentid', 'rn_id', 'cc', 'weight']], impressions,
                            on='rn_id', how='left').fillna(0.0)

    indices = softcal_reps['weight'] > 0.0

    softcal_reps = softcal_reps.loc[indices, :].reset_index(drop=True)

    softcal_reps['respondentid'] = softcal_reps['respondentid'].apply(lambda x: 'r' + str(int(x)))
    softcal_reps.drop('rn_id', axis=1, inplace=True)

    softcal_reps = softcal_reps.fillna(0.0)

    softcal_reps['cc'] = softcal_reps['cc'].astype(int)

    cols_keep = [col for col in softcal_reps.columns if 'p02' not in col]

    softcal_reps = softcal_reps.loc[:, cols_keep]

    return softcal_reps, impressions_home, impressions_work, dcr_pc_df, dcr_pcv_df, dcr_combine_targets_pc, \
           dcr_combine_targets_pcv
