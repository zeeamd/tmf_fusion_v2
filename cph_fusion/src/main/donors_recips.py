import pandas as pd
import numpy as np

import logging


def donors_recips_pandas(npm_df, nol_df, hhp_nol_df, top_500_tv_df, top_200_df):
    """
    :rtype: pd.DataFrame
    :param npm_df:
    :param nol_df:
    :param hhp_nol_df:
    :param top_500_tv_df:
    :param top_200_df:
    :return:
    """
    logger = logging.getLogger(__name__)

    assert (isinstance(npm_df, pd.DataFrame))
    assert (isinstance(nol_df, pd.DataFrame))
    assert (isinstance(top_500_tv_df, pd.DataFrame))
    assert (isinstance(top_200_df, pd.DataFrame))

    # ensure columns are intended types
    top_500_tv_df['respondentid'] = top_500_tv_df['respondentid'].astype(np.int64)
    npm_df['internet'] = npm_df['internet'].astype(np.int64)
    hhp_nol_df['respondentid'] = hhp_nol_df['respondentid'].astype(np.int64)

    nol_rn_dtype = nol_df['rn_id'].dtype
    hhp_nol_df['rn_id'] = hhp_nol_df['rn_id'].astype(nol_rn_dtype)

    logger.info("Merging NPM Demographics data with NPM TV Usage data")

    # we only want NPM data with internet and with person age greater than 2
    indices = (npm_df['internet'] == 1) & (2 <= npm_df['age'])

    npm_universe_df = npm_df.loc[indices, :].reset_index(drop=True).merge(top_500_tv_df, on='respondentid', how='left')

    npm_universe_df[top_500_tv_df.columns] = npm_universe_df[top_500_tv_df.columns].fillna(0.0)

    logger.info("Merging NPM Demographics with mapping of respondentids and rn_id's")

    npm_nol_universe_sdf = pd.merge(npm_universe_df, hhp_nol_df[['respondentid', 'rn_id']], on='respondentid',
                                    how='left')

    npm_nol_universe_sdf['rn_id'] = npm_nol_universe_sdf['rn_id'].fillna(-1.0)

    logger.info("Determining the online weight of each respondent")

    donors_recips = pd.merge(npm_nol_universe_sdf,
                             nol_df[['rn_id', 'weight']].rename(columns={'weight': 'onl_weight'}),
                             on='rn_id', how='left')

    donors_recips['onl_weight'] = donors_recips['onl_weight'].fillna(0.0)

    donor_indices = donors_recips['onl_weight'] > 0.0
    recip_indices = donors_recips['onl_weight'] <= 0.0

    logger.info("Creating the donors and recipient datasets based on having an online-weight or not")

    donors_df = donors_recips.loc[donor_indices, :].reset_index(drop=True)

    cph_recips_df = donors_recips.loc[recip_indices, :].reset_index(drop=True).drop('rn_id', axis=1)

    cph_donors_df = pd.merge(donors_df, top_200_df, on='rn_id', how='left')

    cph_donors_df[top_200_df.columns] = cph_donors_df[top_200_df.columns].fillna(0.0)

    # this renames a few columns to what is expected later on in the code
    cph_donors_df = cph_donors_df.rename(columns={'hh_size1_by_count': 'hh_size1',
                                                  'kids_0to5_by_count': 'kids_0to5',
                                                  'kids_6to11_by_count': 'kids_6to11',
                                                  'kids_12to17_by_count': 'kids_12to17'})

    cph_recips_df = cph_recips_df.rename(columns={'hh_size1_by_count': 'hh_size1',
                                                  'kids_0to5_by_count': 'kids_0to5',
                                                  'kids_6to11_by_count': 'kids_6to11',
                                                  'kids_12to17_by_count': 'kids_12to17'})

    return cph_donors_df, cph_recips_df


def donors_recips_spark(npm_sdf, nol_sdf, hhp_nol_sdf, top_500_tv_sdf, top_200_nol_site_sdf):
    """
    :param npm_sdf:
    :param nol_sdf:
    :param hhp_nol_sdf:
    :param top_500_tv_sdf:
    :param top_200_nol_site_sdf:
    :return:
    """
    npm_universe_sdf = npm_sdf.join(top_500_tv_sdf,
                                    'respondentid', how='left').fillna(0, subset=list(top_500_tv_sdf.columns))\
        .filter('internet == 1').filter('2 <= age')

    npm_nol_universe_sdf = npm_universe_sdf.join(hhp_nol_sdf, 'respondentid', how='left').fillna(-1, subset='rn_id')\
    .join(nol_sdf.select(['rn_id', 'weight']).withColumnRenamed('weight', 'onl_weight'), 'rn_id', how='left')\
    .fillna(0, subset='onl_weight')

    donor_universe_sdf = npm_nol_universe_sdf.filter('0 < onl_weight')
    recip_sdf = npm_nol_universe_sdf.filter('0 == onl_weight').drop('rn_id')

    donor_sdf = donor_universe_sdf.join(top_200_nol_site_sdf, 'rn_id',
                                        how='left').fillna(0, subset=list(top_200_nol_site_sdf.columns))

    return donor_sdf, recip_sdf.drop('rn_id')