import logging

import pandas as pd

from recoding.src.main.recoding import zip_state
from library.src.main.core import pivot_table


def recode_nol_usage_type(df, type_, num_top_sites=200):

    assert isinstance(df, pd.DataFrame)
    assert isinstance(type_, str)

    if not (type_ == 'brand_strm'):

        # rename a column and sort by duration in descending order
        df = df.rename(columns={
            type_ + '_id': 'tree_id',
            type_ + '_name': 'tree_name'
        })
    else:
        df = df.rename(columns={
            'brand_id': 'tree_id',
            'brand_name': 'tree_name'
        })

    top_sites_df = df.groupby('tree_name')['duration'].sum().reset_index(). \
        sort_values(by='duration', ascending=False).reset_index(drop=True)

    num_top_sites = min(top_sites_df.shape[0], num_top_sites) + 1

    top_sites_df = top_sites_df.loc[:num_top_sites, :].reset_index(drop=True)
    top_sites_df['rank_id'] = list(range(1, num_top_sites))

    df = df[['rn_id', 'tree_name', 'duration']]

    top_df = pd.merge(df,
                      top_sites_df[['tree_name', 'rank_id']],
                      on='tree_name', how='inner')

    def funct_temp(x):

        if x < 10:
            yn_root = '_yn00'
        elif x < 100:
            yn_root = '_yn0'
        else:
            yn_root = '_yn'

        return ''.join([type_, yn_root, str(x)])

    def funct_rm_yn(x):
        x_split = x.split('yn')

        return ''.join([x_split[0], str(int(x_split[1]))])

    # creates site variable
    top_df['site'] = top_df['rank_id'].astype(int).apply(funct_temp)

    group_cols = ['site', 'rn_id']

    top_df_grouped = top_df.groupby(group_cols)['duration'].sum().reset_index()
    top_df_grouped['site_raw'] = top_df_grouped['site_raw'].apply(funct_rm_yn)

    top_df = top_df.rename(columns={'tree_name': 'site_name'})

    # ensure missing duration values are set to zero
    top_df_grouped.loc[:, 'duration'] = top_df_grouped.loc[:, 'duration'].fillna(0.0)

    # we bin are data
    top_df_grouped['site_used'] = (top_df_grouped['duration'] > 0.0).astype(int)

    top_df_piv_bin = pivot_table(top_df_grouped, 'site_used', 'rn_id', 'site').to_dense().fillna(0.0)
    top_df_piv = pivot_table(top_df_grouped, 'duration', 'rn_id', 'site_raw').to_dense().fillna(0.0)

    top_site_usage_df = top_df_piv_bin.join(top_df_piv)

    return top_df, top_site_usage_df


def recode_nol_usage(us_200parents, us_200brands, us_200channels):
    """
    :param us_200parents:
    :param us_200brands:
    :param us_200channels:
    :return: top_200_df
    """

    assert isinstance(us_200parents, pd.DataFrame)
    assert isinstance(us_200brands, pd.DataFrame)
    assert isinstance(us_200channels, pd.DataFrame)

    logger = logging.getLogger(__name__)

    logger.info('Recoding NOL Usage Data')

    # this recodes the usage data
    top_parents_df, top_parents_site_usage_df = recode_nol_usage_type(us_200parents, 'parent')

    top_channels_df, top_channel_site_usage_df = recode_nol_usage_type(us_200channels, 'channel')

    top_brands_df, top_brand_site_usage_df = recode_nol_usage_type(us_200brands, 'brand')

    top_200_nol_site_df = top_parents_site_usage_df.join(top_channel_site_usage_df.join(top_brand_site_usage_df,
                                                                                        how='outer'), how='outer')\
        .fillna(0.0).reset_index().rename(columns={'index': 'rn_id'})

    top_parents_df['fused_variable'] = top_parents_df['rank_id'].apply(lambda x: 'parent_yn' + str(x))

    top_channels_df['fused_variable'] = top_channels_df['rank_id'].apply(lambda x: 'channel_yn' + str(x))

    top_brands_df['fused_variable'] = top_brands_df['rank_id'].apply(lambda x: 'brand_yn' + str(x))

    return top_200_nol_site_df


def recode_nol_demos(nol_df, nol_sample_df):
    """
    :param nol_df:
    :param nol_sample_df:
    :return:
    """

    assert isinstance(nol_df, pd.DataFrame)
    assert isinstance(nol_sample_df, pd.DataFrame)

    demographics_phx = pd.merge(nol_df, nol_sample_df.loc[~(nol_sample_df['surf_location_id'] == 9), :]\
                                .drop('surf_location_id', axis=1), on='rn_id')

    # time to create home
    indices = (demographics_phx['gender_id'].astype(int) == -1) | (demographics_phx['age'].isnull()) \
              | (~(demographics_phx['surf_location_id'] == 1))

    home_df = demographics_phx.loc[~indices, :].reset_index(drop=True)

    # truth values are 1 in int, 0 as false

    # work access
    indices = home_df['web_access_locations'].isin([2, 3, 6, 7, 10, 11, 14, 15] & (~home_df['working_status_id']
                                                                                   .isin([-2, 7, 8])))

    home_df['work_access'] = 2 - indices.astype(int)

    # edu
    indices = home_df['education_id'].isin(list(range(5, 8)))

    home_df['edu'] = 2 - indices.astype(int)

    # races
    indices = home_df['race_id'] == 2

    home_df['race1'] = 2 - indices.astype(int)

    indices = home_df['race_id'] == 3

    home_df['race2'] = 2 - indices.astype(int)

    # income
    home_df['inc'] = home_df['income_group_id']

    indices = home_df['inc'] == -1
    home_df.loc[indices, 'inc'] = 4

    # hispanic
    indices = home_df['hispanic_origin_id'].isin([1, -1])

    home_df['hispanicorigin'] = 2 - indices.astype(int)

    # occupation
    home_df['occ'] = 4

    home_df.loc[home_df['occupation_id'].isin([3, 4, 7, 10]), 'occ'] = 1
    home_df.loc[home_df['occupation_id'].isin([1, 8, -1]), 'occ'] = 2
    home_df.loc[home_df['occupation_id'].isin([2, 6, 5, 9, -13, 11]), 'occ'] = 3

    # number of children
    indices = home_df['members_2_11_count'] == 0
    home_df['children1'] = 2 - indices.astype(int)

    indices = home_df['members_12_17_count'] == 0
    home_df['children2'] = 2 - indices.astype(int)

    # geography

    home_df = zip_state(home_df, 'zip_code', 'state')

    home_df['terr'] = 6

    home_df.loc[home_df['state'].isin(['CT', 'DE', 'DC', 'ME', 'MD', 'MA', 'NH', 'NJ', 'NY', 'PA', 'RI', 'VT']), 'terr'] = 1
    home_df.loc[home_df['state'].isin(['IN', 'KY', 'MI', 'OH', 'WV']), 'terr'] = 2
    home_df.loc[home_df['state'].isin(['CO', 'IL', 'IA', 'KS', 'MN', 'MO', 'MT', 'NE', 'ND', 'SD', 'WI', 'WY']), 'terr'] = 3
    home_df.loc[home_df['state'].isin(['AL', 'FL', 'GA', 'MS', 'NC', 'SC', 'TN', 'VA']), 'terr'] = 4
    home_df.loc[home_df['state'].isin(['AR', 'LA', 'NM', 'OK', 'TX']), 'terr'] = 5

    home_terr_dummies = pd.get_dummies(home_df['terr'].astype(object))
    home_terr_dummies.columns = ['terr' + str(col) for col in home_terr_dummies.columns]

    home_terr_dummies.drop('terr6', axis=1, inplace=True)

    # web speed
    indices = home_df['web_conn_speed_id'] <= 5

    home_df['speed'] = 2 - indices.astype(int)

    home_df['cs'] = home_df['county_size_id'].fillna(1)
    home_df['gender'] = home_df['gender_id']

    home_df['age1'] = 9

    indices = home_df['age'] < 65
    home_df.loc[indices, 'age1'] = 8

    indices = home_df['age'] < 55
    home_df.loc[indices, 'age1'] = 7

    indices = home_df['age'] < 45
    home_df.loc[indices, 'age1'] = 6

    indices = home_df['age'] < 35
    home_df.loc[indices, 'age1'] = 5

    indices = home_df['age'] < 25
    home_df.loc[indices, 'age1'] = 4

    indices = home_df['age'] < 18
    home_df.loc[indices, 'age1'] = 3

    indices = home_df['age'] < 12
    home_df.loc[indices, 'age1'] = 2

    indices = home_df['age'] < 6
    home_df.loc[indices, 'age1'] = 1

    cols_keep = ['weight', 'rn_id', 'edu', 'race1', 'race2', 'inc', 'hispanicorigin',
                 'occ', 'children1', 'children2', 'cs', 'age1', 'speed', 'gender']

    return pd.concat([home_df[cols_keep], home_terr_dummies], axis=1)
