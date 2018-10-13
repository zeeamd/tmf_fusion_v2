import pandas as pd
import numpy as np

from .core import pivot_table


def create_dcr_targets_pandas(dcr_pc_df, dcr_pcv_df, dcr_pc_combine_targets, dcr_pcv_combine_targets, tot_univ,
                              dcr_target, dcr_target_sub, dcr_vid):
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

    def split_str(x):
        return str(x).split('_')[0]

    dcr_target['name'] = dcr_target['Target'].apply(split_str)

    dcr_target_sub['name'] = dcr_target_sub['Target'].apply(split_str)

    dcr_vid['name'] = dcr_vid['Target'].apply(split_str)

    # concatenates the target and target_sub data
    dcr_target2 = pd.concat([dcr_target, dcr_target_sub], axis=0, sort=True).reset_index(drop=True)

    dcr_target2 = dcr_target2.rename(columns={col: col.lower() for col in dcr_target2.columns})
    dcr_vid = dcr_vid.rename(columns={col: col.lower() for col in dcr_vid.columns})

    dcr_pc_df['use_level'] = dcr_pc_df['use_level'].astype(int)
    dcr_pcv_df['use_level'] = dcr_pcv_df['use_level'].astype(int)

    dcr_pc_df['dummy'] = 1
    dcr_pcv_df['dummy'] = 1

    # this helps restrict our attention to pairs of name ans level that are in the dcr_pc dataframe
    join1 = dcr_target2.set_index('brandid').join(
        dcr_pc_combine_targets.set_index('site_id')[['prop_reach', 'prop_dur', 'prop_pv']]
        , how='inner').reset_index(drop=True)

    dcr_targets_sub = join1.set_index(['name', 'level']).join(
        dcr_pc_df.set_index(['site', 'use_level'])['dummy'].to_frame().rename_axis(['name', 'level']), how='inner') \
        .rename(columns={'target': 'var'})

    # drop every less than 18
    indices = dcr_targets_sub['var'].apply(lambda x: 'p02' not in x)

    dcr_targets_sub = dcr_targets_sub.loc[indices, :].reset_index(drop=True)

    # sets up reach, duration, imp and code variables
    dcr_targets_sub['reach'] = dcr_targets_sub['reach'] * dcr_targets_sub['prop_reach']

    dcr_targets_sub['dur'] = dcr_targets_sub['duration'] * dcr_targets_sub['prop_dur']

    dcr_targets_sub['imp'] = dcr_targets_sub['impression'] * dcr_targets_sub['prop_pv']

    dcr_targets_sub['code'] = 1

    dcr_targets_sub = dcr_targets_sub[['var', 'code', 'imp', 'reach', 'dur']]

    dcr_targets_sub_d = dcr_targets_sub.copy()

    dcr_targets_sub.drop('dur', axis=1, inplace=True)

    dcr_targets_sub_d['var'] = dcr_targets_sub_d['var'].apply(lambda x: x.replace('_pv_', '_mm_'))

    dcr_targets_sub_d['imp'] = dcr_targets_sub_d['dur'] * 60

    dcr_targets_sub_d.drop('dur', axis=1, inplace=True)

    dcr_targets_sub2 = dcr_targets_sub.copy()
    dcr_targets_sub2['reach'] = tot_univ - dcr_targets_sub2['reach']
    dcr_targets_sub2['imp'] = 0
    dcr_targets_sub2['code'] = 0

    dcr_targets_sub2d = dcr_targets_sub_d.copy()
    dcr_targets_sub2d['reach'] = tot_univ - dcr_targets_sub2d['reach']
    dcr_targets_sub2d['imp'] = 0
    dcr_targets_sub2d['code'] = 0

    # this helps restrict our attention to pairs of name ans level that are in the dcr_pc dataframe
    join1 = dcr_vid.set_index('brandid').join(
        dcr_pcv_combine_targets.set_index('site_id')[['prop_reach', 'prop_dur']]
        , how='inner').reset_index(drop=True)

    dcr_targets_sub_vid = join1.set_index(['name', 'level']).join(
        dcr_pcv_df.set_index(['site', 'use_level'])['dummy'].to_frame() \
            .rename_axis(['name', 'level']), how='inner').rename(columns={'target': 'var'})

    indices = dcr_targets_sub_vid['var'].apply(lambda x: 'p02' not in x)
    dcr_targets_sub_vid = dcr_targets_sub_vid.loc[indices, :].reset_index(drop=True)

    dcr_targets_sub_vid['imp'] = dcr_targets_sub_vid['duration'] * dcr_targets_sub_vid['prop_dur']
    dcr_targets_sub_vid['reach'] = dcr_targets_sub_vid['reach'] * dcr_targets_sub_vid['prop_reach']
    dcr_targets_sub_vid['code'] = 1

    dcr_targets_sub_vid2 = dcr_targets_sub_vid.copy()

    dcr_targets_sub_vid2['reach'] = tot_univ - dcr_targets_sub_vid2['reach']
    dcr_targets_sub_vid2['imp'] = 0
    dcr_targets_sub_vid2['code'] = 0

    targets = pd.concat([dcr_targets_sub, dcr_targets_sub2, dcr_targets_sub_d, dcr_targets_sub2d,
                         dcr_targets_sub_vid, dcr_targets_sub_vid2], axis=0, sort=True).reset_index(drop=True) \
        .dropna(axis=1)

    return targets


def create_demo_var_by_site_id_pandas(input_df, level, name, media_type, site_sfx):
    """
    :param input_df:
    :param level:
    :param name:
    :param media_type:
    :param site_sfx:
    :return:
    """
    if level == 8:
        age_bins = ['0212', '1317', '1820', '2124', '2529', '3034', '3539', '4044', '4549', '5054', '5564', '6599']

    if level == 7:
        age_bins = ['0217', '1824', '2534', '3544', '4554', '5564', '6599']

    if level == 6:
        age_bins = ['0217', '1834', '3554', '5599']

    if level == 5:
        age_bins = ['0217', '1844', '4599']

    if level == 4:
        age_bins = ['0217', '1899']

    if level == 3:
        age_bins = ['0212', '1399']

    if level == 2:
        age_bins = ['0217', 'p1899']

    if level == 1:
        age_bins = ['0212', 'p1399']

    if level == 0:
        age_bins = ['total']

    # drop all columns if demo is in the name
    cols_demo = [col for col in input_df.columns if 'demo' in col and not ('male' in col or 'female' in col)]

    # reverses pivot table of indicators of ages and genders
    output_df = pd.melt(input_df, id_vars=['rn_id'],
                        value_vars=cols_demo,
                        var_name='demo_var',
                        value_name='indicator'
                        )

    # this removes site_ids and rn_ids with no one in a particular bin
    output_df = output_df.loc[output_df['indicator'] == 1, :].reset_index(drop=True) \
        .drop('indicator', axis=1)

    if site_sfx in ['vd', 'sv']:
        # now we recover the columns of interest
        output_df = pd.merge(output_df, input_df[['rn_id', 'minutes']],
                             on=['rn_id'])

        # now we create a dummy column to help us create the proper bins
        output_df['var'] = output_df['demo_var'].apply(
            lambda x: '_'.join([name, media_type, site_sfx, x.replace('demo', 'mm')]))

        indices = output_df['var'].apply(lambda x: np.any([age_bin in x for age_bin in age_bins]))

        res_df = pivot_table(output_df.loc[indices, :].groupby(['rn_id', 'var'])['minutes'].sum() \
                                            .reset_index(), 'minutes', 'rn_id', 'var').reset_index() \
            .rename(columns={'index': 'rn_id'}).to_dense()
    else:
        output_df = pd.merge(output_df, input_df[['rn_id', 'pageviews', 'viewduration']], on=['rn_id'])

        output_df['var'] = output_df['demo_var'].apply(lambda x: '_'.join([name, media_type, site_sfx,
                                                                           x.replace('demo', 'pv')]))

        output_df['var2'] = output_df['demo_var'].apply(lambda x: '_'.join([name, media_type, site_sfx,
                                                                            x.replace('demo', 'mm')]))
        indices = output_df['var'].apply(lambda x: np.any([age_bin in x for age_bin in age_bins]))

        pv_df = pivot_table(
            output_df.loc[indices, :].groupby(['rn_id', 'var'])['pageviews'].sum().reset_index(),
            'pageviews', 'rn_id', 'var').reset_index().rename(columns={'index': 'rn_id'}).to_dense()

        indices = output_df['var2'].apply(lambda x: np.any([age_bin in x for age_bin in age_bins]))

        mm_df = pivot_table(
            output_df.loc[indices, :].groupby(['rn_id', 'var2'])['viewduration'].sum().reset_index(),
            'viewduration', 'rn_id', 'var2').reset_index().rename(
            columns={'index': 'rn_id'}).to_dense()

        res_df = pd.merge(pv_df, mm_df, on='rn_id', how='outer').fillna(0.0)

        if media_type == 'pc':
            pass
        else:
            output_df['dummy_var'] = '_'.join([name, 'pcmob', site_sfx, 'dup'])

            if 'pageviews_mob' in output_df.columns:
                output_df['pageviews_mob'] = output_df['pageviews_mob'].fillna(0.0)
            else:
                output_df['pageviews_mob'] = 0

            output_df['dummy_var_ind'] = ((output_df['pageviews_mob'] > 0) & (output_df['pageviews'] > 0)).astype(
                int)

            indices = output_df['dummy_var'].apply(lambda x: np.any([age_bin in x for age_bin in age_bins]))

            temp_piv = pivot_table(
                output_df.loc[indices, :].groupby(['rn_id', 'dummy_var'])['dummy_var_ind'] \
                    .sum().reset_index(), 'dummy_var_ind', 'rn_id', 'dummy_var').reset_index() \
                .rename(columns={'index': 'rn_id'}).to_dense()

            res_df = pd.merge(temp_piv, res_df, on='rn_id')

    res_df['rn_id'] = res_df['rn_id'].astype(float)

    return res_df


def dcr_use_level(dcr_demos, media_usage_df, min_lvl_cnt_c):
    """
    :param dcr_demos:
    :param media_usage_df:
    :param min_lvl_cnt_c:
    :return:
    """
    breaks_df = dcr_demos.loc[dcr_demos['check'] == 1, ['name', 'level']].reset_index(drop=True)

    break_names = list(breaks_df['name'].values)

    cols = break_names

    breaks_c = pd.melt(media_usage_df.loc[:, cols], id_vars=['site_id'],
                       value_vars=break_names,
                       var_name='break',
                       value_name='indicator')

    breaks_c = breaks_c.loc[breaks_c['indicator'] == 1, :].reset_index(drop=True)

    breaks_c = breaks_c.groupby('break')['indicator'] \
        .sum().reset_index().rename(columns={'indicator': 'count'})

    breaks_c = pd.merge(breaks_c.rename(columns={'break': 'name'}), breaks_df[['name', 'level']], on='name')

    level_c = breaks_c.groupby('level')['count'].min().reset_index()

    for i in range(9):
        ind = level_c['level'] == 8 - i

        if min_lvl_cnt_c < level_c.loc[ind, 'count'].values:
            return 8 - i

    return 99
