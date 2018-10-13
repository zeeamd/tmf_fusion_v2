import pandas as pd
import numpy as np
import string
import logging

from library.src.main.utils.configure_s3_session import pandas_read_s3


# this is where we break down our targets
def target_break_down(data_in):
    indices = data_in["brand_name"].apply(lambda x: 'preview' in str.lower(x))

    data_in = data_in.loc[~indices, :].reset_index(drop=True)

    data_in['Counts'] = data_in.groupby(['brand_name'])['brand_id'].transform('count')
    data_in = data_in.loc[data_in["Counts"] == 26, :].reset_index(drop=True)

    # data_in = data_in[data_in['num'] == 26]
    platform = data_in.loc[0, 'platform']
    content = data_in.loc[0, 'content']

    data_in_dedup = data_in.drop_duplicates(['brand_id'])

    brands = data_in_dedup['brand_name'].unique()
    brand_ids = data_in_dedup['brand_id'].unique()

    # brands = data_in.brand_name.unique()
    # Remove 'MSN/Outlook/Bing/Skype'
    if 'MSN/Outlook/Bing/Skype' in brands:
        brands = np.delete(brands, [i for i, s in enumerate(brands) if s == 'MSN/Outlook/Bing/Skype'][0])

    # Remove special characters from brand names and create target names
    demos = ['total',
             'p0212', 'p1399',
             'p0217', 'p1899',
             'p0212', 'm1399', 'f1399',
             'p0217', 'm1899', 'f1899',
             'p0217', 'm1844', 'm4599', 'f1844', 'f4599',
             'p0217', 'm1834', 'm3554', 'm5599', 'f1834', 'f3554', 'f5599',
             'p0217', 'm1824', 'm2534', 'm3544', 'm4554', 'm5564', 'm6599',
             'f1824', 'f2534', 'f3544', 'f4554', 'f5564', 'f6599',
             'p0212', 'm1317', 'm1820', 'm2124', 'm2529', 'm3034', 'm3539', 'm4044', 'm4549', 'm5054', 'm5564', 'm6599',
             'f1317', 'f1820', 'f2124', 'f2529', 'f3034', 'f3539', 'f4044', 'f4549', 'f5054', 'f5564', 'f6599']
    df_out = pd.DataFrame(data=np.zeros((len(demos) * len(brands), 8)),
                          columns=['Target', 'Reach', 'Impression', 'Duration',
                                   'Code', 'Level', 'BrandId', 'Demo'])

    num_levels = 59
    df_out['Code'] = 1
    df_out['Level'] = ([0] + [1] * 2 + [2] * 2 + [3] * 3 + [4] * 3 + [5] * 5 + [6] * 7 + [7] * 13 + [8] * 23) * len(
        brands)
    #                   1     2       4       6      9        12      17      24      37
    target_names = []
    target_ids = []
    target_demos = []
    for i in range(0, len(brands)):
        brand_name = ''.join(e for e in brands[i] if e.isalnum()).lower()[0:15]
        #        print(i)
        #        print(brand_name)
        brand_id = brand_ids[i]
        #        print(brand_id)
        if content == 'Text':
            temp2 = [brand_id for demo in demos]
            temp3 = [demo for demo in demos]
            if platform == 'Desktop':
                temp = [brand_name + '_pc_br_pv_' + demo for demo in demos]
            elif platform == 'Mobile':
                temp = [brand_name + '_mob_br_pv_' + demo for demo in demos]
            else:
                temp = []
        elif content == 'Video':
            temp2 = [brand_id for demo in demos]
            temp3 = [demo for demo in demos]
            if platform == 'Desktop':
                temp = [brand_name + '_pc_vd_mm_' + demo for demo in demos]
            elif platform == 'Mobile':
                temp = [brand_name + '_mob_vd_mm_' + demo for demo in demos]
            else:
                temp = []
        else:
            temp = []
            temp2 = []
            temp3 = []

        target_names.extend(temp)
        target_ids.extend(temp2)
        target_demos.extend(temp3)
    #    target_names.extend(brand_ids)
    df_out['Target'] = target_names
    df_out['BrandId'] = target_ids
    df_out['Demo'] = target_demos

    # eventually figure out how to change this
    # this is actually ugly
    for i in range(0, len(brands)):
        temp = data_in.loc[data_in['brand_name'] == brands[i], ['reportable_demo_id', 'unique_audience', 'impressions',
                                                                'duration']]
        for j in range(1, 4):
            df_out.iloc[num_levels * i, j] = float(temp[temp['reportable_demo_id'] == 30000].iloc[:, j])
            df_out.iloc[[num_levels * i + 1, num_levels * i + 5, num_levels * i + 36], j] = float(
                temp[temp['reportable_demo_id'] == 20000].iloc[:, j])
            df_out.iloc[num_levels * i + 2, j] = sum(
                temp[(temp['reportable_demo_id'] == 1999) | (temp['reportable_demo_id'] == 2999)].iloc[:, j])
            df_out.iloc[[num_levels * i + 3, num_levels * i + 8, num_levels * i + 11, num_levels * i + 16,
                         num_levels * i + 23], j] = sum(temp[(temp['reportable_demo_id'] == 20000) | (
                    (temp['reportable_demo_id'] == 1000) | (temp['reportable_demo_id'] == 2000))].iloc[:, j])
            df_out.iloc[num_levels * i + 4, j] = sum(
                temp[((temp['reportable_demo_id'] >= 2003) & (temp['reportable_demo_id'] <= 2012)) |
                     ((temp['reportable_demo_id'] >= 1003) & (temp['reportable_demo_id'] <= 1012))].iloc[:, j])
            df_out.iloc[num_levels * i + 6, j] = float(temp[temp['reportable_demo_id'] == 2999].iloc[:, j])

            df_out.iloc[num_levels * i + 7, j] = float(temp[temp['reportable_demo_id'] == 1999].iloc[:, j])
            df_out.iloc[num_levels * i + 9, j] = sum(
                temp[(temp['reportable_demo_id'] >= 2003) & (temp['reportable_demo_id'] <= 2012)].iloc[:, j])
            df_out.iloc[num_levels * i + 10, j] = sum(
                temp[(temp['reportable_demo_id'] >= 1003) & (temp['reportable_demo_id'] <= 1012)].iloc[:, j])
            df_out.iloc[num_levels * i + 12, j] = sum(
                temp[(temp['reportable_demo_id'] >= 2003) & (temp['reportable_demo_id'] <= 2008)].iloc[:, j])
            df_out.iloc[num_levels * i + 13, j] = sum(
                temp[(temp['reportable_demo_id'] >= 2009) & (temp['reportable_demo_id'] <= 2012)].iloc[:, j])
            df_out.iloc[num_levels * i + 14, j] = sum(
                temp[(temp['reportable_demo_id'] >= 1003) & (temp['reportable_demo_id'] <= 1008)].iloc[:, j])
            df_out.iloc[num_levels * i + 15, j] = sum(
                temp[(temp['reportable_demo_id'] >= 1009) & (temp['reportable_demo_id'] <= 1012)].iloc[:, j])
            df_out.iloc[num_levels * i + 17, j] = sum(
                temp[(temp['reportable_demo_id'] >= 2003) & (temp['reportable_demo_id'] <= 2006)].iloc[:, j])
            df_out.iloc[num_levels * i + 18, j] = sum(
                temp[(temp['reportable_demo_id'] >= 2007) & (temp['reportable_demo_id'] <= 2010)].iloc[:, j])
            df_out.iloc[num_levels * i + 19, j] = sum(
                temp[(temp['reportable_demo_id'] >= 2011) & (temp['reportable_demo_id'] <= 2012)].iloc[:, j])
            df_out.iloc[num_levels * i + 20, j] = sum(
                temp[(temp['reportable_demo_id'] >= 1003) & (temp['reportable_demo_id'] <= 1006)].iloc[:, j])
            df_out.iloc[num_levels * i + 21, j] = sum(
                temp[(temp['reportable_demo_id'] >= 1007) & (temp['reportable_demo_id'] <= 1010)].iloc[:, j])
            df_out.iloc[num_levels * i + 22, j] = sum(
                temp[(temp['reportable_demo_id'] >= 1011) & (temp['reportable_demo_id'] <= 1012)].iloc[:, j])
            df_out.iloc[num_levels * i + 24, j] = sum(
                temp[(temp['reportable_demo_id'] >= 2003) & (temp['reportable_demo_id'] <= 2004)].iloc[:, j])
            df_out.iloc[num_levels * i + 25, j] = sum(
                temp[(temp['reportable_demo_id'] >= 2005) & (temp['reportable_demo_id'] <= 2006)].iloc[:, j])
            df_out.iloc[num_levels * i + 26, j] = sum(
                temp[(temp['reportable_demo_id'] >= 2007) & (temp['reportable_demo_id'] <= 2008)].iloc[:, j])
            df_out.iloc[num_levels * i + 27, j] = sum(
                temp[(temp['reportable_demo_id'] >= 2009) & (temp['reportable_demo_id'] <= 2010)].iloc[:, j])
            df_out.iloc[num_levels * i + 28, j] = sum(temp[temp['reportable_demo_id'] == 2011].iloc[:, j])
            df_out.iloc[num_levels * i + 29, j] = sum(temp[temp['reportable_demo_id'] == 2012].iloc[:, j])
            df_out.iloc[num_levels * i + 30, j] = sum(
                temp[(temp['reportable_demo_id'] >= 1003) & (temp['reportable_demo_id'] <= 1004)].iloc[:, j])
            df_out.iloc[num_levels * i + 31, j] = sum(
                temp[(temp['reportable_demo_id'] >= 1005) & (temp['reportable_demo_id'] <= 1006)].iloc[:, j])
            df_out.iloc[num_levels * i + 32, j] = sum(
                temp[(temp['reportable_demo_id'] >= 1007) & (temp['reportable_demo_id'] <= 1008)].iloc[:, j])
            df_out.iloc[num_levels * i + 33, j] = sum(
                temp[(temp['reportable_demo_id'] >= 1009) & (temp['reportable_demo_id'] <= 1010)].iloc[:, j])
            df_out.iloc[num_levels * i + 34, j] = sum(temp[temp['reportable_demo_id'] == 1011].iloc[:, j])
            df_out.iloc[num_levels * i + 35, j] = sum(temp[temp['reportable_demo_id'] == 1012].iloc[:, j])
            df_out.iloc[num_levels * i + 37, j] = sum(temp[temp['reportable_demo_id'] == 2000].iloc[:, j])
            for k in range(1, 11):
                df_out.iloc[num_levels * i + 37 + k, j] = sum(temp[temp['reportable_demo_id'] == (2002 + k)].iloc[:, j])
            df_out.iloc[num_levels * i + 48, j] = sum(temp[temp['reportable_demo_id'] == 1000].iloc[:, j])
            for k in range(1, 11):
                df_out.iloc[num_levels * i + 48 + k, j] = sum(temp[temp['reportable_demo_id'] == (1002 + k)].iloc[:, j])

    return df_out


# this is for sub
def target_break_down_sub(data_in):
    indices = data_in["brand_name"].apply(lambda x: 'preview' in str.lower(x))

    data_in = data_in.loc[~indices, :].reset_index(drop=True)

    data_in['Counts'] = data_in.groupby(['channel_name'])['channel_id'].transform('count')
    data_in = data_in.loc[data_in["Counts"] == 26, :].reset_index(drop=True)
    # data_in = data_in[data_in['num'] == 26]
    platform = data_in.loc[0, 'platform']
    content = data_in.loc[0, 'content']

    data_in_dedup = data_in.drop_duplicates(['channel_id'])
    brands = data_in_dedup.channel_name.unique()
    brand_ids = data_in_dedup.channel_id.unique()
    # brands = data_in.brand_name.unique()
    # Remove 'MSN/Outlook/Bing/Skype'
    if 'MSN/Outlook/Bing/Skype' in brands:
        brands = np.delete(brands, [i for i, s in enumerate(brands) if s == 'MSN/Outlook/Bing/Skype'][0])

    # Remove special characters from brand names and create target names
    demos = ['total',
             'p0212', 'p1399',
             'p0217', 'p1899',
             'p0212', 'm1399', 'f1399',
             'p0217', 'm1899', 'f1899',
             'p0217', 'm1844', 'm4599', 'f1844', 'f4599',
             'p0217', 'm1834', 'm3554', 'm5599', 'f1834', 'f3554', 'f5599',
             'p0217', 'm1824', 'm2534', 'm3544', 'm4554', 'm5564', 'm6599',
             'f1824', 'f2534', 'f3544', 'f4554', 'f5564', 'f6599',
             'p0212', 'm1317', 'm1820', 'm2124', 'm2529', 'm3034', 'm3539', 'm4044', 'm4549', 'm5054', 'm5564', 'm6599',
             'f1317', 'f1820', 'f2124', 'f2529', 'f3034', 'f3539', 'f4044', 'f4549', 'f5054', 'f5564', 'f6599']
    df_out = pd.DataFrame(data=np.zeros((len(demos) * len(brands), 8)),
                          columns=['Target', 'Reach', 'Impression', 'Duration',
                                   'Code', 'Level', 'BrandId', 'Demo'])

    num_levels = 59
    df_out['Code'] = 1
    df_out['Level'] = ([0] + [1] * 2 + [2] * 2 + [3] * 3 + [4] * 3 + [5] * 5 + [6] * 7 + [7] * 13 + [8] * 23) * len(
        brands)
    #                   1     2       4       6      9        12      17      24      37
    target_names = []
    target_ids = []
    target_demos = []
    for i in range(0, len(brands)):
        brand_name = ''.join(e for e in brands[i] if e.isalnum()).lower()[0:15]
        #    print(i)
        #    print(brand_name)
        brand_id = brand_ids[i]
        #    print(brand_id)
        if content == 'Text':
            temp2 = [brand_id for demo in demos]
            temp3 = [demo for demo in demos]
            if platform == 'Desktop':
                temp = [brand_name + '_pc_sb_pv_' + demo for demo in demos]
            elif platform == 'Mobile':
                temp = [brand_name + '_mob_sb_pv_' + demo for demo in demos]
            else:
                temp = []
        elif content == 'Video':
            temp2 = [brand_id for demo in demos]
            temp3 = [demo for demo in demos]
            if platform == 'Desktop':
                temp = [brand_name + '_pc_sv_mm_' + demo for demo in demos]
            elif platform == 'Mobile':
                temp = [brand_name + '_mob_sv_mm_' + demo for demo in demos]
            else:
                temp = []
        else:
            temp = []
            temp2 = []
            temp3 = []

        target_names.extend(temp)
        target_ids.extend(temp2)
        target_demos.extend(temp3)

    df_out['Target'] = target_names
    df_out['BrandId'] = target_ids
    df_out['Demo'] = target_demos

    for i in range(0, len(brands)):
        temp = data_in.loc[
            data_in['channel_name'] == brands[i], ['reportable_demo_id', 'unique_audience', 'impressions',
                                                   'duration']]
        for j in range(1, 4):
            df_out.iloc[num_levels * i, j] = float(temp[temp['reportable_demo_id'] == 30000].iloc[:, j])
            df_out.iloc[[num_levels * i + 1, num_levels * i + 5, num_levels * i + 36], j] = float(
                temp[temp['reportable_demo_id'] == 20000].iloc[:, j])
            df_out.iloc[num_levels * i + 2, j] = sum(
                temp[(temp['reportable_demo_id'] == 1999) | (temp['reportable_demo_id'] == 2999)].iloc[:, j])
            df_out.iloc[[num_levels * i + 3, num_levels * i + 8, num_levels * i + 11, num_levels * i + 16,
                         num_levels * i + 23], j] = sum(temp[(temp['reportable_demo_id'] == 20000) | (
                    (temp['reportable_demo_id'] == 1000) | (temp['reportable_demo_id'] == 2000))].iloc[:, j])
            df_out.iloc[num_levels * i + 4, j] = sum(
                temp[((temp['reportable_demo_id'] >= 2003) & (temp['reportable_demo_id'] <= 2012)) |
                     ((temp['reportable_demo_id'] >= 1003) & (temp['reportable_demo_id'] <= 1012))].iloc[:, j])
            df_out.iloc[num_levels * i + 6, j] = float(temp[temp['reportable_demo_id'] == 2999].iloc[:, j])

            df_out.iloc[num_levels * i + 7, j] = float(temp[temp['reportable_demo_id'] == 1999].iloc[:, j])
            df_out.iloc[num_levels * i + 9, j] = sum(
                temp[(temp['reportable_demo_id'] >= 2003) & (temp['reportable_demo_id'] <= 2012)].iloc[:, j])
            df_out.iloc[num_levels * i + 10, j] = sum(
                temp[(temp['reportable_demo_id'] >= 1003) & (temp['reportable_demo_id'] <= 1012)].iloc[:, j])
            df_out.iloc[num_levels * i + 12, j] = sum(
                temp[(temp['reportable_demo_id'] >= 2003) & (temp['reportable_demo_id'] <= 2008)].iloc[:, j])
            df_out.iloc[num_levels * i + 13, j] = sum(
                temp[(temp['reportable_demo_id'] >= 2009) & (temp['reportable_demo_id'] <= 2012)].iloc[:, j])
            df_out.iloc[num_levels * i + 14, j] = sum(
                temp[(temp['reportable_demo_id'] >= 1003) & (temp['reportable_demo_id'] <= 1008)].iloc[:, j])
            df_out.iloc[num_levels * i + 15, j] = sum(
                temp[(temp['reportable_demo_id'] >= 1009) & (temp['reportable_demo_id'] <= 1012)].iloc[:, j])
            df_out.iloc[num_levels * i + 17, j] = sum(
                temp[(temp['reportable_demo_id'] >= 2003) & (temp['reportable_demo_id'] <= 2006)].iloc[:, j])
            df_out.iloc[num_levels * i + 18, j] = sum(
                temp[(temp['reportable_demo_id'] >= 2007) & (temp['reportable_demo_id'] <= 2010)].iloc[:, j])
            df_out.iloc[num_levels * i + 19, j] = sum(
                temp[(temp['reportable_demo_id'] >= 2011) & (temp['reportable_demo_id'] <= 2012)].iloc[:, j])
            df_out.iloc[num_levels * i + 20, j] = sum(
                temp[(temp['reportable_demo_id'] >= 1003) & (temp['reportable_demo_id'] <= 1006)].iloc[:, j])
            df_out.iloc[num_levels * i + 21, j] = sum(
                temp[(temp['reportable_demo_id'] >= 1007) & (temp['reportable_demo_id'] <= 1010)].iloc[:, j])
            df_out.iloc[num_levels * i + 22, j] = sum(
                temp[(temp['reportable_demo_id'] >= 1011) & (temp['reportable_demo_id'] <= 1012)].iloc[:, j])
            df_out.iloc[num_levels * i + 24, j] = sum(
                temp[(temp['reportable_demo_id'] >= 2003) & (temp['reportable_demo_id'] <= 2004)].iloc[:, j])
            df_out.iloc[num_levels * i + 25, j] = sum(
                temp[(temp['reportable_demo_id'] >= 2005) & (temp['reportable_demo_id'] <= 2006)].iloc[:, j])
            df_out.iloc[num_levels * i + 26, j] = sum(
                temp[(temp['reportable_demo_id'] >= 2007) & (temp['reportable_demo_id'] <= 2008)].iloc[:, j])
            df_out.iloc[num_levels * i + 27, j] = sum(
                temp[(temp['reportable_demo_id'] >= 2009) & (temp['reportable_demo_id'] <= 2010)].iloc[:, j])
            df_out.iloc[num_levels * i + 28, j] = sum(temp[temp['reportable_demo_id'] == 2011].iloc[:, j])
            df_out.iloc[num_levels * i + 29, j] = sum(temp[temp['reportable_demo_id'] == 2012].iloc[:, j])
            df_out.iloc[num_levels * i + 30, j] = sum(
                temp[(temp['reportable_demo_id'] >= 1003) & (temp['reportable_demo_id'] <= 1004)].iloc[:, j])
            df_out.iloc[num_levels * i + 31, j] = sum(
                temp[(temp['reportable_demo_id'] >= 1005) & (temp['reportable_demo_id'] <= 1006)].iloc[:, j])
            df_out.iloc[num_levels * i + 32, j] = sum(
                temp[(temp['reportable_demo_id'] >= 1007) & (temp['reportable_demo_id'] <= 1008)].iloc[:, j])
            df_out.iloc[num_levels * i + 33, j] = sum(
                temp[(temp['reportable_demo_id'] >= 1009) & (temp['reportable_demo_id'] <= 1010)].iloc[:, j])
            df_out.iloc[num_levels * i + 34, j] = sum(temp[temp['reportable_demo_id'] == 1011].iloc[:, j])
            df_out.iloc[num_levels * i + 35, j] = sum(temp[temp['reportable_demo_id'] == 1012].iloc[:, j])
            df_out.iloc[num_levels * i + 37, j] = sum(temp[temp['reportable_demo_id'] == 2000].iloc[:, j])
            for k in range(1, 11):
                df_out.iloc[num_levels * i + 37 + k, j] = sum(temp[temp['reportable_demo_id'] == (2002 + k)].iloc[:, j])
            df_out.iloc[num_levels * i + 48, j] = sum(temp[temp['reportable_demo_id'] == 1000].iloc[:, j])
            for k in range(1, 11):
                df_out.iloc[num_levels * i + 48 + k, j] = sum(temp[temp['reportable_demo_id'] == (1002 + k)].iloc[:, j])

    return df_out


def build_dcr_brand_by_type_data_pandas(df, dcr_type):
    """
    :param df:
    :param dcr_type:
    :return:
    """

    final_cols = ['brand_id', 'unique_audience', 'name', 'duration', 'impressions']

    df['duration'] = df['duration'].astype(float)
    df['impressions'] = df['impressions'].astype(float)
    df['unique_audience'] = df['unique_audience'].astype(float)
    df['reportable_demo_id'] = df['reportable_demo_id'].astype(int)
    df['brand_id'] = df['brand_id'].astype(int)

    table = str.maketrans('', '', string.punctuation + ' ')

    df['name'] = df['brand_name'] \
            .apply(lambda x: x.translate(table).lower()[:15])

    indices = ((df['platform'] == 'Desktop') &
               (df['content'] == dcr_type) &
               (df['unique_audience'] > 250000) &
               (df['reportable_demo_id'] == 30000))

    if dcr_type == 'Text':
        df['duration'] = 60 * df['duration']
        rename_cols_dict = {'brand_id': 'site_id', 'impressions': 'computer'}
    else:
        rename_cols_dict = {'brand_id': 'site_id', 'duration': 'computer'}

    return df.loc[indices, final_cols]\
        .reset_index(drop=True).rename(columns=rename_cols_dict)


def build_dcr_sub_brand_by_type_data_pandas(df, dcr_type, sub_brand_names):
    """
    :param df:
    :param dcr_type:
    :param sub_brand_names:
    :return:
    """

    final_cols = ['channel_id', 'unique_audience', 'name', 'duration', 'impressions']

    df['duration'] = df['duration'].astype(float)
    df['impressions'] = df['impressions'].astype(float)
    df['unique_audience'] = df['unique_audience'].astype(float)
    df['reportable_demo_id'] = df['reportable_demo_id'].astype(int)
    df['channel_id'] = df['channel_id'].astype(int)

    table = str.maketrans('', '', string.punctuation + ' ')

    df['name'] = df['sub_brand_name'] \
        .apply(lambda x: x.translate(table).lower()[:15])

    indices = ((df['platform'] == 'Desktop') &
               (df['content'] == dcr_type) &
               (df['unique_audience'] > 250000) &
               (df['reportable_demo_id'] == 30000) &
               ((~df['sub_brand_name'].isin(sub_brand_names)) |
                 (~df['brand_name'].isin(sub_brand_names))))

    if dcr_type == 'Text':
        rename_cols_dict = {'channel_id': 'site_id', 'impressions': 'computer'}
    else:
        rename_cols_dict = {'channel_id': 'site_id', 'duration': 'computer'}

    return df.loc[indices, final_cols]\
        .reset_index(drop=True).rename(columns=rename_cols_dict)


def build_dcr_data(s3_conn):
    """
    :param s3_conn: S3 Connection, made by boto3
    :return:
    """
    logger = logging.getLogger(__name__)

    logger.info('Reading in data from S3 for DCR PC Brand')

    try:
        all_dcr_brands_pc_text = pandas_read_s3(s3_conn, 'query_result_desktop_text')

        dcr_pc_targets_df = target_break_down(all_dcr_brands_pc_text)

        all_dcr_brands_pc_text = build_dcr_brand_by_type_data_pandas(all_dcr_brands_pc_text, 'Text')

    except Exception as e:
        logger.fatal(e, exc_info=True)
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), \
               pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    logger.info('Reading in data from S3 for DCR Video Brand')

    try:
        all_dcr_brands_video = pandas_read_s3(s3_conn, 'query_result_desktop_video')

        dcr_vid_targets_df = target_break_down(all_dcr_brands_video)

        all_dcr_brands_video = build_dcr_brand_by_type_data_pandas(all_dcr_brands_video, 'Video')

    except Exception as e:
        logger.fatal(e, exc_info=True)
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), \
               pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    logger.info('Reading in data from S3 for DCR PC Sub-brand')

    try:
        all_dcr_sub_brands_pc_text = pandas_read_s3(s3_conn, 'query_result_desktop_text_sub')

        dcr_sub_pc_targets_df = target_break_down_sub(all_dcr_sub_brands_pc_text)

        all_dcr_sub_brands_pc_text = build_dcr_sub_brand_by_type_data_pandas(all_dcr_sub_brands_pc_text, 'Text',
                                                                             ['Preview'])

    except Exception as e:
        logger.fatal(e, exc_info=True)
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), \
               pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    logger.info('Reading in data from S3 for DCR Video Sub-brand')

    try:
        all_dcr_sub_brands_video = pandas_read_s3(s3_conn, 'query_result_desktop_video_sub')

        dcr_sub_vid_targets_df = target_break_down_sub(all_dcr_sub_brands_video)

        all_dcr_sub_brands_video = build_dcr_sub_brand_by_type_data_pandas(all_dcr_sub_brands_video, 'Video',
                                                                           ['Preview', 'Internal'])

    except Exception as e:
        logger.fatal(e, exc_info=True)
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), \
               pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    return all_dcr_brands_pc_text, all_dcr_sub_brands_pc_text, all_dcr_brands_video, all_dcr_sub_brands_video, \
           dcr_pc_targets_df, dcr_vid_targets_df, dcr_sub_pc_targets_df, dcr_sub_vid_targets_df
