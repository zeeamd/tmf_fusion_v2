import logging
import string

from library.src.main.core import pivot_table


def main_npm_usage_recode(npm_demos_recode_df, npm_tv_view_raw_df):

    logger = logging.getLogger(__name__)

    logger.info('Creating PC Column')

    table = str.maketrans('', '', string.punctuation + ' ')

    def funct(x):
        return x.translate(table)[:22]

    npm_tv_view_raw_df['pc'] = npm_tv_view_raw_df['reportable_network_name'].apply(funct) + npm_tv_view_raw_df['daypart'].astype(str)

    npm_tv_view_raw_df['respondentid'] = (1000*npm_tv_view_raw_df['household_id']) + npm_tv_view_raw_df['person_id']

    logger.info('Recoding NPM TV Usage Data')

    indices = (npm_demos_recode_df['internet'] == 1) & (1 <= npm_demos_recode_df['age1'])

    npm_demos_usage_df = npm_demos_recode_df.loc[indices, :].reset_index(drop=True).merge(npm_tv_view_raw_df,
                                                                                           on='respondentid',
                                                                                           how='left')

    tv_500_df = npm_demos_usage_df.groupby('pc')['min_viewed'].sum().reset_index()\
                 .sort_values(by='min_viewed', ascending=False).reset_index(drop=True)

    tv_500 = tv_500_df['pc'].tolist()[:500]

    tv_100 = tv_500[:100]

    tv_top_500_df = pivot_table(npm_demos_usage_df.loc[npm_demos_usage_df['pc'].isin(tv_500), :].reset_index(drop=True),
                                'min_viewed', 'respondentid', 'pc').reset_index().rename(
        columns={'index': 'respondentid'})

    tv_top_100_df = pivot_table(npm_demos_usage_df.loc[npm_demos_usage_df['pc'].isin(tv_100), :].reset_index(drop=True),
                                'min_viewed', 'respondentid', 'pc').reset_index().rename(
        columns={'index': 'respondentid'})

    logger.info('Done!')

    return tv_top_500_df, tv_top_100_df

