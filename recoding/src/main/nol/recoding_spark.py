import re

from pyspark.sql.functions import pandas_udf, concat, lit
from pyspark.sql.types import StringType
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql import Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import Bucketizer

from recoding.src.main.transformers import *


def recode_nol_site_usage_type(input_sdf, type_, num_top_sites=200):
    """
    :param input_sdf:
    :param type_:
    :param num_top_sites:
    :return:
    """

    if type_ == 'brand_strm':
        sdf_1 = input_sdf.withColumnRenamed('brand_id', 'tree_id')
        sdf_2 = sdf_1.withColumn('tree_name', concat(sdf_1['brand_name'], lit('_brand')))
    else:
        # rename a column and sort by duration in descending order
        sdf_1 = input_sdf.withColumnRenamed(type_ + '_id', 'tree_id')
        sdf_2 = sdf_1.withColumn('tree_name', concat(sdf_1[type_+'_name'], lit('_'+type_)))

    top_200_sites_sdf = sdf_2.groupby('tree_name').sum('duration').sort('sum(duration)', ascending=False)\
        .limit(num_top_sites)

    top_200_sites_sdf_rank = top_200_sites_sdf.select('tree_name').rdd.zipWithIndex().map(lambda x: x[0] + (x[1], ))\
        .toDF(["tree_name", "rank"])

    top_sdf = sdf_2.join(top_200_sites_sdf_rank, 'tree_name').withColumn('rank_id', top_200_sites_sdf_rank["rank"] + 1)

    top_sdf_site = top_sdf.withColumn('site', concat(lit(type_+'_'), top_sdf['rank_id']))

    top_grouped_sdf = top_sdf_site.groupby(['rn_id', 'site']).sum('duration')

    # creates site variable
    @pandas_udf(StringType())
    def funct(site_series):
        def funct_temp(x):
            site_type, site_rank = x.split('_')[0], int(x.split('_')[1])

            if site_rank < 10:
                return site_type + '_yn00' + str(site_rank)
            elif site_rank < 100:
                return site_type + '_yn0' + str(site_rank)
            else:
                return site_type + '_yn' + str(site_rank)

        return site_series.apply(funct_temp)

    top_sdf_1 = top_grouped_sdf.withColumn('site_yn', funct(top_grouped_sdf['site']))
    top_sdf_grouped = top_sdf_1.withColumn('indicator', when(top_sdf_1['sum(duration)'] > 0.0, 1).otherwise(0))

    top_sdf_piv_bin = top_sdf_grouped.groupby('rn_id').pivot('site_yn').max('indicator')
    top_sdf_piv = top_sdf_grouped.groupby('rn_id').pivot('site').sum('sum(duration)')

    top_200_site_usage_sdf = top_sdf_piv_bin.join(top_sdf_piv, 'rn_id').fillna(0.0)

    return top_200_site_usage_sdf


def recode_nol_site_usage(us_200parents, us_200channels, us_200brands):
    """
    :param us_200parents:
    :param us_200channels:
    :param us_200brands:
    :return:
    """

    top_parents_site_usage_sdf = recode_nol_site_usage_type(us_200parents, 'parent')

    top_channel_site_usage_sdf = recode_nol_site_usage_type(us_200channels, 'channel')

    top_brand_site_usage_sdf = recode_nol_site_usage_type(us_200brands, 'brand')

    top_site_usage_sdf = top_parents_site_usage_sdf.join(
        top_channel_site_usage_sdf.join(top_brand_site_usage_sdf, 'rn_id', how='outer'),
        'rn_id', how='outer').fillna(0.0)

    return top_site_usage_sdf


def recode_nol_terr(nol_sdf, zip_code_state_sdf, zip_col):
    """
    :param nol_sdf:
    :param zip_code_state_sdf:
    :param zip_col:
    :return:
    """

    state_sdf = nol_sdf.join(zip_code_state_sdf, zip_col)

    terr_1_list = ['CT', 'DE', 'DC', 'ME', 'MD', 'MA', 'NH', 'NJ', 'NY', 'PA', 'RI', 'VT']
    terr_2_list = ['IN', 'KY', 'MI', 'OH', 'WV']
    terr_3_list = ['CO', 'IL', 'IA', 'KS', 'MN', 'MO', 'MT', 'NE', 'ND', 'SD', 'WI', 'WY']
    terr_4_list = ['AL', 'FL', 'GA', 'MS', 'NC', 'SC', 'TN', 'VA']
    terr_5_list = ['AR', 'LA', 'NM', 'OK', 'TX']

    terr_sdf = state_sdf.withColumn('terr', when(state_sdf['st'].isin(terr_1_list), 1).otherwise(
        when(state_sdf['st'].isin(terr_2_list), 2).otherwise(when(state_sdf['st'].isin(terr_3_list), 3).otherwise(
            when(state_sdf['st'].isin(terr_4_list), 4).otherwise(when(state_sdf['st'].isin(terr_5_list), 5).otherwise(6)
        )))))

    terr_sdf_1 = terr_sdf.withColumn('terr_1', when(terr_sdf['terr'] == 1, 1).otherwise(0))
    terr_sdf_2 = terr_sdf_1.withColumn('terr_2', when(terr_sdf_1['terr'] == 2, 1).otherwise(0))
    terr_sdf_3 = terr_sdf_2.withColumn('terr_3', when(terr_sdf_2['terr'] == 3, 1).otherwise(0))
    terr_sdf_4 = terr_sdf_3.withColumn('terr_4', when(terr_sdf_3['terr'] == 4, 1).otherwise(0))
    terr_sdf_5 = terr_sdf_4.withColumn('terr_5', when(terr_sdf_4['terr'] == 5, 1).otherwise(0))
    res_sdf = terr_sdf_5.withColumn('terr_6', when(terr_sdf['terr'] == 6, 1).otherwise(0))

    return res_sdf


def recode_nol_demos(nol_sdf, nol_sample_sdf):
    """
    :param nol_sdf:
    :param nol_sample_sdf:
    :return:
    """

    sdf = nol_sdf.join(nol_sample_sdf.filter('surf_location_id != 9').drop('surf_location_id'), 'rn_id')\
        .filter('gender_id > 0').filter('surf_location_id = 1').dropna(subset='age')

    nol_demo_pipeline = Pipeline(stages=[
        Bucketizer(splits=[0, 2, 6, 12, 18, 25, 35, 45, 55, 65, 150], inputCol='age', outputCol="age1"),
        Bucketizer(splits=[0, 18, 25, 35, 45, 55, 65, 150], inputCol='age', outputCol="age7"),
        Bucketizer(splits=[0, 12, 18, 25, 35, 45, 55, 65, 150], inputCol='age', outputCol="age8"),
        IfElseTransformer(vals=[2, 3, 6, 7, 10, 11, 14, 15], inputCol='web_access_locations',
                          outputCol='work_access_dummy'),
        InlierMapperTransformer(values_mapper=[[-2, 7, 8], 2], inputCols=['working_status_id', 'work_access_dummy'],
                                outputCol='work_access'),
        IfElseTransformer(vals=[5, 6, 7], inputCol='education_id', outputCol='edu'),
        IfElseTransformer(vals=[2], inputCol='race_id', outputCol='race1'),
        IfElseTransformer(vals=[3], inputCol='race_id', outputCol='race2'),
        InlierMapperTransformer(values_mapper=[[-1], 4], inputCols=['income_id', 'income_id'],
                                outputCol='inc'),
        IfElseTransformer(vals=[-1, 1], inputCol='hispanic_origin_id', outputCol='hispanic'),
        IfElseTransformer(vals=[0], inputCol='members_2_11_count', outputCol='children1'),
        IfElseTransformer(vals=[0], inputCol='members_12_17_count', outputCol='children2'),
        IsInTransformer(isin_bins=[[3, 4, 7, 10], [1, 8, -1], [2, 6, 5, 9, -13, 11]],
                        inputCol='occupation_id', outputCol='occ_dummy'),
        InlierMapperTransformer(values_mapper=[[np.nan], 4], inputCols=['occ_dummy', 'occ_dummy'],
                                outputCol='occ'),
        InlierMapperTransformer(values_mapper=[[np.nan], 1], inputCols=['county_size_id', 'county_size_id'],
                                outputCol='cs'),
        IfElseTransformer(vals=list(range(-1, 6)), inputCol='web_conn_speed_id',
                          outputCol='speed'),
        IfElseTransformer(vals=[1], inputCol='gender_id', outputCol='gender')
    ])

    model = nol_demo_pipeline.fit(sdf)
    res_sdf = model.transform(sdf)

    return res_sdf


def get_home_usage(nol_usage_sdf):
    """
    :param nol_usage_sdf:
    :return:
    """

    twh_usage_sdf = nol_usage_sdf.filter('duration > 0').groupby(['brand_cs_id', 'cc'])[
        'weight'].sum().wothColumnRenamed('weight', 'twh')

    windowval = (Window.partitionBy('brand_cs_id').orderBy('cc')
                 .rangeBetween(Window.unboundedPreceding, 0))

    sdf_1 = nol_usage_sdf.withColumn('rw', spark_sum('value').over(windowval)).join(twh_usage_sdf,
                                                                                    ['brand_cs_id', 'cc'])

    sdf_2 = sdf_1.withColumn('rw_twh', sdf_1['rw'] / sdf_1['twh'])

    sdf_3 = sdf_2.withColumn('homeusage', when(sdf_2['duration'] == 0.0, 0).otherwise(
        when(sdf_2['rw_twh'] < 1.0/3.0, 3).otherwise(when(sdf_2['rw_twh'] < 2.0 / 3.0, 2).otherwise(1))))

    return sdf_3


def recode_nol_category_usage(nol_recoded_sdf, nol_brand_c_sdf, nol_channel_c_sdf):
    """
    :param nol_recoded_sdf:
    :param nol_brand_c_sdf:
    :param nol_channel_c_sdf:
    :return:
    """

    assert 'age7' in nol_recoded_sdf.columns

    sdf1 = nol_recoded_sdf.withColumn('cc', when(nol_recoded_sdf['gender'] == 2,
                                                 nol_recoded_sdf['age7'] + 7).otherwise(nol_recoded_sdf['age7']))

    nol_usage_brand_sdf = sdf1.select(['rn_id', 'weight', 'cc']).join(nol_brand_c_sdf\
                                                                      .select(['rn_id', 'category_id',
                                                                               'subcategory_id', 'duration']),
                                                                      'rn_id').fillna(0.0)

    nol_usage_channel_sdf = sdf1.select(['rn_id', 'weight', 'cc']).join(nol_channel_c_sdf\
                                                                        .select(['rn_id', 'category_id',
                                                                                 'subcategory_id', 'duration']),
                                                                        'rn_id').fillna(0.0)

    nol_usage_brand_sdf_1 = nol_usage_brand_sdf.withColumn('brand_cs_id', concat(lit('brand_c_'),
                                                                                 nol_usage_brand_sdf['category_id'],
                                                                                 lit('_s_'),
                                                                                 nol_usage_brand_sdf['subcategory_id']))

    nol_usage_channel_sdf_1 = nol_usage_channel_sdf.withColumn('brand_cs_id', concat(lit('brand_c_'),
                                                                                     nol_usage_channel_sdf['category_id'],
                                                                                     lit('_s_'),
                                                                                     nol_usage_channel_sdf['subcategory_id']))

    res_sdf = get_home_usage(nol_usage_brand_sdf_1).groupby('rn_id').pivot('brand_cs_id').max('homeusage').join(
        get_home_usage(nol_usage_channel_sdf_1).groupby('rn_id').pivot('brand_cs_id').max('homeusage'),
        'rn_id', how='outer').fillna(0)

    return res_sdf
