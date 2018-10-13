import logging
import string

from pyspark.ml import Pipeline
from pyspark.ml.feature import Bucketizer
from pyspark.sql.functions import pandas_udf, concat
from pyspark.sql.types import StringType
from recoding.src.main.transformers import *


def npm_demos_recode(spark_df):
    """
    :param spark_df:
    :return:
    """

    npm_demo_pipeline = Pipeline(stages=[
        Bucketizer(splits=[0, 2, 6, 12, 18, 25, 35, 45, 55, 65, 150], inputCol='age', outputCol="age1"),
        Bucketizer(splits=[0, 18, 25, 35, 45, 55, 65, 150], inputCol='age', outputCol="age7"),
        Bucketizer(splits=[0, 12, 18, 25, 35, 45, 55, 65, 150], inputCol='age', outputCol="age8"),
        Bucketizer(splits=[-25, 0, 25., 50., 75., 100., float('Inf')], inputCol='income_amt', outputCol="income1"),
        Bucketizer(splits=[-25, 0, 25., 35., 50., 75., 100., float('Inf')], inputCol='income_amt', outputCol="income9"),
        Bucketizer(splits=[-1.0*float('Inf'), 0, 1, 30, float('Inf')], inputCol='working_hours_number',
                   outputCol="employ_dummy"),
        InlierMapperTransformer(values_mapper=[[8.], 1.], inputCols=['nielsen_occupation_code', 'employ_dummy'],
                                outputCol='employment1'),
        IfElseTransformer(vals=[2], inputCol='origin_code', outputCol='hispanic'),
        IfElseTransformer(vals=[1], inputCol='gender_code', outputCol='gender'),
        IfElseTransformer(vals=[2], inputCol='race_code', outputCol='race_back'),
        IfElseTransformer(vals=[3, 4, 5, 6, 7, 9], inputCol='race_code', outputCol='race_asian'),
        YesNoTransformer(inputCol='dvr_flag', outputCol='dvr'),
        YesNoTransformer(inputCol='cable_plus_flag', outputCol='cableplus'),
        YesNoTransformer(inputCol='video_game_owner_flag', outputCol='video_game'),
        YesNoTransformer(inputCol='internet_access_flag', outputCol='internet'),
        YesNoTransformer(inputCol='pay_cable_flag', outputCol='paycable'),
        YesNoTransformer(inputCol='television_high_definition_display_capability_flag', outputCol='hdtv'),
        YesNoTransformer(inputCol='alternative_delivery_flag', outputCol='satellite'),
        IsInTransformer(isin_bins=[[0, 1], [2], [3, 4, 5, 6, 7], [8]], inputCol='nielsen_occupation_code',
                        outputCol='occupation1'),
        IsInTransformer(isin_bins=[[0, 8, 9, 10, 11, 12], [13, 14, 15], [16], [18, 19, 20]],
                        inputCol='education_level_number', outputCol='education7'),
        IsInTransformer(isin_bins=[[16, 18, 19, 20], [0, 8, 9, 10, 11, 12, 13, 14, 15]],
                        inputCol='education_level_number', outputCol='education2'),
        IsInTransformer(isin_bins=[['A'], ['B'], ['C'], ['D']], inputCol='county_size_code', outputCol='county_size'),
        IsInTransformer(isin_bins=[[1], [3], [5], [4], [2]], inputCol='language_class_code',
                        outputCol='spanish_lang_dummy'),
        InlierMapperTransformer(values_mapper=[[1.], 6.], inputCols=['origin_code', 'spanish_lang_dummy'],
                                outputCol='spanish_language1'),
        MoreThanTransformer(inputCols=['number_of_kids_less_than_6'], outputCol='kids_0to5'),
        MoreThanTransformer(inputCols=['number_of_kids_less_than_12', 'number_of_kids_less_than_6'],
                            outputCol='kids_6to11'),
        MoreThanTransformer(inputCols=['number_of_kids_less_than_18', 'number_of_kids_less_than_12'],
                            outputCol='kids_12to17'),
        ClipperTransformer(clip_lower=0, clip_upper=5, inputCol='number_of_tv_sets', outputCol='number_of_tvs1'),
        ClipperTransformer(clip_lower=0, clip_upper=5, inputCol='number_of_persons_count_all', outputCol='hh_size1')
    ])

    model = npm_demo_pipeline.fit(spark_df)
    res_sdf = model.transform(spark_df)

    return res_sdf


def main_npm_demos_recode(npm_hh_raw_sdf, npm_person_raw_sdf):
    """
    :param npm_hh_raw_sdf: Spark DataFrame
    :param npm_person_raw_sdf: Spark DataFrame
    :return: npm_recode_sdf: Spark DataFrame
    """

    logger = logging.getLogger(__name__)

    logger.info('Recoding NPM Demographics')

    npm_hh_1_sdf = npm_hh_raw_sdf.withColumnRenamed('avgwt', 'hh_weight')
    npm_hh_2_sdf = npm_hh_1_sdf.withColumnRenamed('cph_daily_avgwt', 'hh_cph_weight')
    npm_hh_3_sdf = npm_hh_2_sdf.withColumnRenamed('countintab', 'hh_countintab')
    npm_hh_4_sdf = npm_hh_3_sdf.withColumnRenamed('zip_code', 'zip_code_hh')

    npm_person_1_sdf = npm_person_raw_sdf.withColumnRenamed('avgwt', 'weight')
    npm_person_2_sdf = npm_person_1_sdf.withColumnRenamed('cph_daily_avgwt', 'cph_weight')
    npm_person_3_sdf = npm_person_2_sdf.withColumnRenamed('zip_code', 'zip_code_psn')
    npm_person_4_sdf = npm_person_3_sdf.withColumnRenamed('language_spoken_code_psn', 'language_class_code')

    npm_hh_dup_sdf = npm_hh_4_sdf.dropDuplicates(subset=['household_id'])

    npm_person_dup_sdf = npm_person_4_sdf.dropDuplicates(subset=['household_id', 'person_id'])

    npm_raw_sdf = npm_hh_dup_sdf.join(npm_person_dup_sdf, 'household_id').filter('hh_countintab > 0').filter('countintab > 0')

    npm_with_id_sdf = npm_raw_sdf.withColumn('respondentid', npm_raw_sdf['household_id'] * 1000 + npm_raw_sdf['person_id'])

    npm_df = npm_with_id_sdf.join(npm_person_dup_sdf.groupby('household_id').agg({'person_id':  'count'})\
                                   .withColumnRenamed('count(person_id)', 'number_of_persons_count_all'), 'household_id')

    npm_recode_sdf = npm_demos_recode(npm_df)

    logger.info('Done!')

    return npm_recode_sdf


def main_npm_usage_recode(npm_demos_recode_sdf, npm_tv_view_raw_sdf):

    logger = logging.getLogger(__name__)

    logger.info('Creating PC Column')

    table = str.maketrans('', '', string.punctuation + ' ')

    @pandas_udf(StringType())
    def funct(x):
        return x.translate(table)[:22]

    npm_tv_view_raw_sdf_pc = npm_tv_view_raw_sdf.withColumn('pc', concat(funct(npm_tv_view_raw_sdf['reportable_network_name']),
                                                                         npm_tv_view_raw_sdf['daypart'].cast(StringType())))

    npm_tv_view_raw_sdf_pc_res_id = npm_tv_view_raw_sdf_pc.withColumn('respondentid',
                                                                      npm_tv_view_raw_sdf_pc['person_id'] +
                                                                      (npm_tv_view_raw_sdf_pc['household_id'] * 1000))

    logger.info('Recoding NPM TV Usage Data')

    npm_demos_usage_sdf = npm_demos_recode_sdf.filter('internet == 1').filter('1 <= age1')\
        .join(npm_tv_view_raw_sdf_pc_res_id, 'respondentid')

    tv_500_sdf = npm_demos_usage_sdf.groupby('pc').sum('min_viewed').sort('sum(min_viewed)',
                                                                              ascending=False).limit(500)

    tv_100_sdf = tv_500_sdf.limit(100)

    tv_top_500_df = npm_demos_usage_sdf.join(tv_500_sdf, 'pc').groupby('respondentid', 'pc').sum('min_viewed')\
        .groupby('respondentid').pivot('pc').sum('sum(min_viewed)').fillna(0.0)

    tv_top_100_df = npm_demos_usage_sdf.join(tv_100_sdf, 'pc').groupby('respondentid', 'pc').sum('min_viewed')\
            .groupby('respondentid').pivot('pc').sum('sum(min_viewed)').fillna(0.0)

    logger.info('Done!')

    return tv_top_500_df, tv_top_100_df

