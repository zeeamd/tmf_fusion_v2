from pyspark.ml import Pipeline
from pyspark.ml.feature import Bucketizer
from pyspark.sql.functions import when

from recoding.src.main.transformers import *


def main_emm_recode_demos(emm_raw_sdf):

    recode_demo_pipeline = Pipeline(stages=[
        Bucketizer(splits=[0, 2, 6, 12, 18, 25, 35, 45, 55, 65, 150], inputCol='age', outputCol="age1"),
        Bucketizer(splits=[0, 18, 25, 35, 45, 55, 65, 150], inputCol='age', outputCol="age7"),
        Bucketizer(splits=[0, 12, 18, 25, 35, 45, 55, 65, 150], inputCol='age', outputCol="age8"),
#        Bucketizer(splits=[-25, 0, 25., 50., 75., 100., float('Inf')], inputCol='income_amt', outputCol="income1"),
#        Bucketizer(splits=[-25, 0, 25., 35., 50., 75., 100., float('Inf')], inputCol='income_amt', outputCol="income9"),
        IfElseTransformer(vals=[83], inputCol='hispanicid', outputCol='hispanic'),
        IfElseTransformer(vals=['M'], inputCol='gender_char', outputCol='gender'),
        IfElseTransformer(vals=[86], inputCol='raceid', outputCol='race_back'),
        IfElseTransformer(vals=[88], inputCol='raceid', outputCol='race_asian'),
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
        IsInTransformer(isin_bins=[['A'], ['B'], ['C'], ['D']], inputCol='county_size_code', outputCol='county_size')
    ])

    return None