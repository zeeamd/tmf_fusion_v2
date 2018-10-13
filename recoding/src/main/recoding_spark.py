from pyspark.ml.feature import Bucketizer
from pyspark.sql.functions import when


def age_recoder(spark_df, age_col):
    """
    :param spark_df:
    :param age_col:
    :return:
    """

    age1 = Bucketizer(splits=[0, 2, 6, 12, 18, 25, 35, 45, 55, 65, 150], inputCol=age_col, outputCol="age1")
    age7 = Bucketizer(splits=[0, 18, 25, 35, 45, 55, 65, 150], inputCol=age_col, outputCol="age7")
    age8 = Bucketizer(splits=[0, 12, 18, 25, 35, 45, 55, 65, 150], inputCol=age_col, outputCol="age8")

    sdf_1 = age1.setHandleInvalid("keep").transform(spark_df)
    sdf_2 = age7.setHandleInvalid("keep").transform(sdf_1)
    res_sdf = age8.setHandleInvalid("keep").transform(sdf_2)

    return res_sdf


def zip_code_region_mapper(spark_df, zip_code_state_sdf, zip_col):
    """
    :param spark_df:
    :param zip_code_state_sdf: Spark DataFrame
    :param zip_col:
    :return:
    """

    northeast_states = ['NJ', 'NY', 'PA', 'CT', 'ME', 'MA', 'NH', 'RI', 'VT']
    midwest_states = ['IL', 'IN', 'MI', 'OH', 'WI', 'IA', 'KS', 'MN', 'MO', 'ND', 'NE', 'SD']
    south_states = ['AL', 'KY', 'MS', 'TN', 'DC', 'DE', 'FL', 'GA', 'MD', 'NC', 'SC', 'VA', 'WV', 'AR',
                    'LA',
                    'OK', 'TX']
    west_states = ['AZ', 'CO', 'ID', 'MT', 'NM', 'NV', 'UT', 'WY', 'AK', 'CA', 'HI', 'OR', 'WA']

    state_sdf = spark_df.join(zip_code_state_sdf, zip_col)

    region_sdf = state_sdf.withColumn('regionb', when(state_sdf['st'].isin(northeast_states), 1).otherwise(
        when(state_sdf['st'].isin(midwest_states), 2 ).otherwise(
            when(state_sdf['st'].isin(south_states), 3).otherwise(
                when(state_sdf['st'].isin(west_states), 4)
            )
        )
    )
                                      )

    sdf_1 = region_sdf.withColumn('regionb_northeast', when(region_sdf['regionb'] == 1., 1).otherwise(2))
    sdf_2 = sdf_1.withColumn('regionb_midwest', when(sdf_1['regionb'] == 2., 1).otherwise(2))
    sdf_3 = sdf_2.withColumn('regionb_south', when(sdf_2['regionb'] == 3., 1).otherwise(2))
    res_sdf = sdf_3.withColumn('regionb_west', when(sdf_3['regionb'] == 4., 1).otherwise(2))

    return res_sdf

