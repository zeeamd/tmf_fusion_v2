import logging

from pyspark.sql import SparkSession


def configure_spark_session(app_name):
    """
    :param app_name:
    :return:
    """

    logger = logging.getLogger(__name__)

    logger.info('Building Spark Session')

    spark = SparkSession \
        .builder \
        .appName(app_name).getOrCreate()

    spark.conf.set("spark.sql.execution.arrow.enabled", "true")
    spark.conf.set('spark.sql.shuffle.partitions', '10')
    spark.conf.set("spark.files.maxPartitionBytes", "100")
    spark.conf.set("spark.memory.offHeap.enabled", "true")
    spark.conf.set("spark.memory.offHeap.size", "32g")
    spark.conf.set("spark.executor.memory", "12g")
    spark.conf.set('spark.jars.packages', "org.apache.hadoop:hadoop-aws:2.7.3")

    return spark

