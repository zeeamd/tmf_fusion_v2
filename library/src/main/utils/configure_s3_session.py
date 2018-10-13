import pandas as pd
import boto3
from pyspark.sql import SparkSession
import io


def configure_s3_client(profile_name='default'):
    session = boto3.Session(profile_name=profile_name)

    return session.client('s3')


def print_s3_objs(bucket_name='useast1-nlsn-mdl-w-tam-totalmediafusion-dev'):
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(bucket_name)
    for obj in bucket.objects.all():
        print(obj)


def configure_s3_connection(aws_profile='default'):
    session = boto3.Session(profile_name=aws_profile)

    conn = session.resource('s3')

    return conn


def parallel_spark_read_s3(spark, s3_conn, flnm, bckt_nm='useast1-nlsn-mdl-w-tam-totalmediafusion-dev',
                           fl_type='csv'):
    assert isinstance(spark, SparkSession)

    sc = spark.sparkContext

    s3_files = s3_conn.Bucket(bckt_nm).objects.all()

    flnm_strs = [s3_conn.Object(bckt_nm, s3_file.key).get()['Body'].read()
                 for s3_file in s3_files if (flnm in s3_file.key) and (fl_type in s3_file.key)]

    def funct(flnm_str):
        for line in flnm_str.splitlines():
            yield line.decode("utf-8").split(',')

    pkeys = sc.parallelize(flnm_strs)

    data_rdd = pkeys.flatMap(funct)

    header = data_rdd.first()

    data = data_rdd.filter(lambda line: line != header).toDF(header)

    return data


def pandas_read_s3(s3_conn, flnm, bckt_nm='useast1-nlsn-mdl-w-tam-totalmediafusion-dev',
                           fl_type='csv'):

    s3_files = s3_conn.Bucket(bckt_nm).objects.all()

    flnm_strs = [s3_conn.Object(bckt_nm, s3_file.key).get()['Body'].read()
                 for s3_file in s3_files if (flnm in s3_file.key) and (fl_type in s3_file.key)]

    df = pd.concat([pd.read_csv(io.BytesIO(flnm_str)) for flnm_str in flnm_strs], ignore_index=True)

    df.columns = df.iloc[1]
    df.reindex(df.index.drop(1))

    return df