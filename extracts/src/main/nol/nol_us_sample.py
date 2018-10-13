"""
File: nol_us_sample.py
Author: KiranK

 This is master function for executing adaptor steps in ordered fashion

"""

# imports from Python Standard Library
import sys
from pyspark.sql.functions import regexp_replace
import logging
import os


from library.src.main.commonutils import utils  #generic utilities
# local dependencies
from extracts.src.main.commonsql.nol_adaptor_data_pull import get_nol_sample_sql

spark = utils.spark_session("NOL US Sample Integration")

sc = spark.sparkContext
# sc.setLogLevel("WARN")
spark.conf.set("spark.sql.shuffle.partitions", 2)


# parsing system arguments from Json File
intab_prd_id = utils.readFilePath("intab_prd_id")
intab_prd_stdt = utils.readFilePath("intab_prd_stdt")
intab_prd_endt = utils.readFilePath("intab_prd_endt")
sch_nm = utils.readFilePath("sch_nm")
tab_nm = utils.readFilePath("tab_nm")
fpath = utils.readFilePath("fpath")

directory = os.getcwd()
print("Current working directory : %s" % directory)
# Now change the directory
os.chdir( fpath )
directory = os.getcwd()
print("Directory changed successfully to : %s" % directory)
dttm = utils.get_dttm().strftime("%Y-%m-%d_%H-%M-%S")
#filename = os.path.basename(__file__)
fname = os.path.basename(__file__).replace(".py","")
print("Log filename is : %s" % fname)
path_to_file = directory + '/' + fname + '_' + dttm + ".log"
utils.create_logfile(path_to_file)
stdttm = utils.get_dttm()
utils.start_end("start")

# Main functionality


def main(intab_prd_id, intab_prd_stdt, intab_prd_endt, sch_nm, tab_nm):
    """
    Describe NOL US Sample transformations and actions here.
    """
    # spark is an existing SparkSession


try:
    utils.writeToLog("Running NOL Sample Steps")
    rpt_samples_etl = get_nol_sample_sql(intab_prd_id, sch_nm)

    rpt_samples_etl_sdf = utils.build_dataframe(spark, lambda: get_nol_sample_sql(intab_prd_id, sch_nm))
    rpt_samples_etl_sdf.registerTempTable(tab_nm)
    dst_cnt = rpt_samples_etl_sdf.count()

    utils.writeToLog("rpt_samples_etl_sdf - {}".format(rpt_samples_etl_sdf))



except Exception as e:
    utils.writeToLog("Unable to fetch required data for sample criteria from database"  + str(e))
    exit(1)

#rpt_samples_etl_sdf.repartition(2).write.csv("s3://useast1-nlsn-mdl-w-tam-totalmediafusion-dev/us_sample_201802.csv")
#rpt_samples_etl_sdf.repartition(2).write.format("com.databricks.spark.csv").option("header", "true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").mode('overwrite').csv("s3://useast1-nlsn-mdl-w-tam-totalmediafusion-dev/us_sample_201803.csv")
logging.warning( "File has been written in s3 location")
# spark.catalog.dropTempView(tab_nm)
logging.warning( "Temp View is dropped")
utils.writeToLog("++++++++++++++++++++++++++++++++SUMMARY LOG++++++++++++++++++++++++++++++++++++++++++++++++++++")
utils.writeToLog("FileName                  : %s.py " %fname)
utils.writeToLog("Process Start time        : %s" %stdttm)
utils.writeToLog("final data count          : %s" %dst_cnt)
endtm = utils.get_dttm()
utils.writeToLog("Process End time          : %s" %endtm)
utils.writeToLog("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
utils.start_end( "end")

if __name__ == "__main__":
    # Execute main functionality
    main(intab_prd_id, intab_prd_stdt, intab_prd_endt, sch_nm, tab_nm)
    # stop the SparkSession object
    spark.stop()
    utils.writeToLog( "NOL US Sample Application Completed")

