"""
File: nol_us200parents.py
Author: KiranK

 This is master function for executing adaptor steps in ordered fashion

"""

# imports from Python Standard Library
import sys
import os
import logging

# Initiate BD3 spark Log
from library.src.main.commonutils import utils # generic utilities
# local dependencies
from extracts.src.main.commonsql.nol_adaptor_data_pull import get_report_credit_sql, get_nol200Parents_sql, get_nol_aggr_top200parents_sql

spark = utils.spark_session("NOL Top200 Parents Integration")

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
    Describe NOL US 200 Channels transformations and actions here.
    """
    # spark is an existing SparkSession


try:
    utils.writeToLog("Running NOL top 200 parents Steps")
    rpt_factors_etl_sdf_frmt = get_report_credit_sql(sch_nm)

    rpt_factors_etl_sdf = utils.build_dataframe(spark, lambda: get_report_credit_sql(sch_nm))
    rpt_factors_etl_sdf.createOrReplaceTempView(tab_nm)

    utils.writeToLog("rpt_factors_etl_sdf - {}".format(rpt_factors_etl_sdf))

    proj_factors_sdf_frmt = get_nol200Parents_sql(intab_prd_id, intab_prd_stdt, intab_prd_endt, sch_nm, tab_nm)
    proj_factors_sdf = utils.build_dataframe(spark, lambda: get_nol200Parents_sql(intab_prd_id, intab_prd_stdt,
                                                                                  intab_prd_endt, sch_nm, tab_nm))
    proj_factors_sdf.registerTempTable(tab_nm)

    utils.writeToLog("proj_factors_sdf - {}".format(proj_factors_sdf))

    nol_parents_sdf_frmt = get_nol_aggr_top200parents_sql(tab_nm)
    nol_parents_aggr_sdf = utils.build_dataframe(spark, lambda: get_nol_aggr_top200parents_sql(tab_nm))
    spark.catalog.dropTempView(tab_nm)
    nol_parents_aggr_sdf.registerTempTable(tab_nm)
    dst_cnt = nol_parents_aggr_sdf.count()
    utils.writeToLog("nol_parents_aggr_sdf - {}".format(nol_parents_aggr_sdf))


except Exception as e:
    utils.writeToLog(str(e) +"Unable to fetch required data for eligibility criteria from database")
    exit(1)

nol_parents_aggr_sdf.repartition(2).write.format("com.databricks.spark.csv").option("header", "true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").mode('overwrite').csv("s3://useast1-nlsn-mdl-w-tam-totalmediafusion-dev/us_200parents_201821")
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
utils.start_end("end")

if __name__ == "__main__":
    # Execute main functionality
    main(intab_prd_id, intab_prd_stdt, intab_prd_endt, sch_nm, tab_nm)
    # stop the SparkSession object
    spark.stop()
    utils.writeToLog("NOL Top 200 Parents Application Completed")
