"""
File: nol_us_demo.py
Author: KiranK

 This is master function for executing adaptor steps in ordered fashion

"""

# imports from Python Standard Library
import sys
import os
import logging

# local dependencies
from extracts.src.main.commonsql.nol_adaptor_data_pull import get_metered_person_wt_sql, \
    get_metered_person_1mth_sql, \
    get_metered_person_dedups_sql, \
    get_metered_person_demo_fnl_sql

# Initiate BD3 spark Log
from library.src.main.commonutils import utils  #generic utilities

spark = utils.spark_session("Pyspark NOL Demographics Application integration")

sc = spark.sparkContext
# sc.setLogLevel("WARN")
spark.conf.set("spark.sql.shuffle.partitions", 2)

# check number of argumnets
# if (len(sys.argv) - 1) != 5:
#     raise Exception(
#         'Total number of argunets wrong. Expected 8 (fintab_prd_id, intab_prd_stdt, intab_prd_endt, sch_nm, tab_nm) but passed {0}'.format(
#             len(sys.argv) - 1))
#     exit(1)
#
# # parsing system arguments
# intab_prd_id = sys.argv[1]
# intab_prd_id = intab_prd_id.split("=")[1]
# intab_prd_stdt = sys.argv[2]
# intab_prd_stdt = intab_prd_stdt.split("=")[1]
# intab_prd_endt = sys.argv[3]
# intab_prd_endt = intab_prd_endt.split("=")[1]
# sch_nm = sys.argv[4]
# sch_nm = sch_nm.split("=")[1]
# tab_nm = sys.argv[5]
# tab_nm = tab_nm.split("=")[1]

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


## Main functionality
def main(intab_prd_id, intab_prd_stdt, intab_prd_endt, sch_nm, tab_nm):
    """
    Describe NOL US demos transformations and actions here.
    """
    # spark is an existing SparkSession


try:
    utils.writeToLog("Running NOL Demographics Steps")
    rpt_factors_etl_sdf_frmt = get_metered_person_wt_sql(sch_nm)

    rpt_factors_etl_sdf = utils.build_dataframe(spark, lambda: get_metered_person_wt_sql(sch_nm))
    rpt_factors_etl_sdf.createOrReplaceTempView(tab_nm)

    utils.writeToLog("rpt_factors_etl_sdf - {}".format(rpt_factors_etl_sdf))

    proj_factors_sdf_frmt = get_metered_person_1mth_sql(intab_prd_stdt, intab_prd_endt, tab_nm)
    proj_factors_sdf = utils.build_dataframe(spark,
                                             lambda: get_metered_person_1mth_sql(intab_prd_stdt, intab_prd_endt,
                                                                                 tab_nm))
    proj_factors_sdf.createOrReplaceTempView(tab_nm)
    # proj_factors_sdf.show()

    utils.writeToLog("proj_factors_sdf - {}".format(proj_factors_sdf))

    etl_metered_pw_dedups_sql = get_metered_person_dedups_sql(tab_nm)
    etl_metered_pw_dedups_sdf = utils.build_dataframe(spark, lambda: get_metered_person_dedups_sql(tab_nm))
    etl_metered_pw_dedups_sdf.createOrReplaceTempView(tab_nm)

    utils.writeToLog("etl_metered_pw_dedups_sdf - {}".format(etl_metered_pw_dedups_sdf))

    etl_metered_pw_final_sql = get_metered_person_demo_fnl_sql(sch_nm, tab_nm)
    etl_metered_pw_final_sdf = utils.build_dataframe(spark,
                                                     lambda: get_metered_person_demo_fnl_sql(sch_nm, tab_nm))
    etl_metered_pw_final_sdf.createOrReplaceTempView(tab_nm)
    dst_cnt = etl_metered_pw_final_sdf.count()

    utils.writeToLog("etl_metered_pw_final_sdf - {}".format(etl_metered_pw_final_sdf))

except Exception:
    utils.writeToLog("Unable to fetch required data for eligibility criteria from database")
    exit(1)

etl_metered_pw_final_sdf.repartition(2).write.format("com.databricks.spark.csv").option("header", "true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").mode('overwrite').csv("s3://useast1-nlsn-mdl-w-tam-totalmediafusion-dev/us_demos_201827")
#etl_metered_pw_final_sdf.write.csv("s3://useast1-nlsn-mdl-w-tam-totalmediafusion-dev/us_demos_201827.csv")
utils.writeToLog("File has been written in s3 location")
# spark.catalog.dropTempView(tab_nm)
utils.writeToLog("Temp View is dropped")
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
    utils.writeToLog("NOL US Demos Demogrpahics Application Completed")
