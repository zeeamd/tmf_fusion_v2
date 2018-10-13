"""
File: nol_ustop200sites_channels.py
Author: KiranK

 This is master function for executing adaptor steps in ordered fashion

"""

# imports from Python Standard Library
import sys
from pyspark.sql.functions import regexp_replace
import logging
import os


# Initiate BD3 spark Log
from library.src.main.commonutils import utils  #generic utilities
# local dependencies
from extracts.src.main.commonsql.nol_adaptor_data_pull import get_nol_us_top200sites_channels_sql,get_report_credit_sql


spark = utils.spark_session("NOL US Top200 Sites Channels Integration")

sc = spark.sparkContext
# sc.setLogLevel("WARN")
spark.conf.set("spark.sql.shuffle.partitions", 2)

# parsing system arguments from Json File
intab_prd_id = utils.readFilePath("intab_prd_id")
intab_prd_stdt = utils.readFilePath("intab_prd_stdt")
intab_prd_endt = utils.readFilePath("intab_prd_endt")
sch_nm = utils.readFilePath("sch_nm")
tab_nm = utils.readFilePath("tab_nm")
brndvw_nm = utils.readFilePath("brndvw_nm")
chnvw_nm = utils.readFilePath("chnvw_nm")
parvw_nm = utils.readFilePath("parvw_nm")
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


def main(intab_prd_id, intab_prd_stdt, intab_prd_endt, sch_nm, tab_nm, chnvw_nm):
    """
    Describe NOL US Sample transformations and actions here.
    """
    # spark is an existing SparkSession


try:
    utils.writeToLog("Running NOL USTop200 Channel Sites Steps")
    rpt_factors_etl_sdf_frmt = get_report_credit_sql(sch_nm)

    rpt_factors_etl_sdf = utils.build_dataframe(spark, lambda: get_report_credit_sql(sch_nm))
    rpt_factors_etl_sdf.registerTempTable(tab_nm)

    utils.writeToLog("rpt_factors_etl_sdf - {}".format(rpt_factors_etl_sdf))

    rpt_channels_etl = get_nol_us_top200sites_channels_sql(intab_prd_id, intab_prd_stdt, intab_prd_endt, sch_nm, tab_nm, chnvw_nm)

    rpt_channels_etl_sdf = utils.build_dataframe(spark, lambda: get_nol_us_top200sites_channels_sql(intab_prd_id, intab_prd_stdt, intab_prd_endt, sch_nm, tab_nm, chnvw_nm))
    rpt_channels_etl_sdf.registerTempTable(chnvw_nm)
    #rpt_channels_etl_sdf.cache().show()

    utils.writeToLog("rpt_channels_etl_sdf - {}".format(rpt_channels_etl_sdf))

    sql = """select intab_period_id as period_id,
                    tree_id,
                    entity_name as tree_name,
                    entity_level_code as tree_level_code,
                    entity_level as tree_level,
                    unique_audience 
            from {chnvw_nm} 
            order by 6 desc limit 200 """ .format(chnvw_nm=chnvw_nm)

    final_df = spark.sql(sql)
    dst_cnt = final_df.count()

except Exception as e:
    utils.writeToLog( "-------------Failed/Error: {e} ----------------".format(e=e))
    exit(1)

#rpt_samples_etl_sdf.repartition(2).write.csv("s3://useast1-nlsn-mdl-w-tam-totalmediafusion-dev/us_sample_201802.csv")
final_df.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss").mode('overwrite').csv("s3://useast1-nlsn-mdl-w-tam-totalmediafusion-dev/us_top200_sites_channels_20181008")
utils.writeToLog( "File has been written in s3 location")
# spark.catalog.dropTempView(tab_nm)
utils.writeToLog( "Temp View is dropped")
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
    main(intab_prd_id, intab_prd_stdt, intab_prd_endt, sch_nm, tab_nm, chnvw_nm)
    # stop the SparkSession object
    spark.stop()
    utils.writeToLog( "NOL US Top200 Sites Channels Application Completed")
