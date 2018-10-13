# imports from Python Standard Library
import datetime
import logging
import json
import os


# imports downloaded from PyPi
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from functools import reduce
from dateutil.parser import parse


def spark_context(mode,app_name):
    sparkContext = SparkContext(mode,app_name)
    return sparkContext


def spark_session(app_name):
    sparkSession = SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()
    return sparkSession


def readFilePath(key):
    file_path = os.getcwd()
    with open(file_path + '/nol_config.json', 'r') as f:
        config = json.load(f)
        for i in key.split("."):
            if i in config:
                config = config[i]
            else:
                return None
        return config


lvl = readFilePath("level")


def get_dttm():
    dttm = datetime.datetime.now()
    return dttm


def create_logfile(filename):
    logging.basicConfig(filename=filename, format='%(asctime)s [STATUS] - %(lineno)d - %(message)s')
    #logger = logging.getLogger()
    #handler = handlers.RotatingFileHandler(filename, maxBytes=1024*100, backupCount=5)
    #logger.addHandler(handler)


def get_level(level):
    levels = {

        'verbose': logging.warning,
        'simple': logging.error,
        'error': logging.info
    }

    if level == 'simple':
        logger = levels['simple']
    elif level == 'verbose':
        logger = levels['verbose']
    else:
        logger = levels['error']
        print("Set level as 'simple' or 'verbose' ")
        logger("Set level as 'simple' or 'verbose' ")
        exit()
    return logger


logger = get_level(lvl)

def writeToLog(message):
    #logg = get_level(logger)
    logger(message)


# logging.exception(message)


def build_dataframe(spark, sqlQuery):
    temp = sqlQuery()
    df = spark.sql(temp)
    # spark.catalog.dropTempView("tab_nm")
    # df.createOrReplaceTempView("tab_nm")
    # cnt = df.count
    return df

def union_many(*dfs):
    #this function can have as many dataframes as you want passed into it
    #the asterics before the name `dfs` tells Python that `dfs` will be a list
    #containing all of the arguments we pass into union_many when it is called

    return reduce(DataFrame.union, dfs)


class ADLHost:

    def parseAdlHost(self):
        tmdev = "dev"
        tmuat = "uat"
        tmpreprod = "preprod"
        tmbreakfix = "breakfix"
        self.tmdev = tmdev
        self.tmuat = tmuat
        self.tmpreprod = tmpreprod
        self.tmbreakfix = tmbreakfix
        return self


def getAdlHost(self, env):
    if env.lower() == 'dev':
        return self.tmdev.format(env = env.lower())
    if env.lower() == 'uat':
        return self.tmuat.format(env = env.lower())
    if env.lower() == 'preprod':
        return self.tmpreprod.format(env = env.lower())
    if env.lower() == 'breakfix':
        return self.tmbreakfix.format(env = env.lower())


# Check whether a particular string could represent a date
def is_date(date_text):
    try:
        parse(date_text)
        return True
    except ValueError:
        return False


def date_format_validate(date_text, date_format):
    try:
        datetime.datetime.strptime(date_text, date_format)
        return True
    except ValueError:
        return False


def start_end(st_end):
    #logger = get_level(level)

    logger("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    logger('Logging ' + st_end + ' at :' + str(get_dttm().strftime("%Y-%m-%d %H:%M:%S")))
    logger("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
