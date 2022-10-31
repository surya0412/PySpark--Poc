import os
import sys

if os.path.exists('src.zip'):
        sys.path.insert(0,'src.zip')
else:
        sys.path.insert(0,'./src')

from lib import utilities
from lib import logger

if __name__ == "__main__":

        from pyspark.sql import *

        spark = SparkSession.builder\
                .appName("pyspark-poc")\
                .enableHiveSupport()\
                .master("local")\
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        logger = logger.Log4j(spark)

        logger.info("Spark Session Started")
        # spark.read("")
        # path = "gs://dataproc-gs/sql-data/1.csv"
        # options = {"credentials":"credential.base64ServiceAccount", "parentProject":"credential.projectId"}
        # df = spark.read.csv(path,header=True)
        # grouped_df = df.groupBy("province_state").sum('confirmed','deaths','recovered').collect()
        # # .options(options)\
        
        # print(grouped_df)
        print(10/10)
        print("Hello")
        # utilities.get_hiveTable(spark)
        logger.info("Spark session ended")
        # spark.stop()
