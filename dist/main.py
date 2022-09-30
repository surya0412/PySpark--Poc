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

        # spark_conf = utilities.get_sparkConf()
        # spark = SparkSession.builder\
        #         .config(conf = spark_conf)\
        #         .getOrCreate()

        spark = SparkSession.builder\
                .appName("pyspark-poc")\
                .master("local")\
                .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        logger = logger.Log4j(spark)

        logger.info("Spark Session Started")

        print(10/10)
        print("Hello")
        utilities.get_hiveTable(spark)
        logger.info("Spark session ended")
        # spark.stop()
