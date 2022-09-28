from pyspark.sql import *
from lib.logger import Log4j
from lib.utils import get_sparkConf

if __name__ == "__main__":

        spark_conf = get_sparkConf()

        spark = SparkSession.builder\
                .config(conf = spark_conf)\
                .getOrCreate()

        logger = Log4j(spark)

        logger.info("Spark Session Started")

        print(10/10)

        logger.info("Spark session ended")
        # spark.stop()
