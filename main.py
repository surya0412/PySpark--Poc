import os
import sys
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql import *
from pyspark.sql.types import *

# Adding Pyspark job dependency files (zip)
if os.path.exists('src.zip'):
        sys.path.insert(0,'src.zip')
else:
        sys.path.insert(0,'./src')

from lib import logger

if __name__ == "__main__":

        file_nm = sys.argv[1]

        schema = StructType([
        StructField("province_state", StringType()),
        StructField("country_region", StringType()),
        StructField("date", DateType()),
        StructField("latitude", FloatType()),
        StructField("longitude", FloatType()),
        StructField("sub_region1_name", StringType()),
        StructField("location_geom:", StringType()),
        StructField("confirmed", IntegerType()),
        StructField("deaths", IntegerType()),
        StructField("recovered", IntegerType()),
        StructField("active", IntegerType()),
        ])

        spark = SparkSession.builder\
                .appName("pyspark-poc")\
                .enableHiveSupport()\
                .master("local")\
                .getOrCreate()
        # spark.sparkContext.setLogLevel("INFO")

        logger = logger.Log4j(spark)
        logger.info("Spark Session Started")

        path = "gs://cloud_function_trigger/"+file_nm
        # path = "file:///C:/Users/002V42744/Downloads/2.csv"

        df = spark.read.csv(path,header=True,schema=schema)
        # df.show(5)
        # df.printSchema()

        # grouping data based on state and date, returns each day results for each state
        grouped_df = df.groupBy("province_state","date").sum('confirmed','deaths','recovered','active').orderBy("province_state","date")

        # Defining a Window function for calculating cumulative active counts for last 7 days
        window = Window.partitionBy("province_state").orderBy("date").rowsBetween(-7, 0)
        window_df = grouped_df.select("*",sum('sum(active)').over(window).alias("window_active"))

        # Adding extar column "YYYY-MM" which will become easy for partition.
        year_month_df = window_df.select("*",date_format("date", "yyyy-MM").alias("YYYY-MM"))

        # Write to CSV with partition on state and Year Month.
        year_month_df.write.option("header", True) \
                        .partitionBy("province_state","YYYY-MM")\
                        .mode("overwrite")\
                        .csv("gs://dataproc-gs/pyspark-job/output/")

        # year_month_df.write.saveAsTable('sampleStudentTable')

        logger.info("Spark session ended")
        # spark.stop()