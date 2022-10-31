import os
from pydoc import describe
import sys
from tokenize import String
from xml.dom.minicompat import StringTypes

if os.path.exists('src.zip'):
        sys.path.insert(0,'src.zip')
else:
        sys.path.insert(0,'./src')

from lib import utilities
from lib import logger

if __name__ == "__main__":

        from pyspark.sql import *
        from pyspark.sql.types import *

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
        spark.sparkContext.setLogLevel("ERROR")

        logger = logger.Log4j(spark)

        logger.info("Spark Session Started")
        # spark.read("")
        # path = "gs://dataproc-gs/sql-data/1.csv"
        path = "file:///C:/Users/002V42744/Downloads/1.csv"
        options = {"credentials":"credential.base64ServiceAccount", "parentProject":"credential.projectId"}
        df = spark.read.csv(path,header=True,schema=schema)
        df.show(5)
        # df.printSchema()
        print(df.describe("date").show())
        
        grouped_df = df.groupBy("province_state").sum('confirmed','deaths','recovered','active')
        display_df = grouped_df.select("*").orderBy("sum(confirmed)","sum(deaths)","sum(recovered)","sum(active)", ascending=False)
        display_df.show(n=50)
        # print("province_state" + ', ' + "sum(confirmed)" + ', ' + "sum(deaths)" + ', '+ "sum(recovered)" + ', '+ "sum(active)")
        # for i in grouped_df:
        #         print(i["province_state"] + '\t \t \t \t \t \t, ' + str(i["sum(confirmed)"]) + '\t, ' + str(i["sum(deaths)"]) + '\t, '+ str(i["sum(recovered)"]) + '\t, '+ str(i["sum(active)"]))
        # utilities.get_hiveTable(spark)
        logger.info("Spark session ended")
        # spark.stop()