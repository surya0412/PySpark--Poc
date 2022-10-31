import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder\
                .appName("pyspark-poc")\
                .enableHiveSupport()\
                .master("local")\
                .getOrCreate()

spark.read()
print("Some")
# url = "jdbc:mysql://34.134.95.218:3306/test"
# driver = "com.mysql.jdbc.Driver"
# user = "root"
# password = "root"
# df =  spark.read\
#     .format("jdbc")\
#     .option("driver", driver)\
#     .option("url", url)\
#     .option("user", user)\
#     .option("password", password)\
#     .option("dbtable", "corona_pandemic_table")\
#     .load()

# df.count()


# jdbcDF = spark.read \
#     .format("jdbc")\
#     .options(
#     url="jdbc:mysql://34.134.95.218:3306/test",
#     # driver = "com.mysql.jdbc.Driver",
#     dbtable = "corona_pandemic_table",
#     user="root",
#     password="root")\
#     .load()
# print("Success")
# jdbcDF.write.parquet("dist/")