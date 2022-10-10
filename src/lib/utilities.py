import configparser

from pyspark import SparkConf

def get_sparkConf():

    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (k,v) in config.items('Spark_Configuration'):
        spark_conf.set(k,v)
        print(type(spark_conf))
        print(spark_conf)

    return spark_conf

def get_hiveTable(spark):
    data = [{"Category": 'A', "ID": 1, "Value": 121.44, "Truth": True},
        {"Category": 'B', "ID": 2, "Value": 300.01, "Truth": False},
        {"Category": 'C', "ID": 3, "Value": 10.99, "Truth": None},
        {"Category": 'E', "ID": 4, "Value": 33.87, "Truth": True}
        ]
    df = spark.createDataFrame(data)
    df.write.format('parquet').saveAsTable('default'+'.'+'test')
#     fetch_sql = """use mysourcedb;
# create table corona_pandemic_table(
# province_state  VARCHAR(45),
# country_region VARCHAR(45),
# date DATE,
# latitude FLOAT,
# longitude FLOAT,
# sub_region1_name VARCHAR(45),
# location_geom VARCHAR(45),
# confirmed INTEGER,
# deaths INTEGER,
# recovered INTEGER,
# active INTEGER);"""
    fetch_sql = "select * from default.test"
    print(fetch_sql)
    #Run sql and retrieve data frame elements as an array
    table_res = spark.sql(fetch_sql).collect()
    # Loop through the result set and printing the each column values
    for row in table_res:
        print(row)

    print("for loop is exit")
