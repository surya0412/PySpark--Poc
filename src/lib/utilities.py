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

    fetch_sql = "select * from random_numbers"
    print(fetch_sql)
    #Run sql and retrieve data frame elements as an array
    table_res = spark.sql(fetch_sql).collect()
    # Loop through the result set and printing the each column values
    for row in table_res:
        # car_model_name = row["car_model"]
        # car_price = row["price_in_usd"]

        # print("car model name : " + car_model_name)
        # print("car price : " + car_price)
        print(row)

    print("for loop is exit")
