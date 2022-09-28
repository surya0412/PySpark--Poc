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
