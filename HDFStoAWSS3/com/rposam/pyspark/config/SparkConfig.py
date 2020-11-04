import configparser as cp

from pyspark import SparkConf

def getSparkConfig():
    spark_conf = SparkConf()
    conf = cp.ConfigParser()
    conf.read("spark.conf")
    for property, value in conf.items("SPARK_APP_CONFIGS"):
        spark_conf.set(property, value)
    return spark_conf

