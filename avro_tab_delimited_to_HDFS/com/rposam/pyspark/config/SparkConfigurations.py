from pyspark import SparkConf
import configparser

def getSparkConf():
    conf = SparkConf()
    sparkConfig = configparser.ConfigParser()
    sparkConfig.read("spark.conf")
    for name,value in sparkConfig.items("SPARK_APP_CONFIGS"):
        conf.set(name,value)
    return conf
