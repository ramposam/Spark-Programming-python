from pyspark.sql import SparkSession
from com.rposam.pyspark.config.SparkConfigurations import getSparkConf
from com.rposam.pyspark.config.logger import Log4j
import sys

if __name__ == "__main__":
    conf = getSparkConf()
    spark = SparkSession.builder.config(conf=conf).appName("Read AVRO Write to HDFS").getOrCreate()
    # logger = Log4j(spark)
    # logger.info("Starting the pyspark application")
    inputDir = sys.argv[1]
    outputDir = sys.argv[2]
    # logger.info(f"input Dir:{inputDir} , output Dir: {outputDir}")
    # logger.info("file reading started...")
    df = spark.read.format("avro").option("delimiter", "\t").option("inferschema", "true").load(inputDir)
    # logger.info("Schema: " + df.schema.simpleString())
    df.show()
    df.write.format("parquet").option("delimiter", ",").option("header", "true").save(outputDir)
    spark.stop()
    # logger.info("Spark application stopped")