from pyspark.sql import SparkSession
from com.rposam.pyspark.config.SparkConfigurations import getSparkConf
from com.rposam.pyspark.config.logger import Log4j
from com.rposam.pyspark.schema.empschema import getEmpSchema
import sys

if __name__ == "__main__":
    conf = getSparkConf()
    schema= getEmpSchema()
    spark = SparkSession.builder.config(conf=conf).appName("Read csv Write as Avro Tab delimiter").getOrCreate()
    # logger = Log4j(spark)
    # logger.info("Starting the pyspark application")
    inputDir = sys.argv[1]
    outputDir = sys.argv[2]
    # logger.info(f"input Dir:{inputDir} , output Dir: {outputDir}")
    # logger.info("file reading started...")
    df = spark.read.format("csv").option("inferschema", "true").schema(schema).load(inputDir)
    # logger.info("Schema: " + df.schema.simpleString())
    df.show(10)
    df.write.format("avro").option("delimiter","\t").option("header","true").save(outputDir)
    spark.stop()
    # logger.info("Spark application stopped")