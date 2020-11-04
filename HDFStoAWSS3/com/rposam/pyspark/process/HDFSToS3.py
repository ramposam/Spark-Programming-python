import sys
from pyspark.sql.functions import col, from_unixtime
from pyspark.sql import SparkSession
from pyspark import SparkContext

from com.rposam.pyspark.config.SparkConfig import getSparkConfig
from com.rposam.pyspark.log.Log4j import Log4j
import re


def reNameColumns(cols):
    reNamedCols = []
    for col in cols:
        # Repalce all special symbols,spaces with _
        reNamedCols.append(re.sub("[^a-zA-Z0-9]", "_", col.lower()))
    return reNamedCols


if __name__ == "__main__":
    conf = getSparkConfig()
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    inputDir = sys.argv[1]
    AWS_ACCESS_KEY = sys.argv[2]
    AWS_SECRET_KEY = sys.argv[3]

    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key",AWS_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.secret.key", AWS_SECRET_KEY)
    hadoop_conf.set("spark.hadoop.fs.s3a.endpoint","s3.us-east-2.amazonaws.com")

    logger = Log4j(spark)
    logger.info("Starting spark session...")

    logger.info("passing input directory and aws credntials as an arguments ")

    df = spark.read.format("json") \
        .load(inputDir)

    logger.info("Acutal columns: {}".format(df.schema.simpleString()))
    cols = reNameColumns(df.columns)

    jsonDF = df.toDF(*cols)

    logger.info("Renamed columns:{}".format(jsonDF.schema.simpleString()))

    logger.info("Schema definition:{}".format(jsonDF.schema))

    dfNew = jsonDF. \
        withColumn("earnings_dt", from_unixtime(
        col("earnings_date.`$date`").cast("long"))). \
        withColumn("id", col("`_id`.`$oid`")). \
        drop("earnings_date", "_id")
    logger.info("Write dataframe result to AWS S3")
    dfNew.write.format("csv").option("compression", "gzip").option("path","s3a://aws-s3-bucket-spark-test/output/stocks").save()

    logger.info("Stopping spark session..")
    spark.stop()
