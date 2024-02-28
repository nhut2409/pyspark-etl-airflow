import os
import pyspark
from pyspark.sql import SparkSession
import findspark
findspark.init()
# 
# Configuring pyspark
# 
def spark_conf(access_id, access_key):
    os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:3.3.2 pyspark-shell"

    sc=pyspark.SparkContext()
    sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")

    hadoop_conf=sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
    hadoop_conf.set("fs.s3a.access.key", access_id)
    hadoop_conf.set("fs.s3a.secret.key", access_key)
    hadoop_conf.set("fs.s3a.connection.maximum", "100000")
    hadoop_conf.set("fs.s3a.fast.upload", "true")
    hadoop_conf.set("fs.s3a.fast.upload.buffer", "bytebuffer")
    # hadoop_conf.set("fs.s3a.endpoint", "s3." + aws_region + ".amazonaws.com")

    # Build a spark session
    spark = SparkSession.builder.config('pspark.jars','postgresql-42.7.2.jar').getOrCreate()
    return spark