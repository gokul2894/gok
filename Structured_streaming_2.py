# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

userSchema = StructType([ StructField("desc",StringType(), True), StructField("zip", IntegerType(), True),StructField("title",StringType(), True),StructField("timeStamp",StringType(), True),StructField("twp",StringType(), True),StructField("addr",StringType(), True) ])


# COMMAND ----------

fileStreamDf = spark.readStream.option("header", "true").schema(userSchema).csv("/FileStore/tables/")

# COMMAND ----------

fileStreamDf.createOrReplaceTempView("salary")

# COMMAND ----------

totalSalary = spark.sql("select count(title),title from salary group by title")

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "2")  # keep the size of shuffles small

query = (
     totalSalary
    .writeStream
    .format("console")        # memory = store in-memory table (for testing only in Spark 2.0)
    .queryName("totalSalary")     # counts = name of the in-memory table
    .outputMode("complete")  # complete = all the counts should be in the table
    .start()
)

# COMMAND ----------

from pyspark.sql.functions import unix_timestamp, from_unixtime
from datetime import date

# COMMAND ----------

Query = (
   fileStreamDf \
  .writeStream \
  .format("parquet") \
  .option("path","/FileStore/tables/") \
  .option("checkpointLocation", "/FileStore/")\
  .trigger(processingTime="10 seconds")
  .start()
)

# COMMAND ----------

sql("select * from parquet.`{}`".format("/FileStore/tables/")).count()

# COMMAND ----------

# MAGIC %sql select count(title),title from salary group by title

# COMMAND ----------

# MAGIC %fs rm -r FileStore/tables
