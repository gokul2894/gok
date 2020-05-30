# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC 
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/Emergency_Calls.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

df.count(),len(df.columns)

# COMMAND ----------

df.dtypes

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.columns

# COMMAND ----------

col=['lat',
 'lng',
 'desc',
 'zip',
 'title',
 'twp',
 'addr',
 'e',
 'Reason',
 'Code',
 'month',
 'year',
 'day',
 'date']

# COMMAND ----------

for co in col:
    print(co,'=',df.filter((df[co] == "") | df[co].isNull() | isnan(df[co])).count())


# COMMAND ----------

df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()


# COMMAND ----------

display(df.describe())

# COMMAND ----------

df.describe().show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

display(df.select('twp').distinct())

# COMMAND ----------

df.select('twp').distinct().count()

# COMMAND ----------

df.select(countDistinct(df.twp)).show()

# COMMAND ----------

df.groupBy('twp').count().show()

# COMMAND ----------

display(df.groupBy('twp').count().orderBy(col('count'),ascending=False).head(5))

# COMMAND ----------

display(df.filter(df.zip.isNotNull()).groupBy('zip').count().orderBy(col('count'),ascending=False).head(5))

# COMMAND ----------

df=df.withColumn('Reason',split(df.title,': ')[0]).withColumn('Code',split(df.title,': ')[1])
display(df)

# COMMAND ----------

display(df.groupBy('Reason').count())

# COMMAND ----------

df=df.withColumn('month',date_format(df.timeStamp,'MMMM'))

# COMMAND ----------

df=df.withColumn('year',date_format(df.timeStamp,'YYYY'))

# COMMAND ----------

df.stat.crosstab('year','month').show()

# COMMAND ----------

display(df.groupBy('month').count())

# COMMAND ----------

display(df.groupBy('month','Reason').count().orderBy('month','Reason'))

# COMMAND ----------

display(df.withColumn('dow',date_format(df.timeStamp,'u')))

# COMMAND ----------

df=df.withColumn('day',date_format(df.timeStamp,'EEEE'))

# COMMAND ----------

display(df.groupBy('day','Reason').count().orderBy('day','Reason'))

# COMMAND ----------

df=df.withColumn('date',date_format(df.timeStamp,'yyyy-MM-dd'))

# COMMAND ----------

df1=df.groupBy('year','month','Reason').agg(count(df.twp)).orderBy('year')

# COMMAND ----------

df1.coalesce(1).write.format('com.databricks.spark.csv').save("/FileStore/tables/9112data.csv",header = 'true')


# COMMAND ----------

display(df.filter(df.Reason=='Fire').groupBy('date').agg(count(df.twp)).orderBy('date'))

# COMMAND ----------

display(df1)

# COMMAND ----------

display(df.filter(df.Reason=='EMS').groupBy('date').agg(count(df.twp)).orderBy('date'))

# COMMAND ----------

display(df.filter(df.Reason=='Traffic').groupBy('date').agg(count(df.twp)).orderBy('date'))

# COMMAND ----------

display(df.groupBy('Code').count().orderBy(col('count'),ascending=False).head(10))

# COMMAND ----------

display(df.stat.crosstab('day','month'))

# COMMAND ----------

df.filter(df.Reason=='EMS').count()

# COMMAND ----------

display(df.select('Code'))

# COMMAND ----------

regex_string='\\ -'
display(df.select('Code',regexp_replace('Code',regex_string,'').alias('Code1')))

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# Create a view or table

temp_table_name = "Emergency_Calls_csv"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC 
# MAGIC select * from `Emergency_Calls_csv`

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "Emergency_Calls_csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)
