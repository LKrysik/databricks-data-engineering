# Databricks notebook source
# MAGIC %sql
# MAGIC -- Specify default database so we don't have to name it every time
# MAGIC 
# MAGIC USE flight_school_assignment

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sensor_readings_historical_labeled

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sensor_readings_raw_vmunged;
# MAGIC 
# MAGIC CREATE TABLE sensor_readings_raw_vmunged AS (
# MAGIC   SELECT 
# MAGIC     reading_time,
# MAGIC     device_type,
# MAGIC     device_id,
# MAGIC     device_operational_status,
# MAGIC     reading_1,
# MAGIC     reading_2,
# MAGIC     reading_3
# MAGIC   FROM sensor_readings_historical_labeled
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This is our starting-point data for creation of tables for flight school
# MAGIC 
# MAGIC SELECT * FROM sensor_readings_raw_vmunged

# COMMAND ----------

# Now my goal is to munge up reading_1 so that assignment 3 Machine Learning will work nicely

from pyspark.ml.feature import IndexToString, StringIndexer, VectorAssembler, OneHotEncoder
from pyspark.ml.classification import DecisionTreeClassifier

# 
# Read in the silver table as "raw" data to be munged

df_raw_data = spark.sql("""
  SELECT
    reading_time,
    device_type,
    device_id,
    device_operational_status,
    reading_1,
    reading_2,
    reading_3
  FROM sensor_readings_raw_vmunged
""")

# Create a numerical index of device_type values (it's a category, but Decision Trees only accept numerical input)
device_type_indexer = StringIndexer(inputCol="device_type", outputCol="device_type_index")
df_raw_data = device_type_indexer.fit(df_raw_data).transform(df_raw_data)

# Create a numerical index of device_id values (it's a category, but Decision Trees only accept numerical input)
device_id_indexer = StringIndexer(inputCol="device_id", outputCol="device_id_index")
df_raw_data = device_id_indexer.fit(df_raw_data).transform(df_raw_data)

# Create a numerical index of label values (device operational status) (it's a category, but Decision Trees only accept numerical input)
label_indexer = StringIndexer(inputCol="device_operational_status", outputCol="device_operational_status_index")
df_raw_data = label_indexer.fit(df_raw_data).transform(df_raw_data)

#df_raw_data.createOrReplaceTempView("vw_raw_data")

#df_raw_data = spark.sql("""
#  SELECT 
#    label_index AS label, 
#    device_type_index AS device_type,
#    device_id_index AS device_id,
#    reading_1,
#    reading_2,
#    reading_3
#  FROM vw_raw_data
#""")

display(df_raw_data)
df_raw_data.createOrReplaceTempView("vw_raw_data")

# COMMAND ----------

df_raw_data = spark.sql("""
  SELECT
    reading_time,
    device_type,
    device_id,
    device_operational_status,
    ((device_operational_status_index * 10) + reading_1 + (device_type_index * 10)) AS reading_1,
    reading_2,
    reading_3
  FROM vw_raw_data
""")

display(df_raw_data)

# COMMAND ----------

df_raw_data.createOrReplaceTempView("vw_raw_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vw_raw_data

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sensor_readings_raw_ready;
# MAGIC 
# MAGIC CREATE TABLE sensor_readings_raw_ready AS (
# MAGIC   SELECT
# MAGIC     reading_time,
# MAGIC     device_type,
# MAGIC     device_id,
# MAGIC     device_operational_status,
# MAGIC     reading_1,
# MAGIC     reading_2,
# MAGIC     reading_3
# MAGIC   FROM vw_raw_data
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sensor_readings_raw_ready

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This table enhances the raw data by adding row numbers
# MAGIC -- It's called "labeled" because it has the status columns populated (we'll be using this with ML)
# MAGIC 
# MAGIC DROP TABLE IF EXISTS sensor_readings_historical_labeled;
# MAGIC 
# MAGIC CREATE TABLE sensor_readings_historical_labeled 
# MAGIC USING DELTA
# MAGIC AS (
# MAGIC   SELECT
# MAGIC   row_number() OVER (ORDER BY reading_time ASC) AS id,
# MAGIC   *
# MAGIC   FROM sensor_readings_raw_ready
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Taking a peek at the data
# MAGIC 
# MAGIC SELECT * FROM sensor_readings_historical_labeled
# MAGIC ORDER BY reading_time ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now we create a table with a few hundred dirty records.
# MAGIC -- The premise is that failed measurements are persisted as 999.99
# MAGIC -- Flight School task will be to clean up these readings by averaging the prior and subsequent readings for that device (using functions LAG() and LEAD())
# MAGIC 
# MAGIC DROP TABLE IF EXISTS sensor_readings_historical_labeled_dirt;
# MAGIC 
# MAGIC CREATE TABLE sensor_readings_historical_labeled_dirt 
# MAGIC USING DELTA
# MAGIC AS (
# MAGIC   SELECT 
# MAGIC     id,
# MAGIC     reading_time,
# MAGIC     device_type,
# MAGIC     device_id,
# MAGIC     device_operational_status,
# MAGIC     999.99 AS reading_1,
# MAGIC     999.99 AS reading_2,
# MAGIC     999.99 AS reading_3
# MAGIC   FROM sensor_readings_historical_labeled
# MAGIC   WHERE MOD(id, 17000) = 0 -- select every 17,000th row... 367 records
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Taking a peek at the data
# MAGIC 
# MAGIC SELECT * FROM sensor_readings_historical_labeled_dirt

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now create a table that we can "dirty up" with a few invalid readings on the labeled historical file
# MAGIC 
# MAGIC DROP TABLE IF EXISTS sensor_readings_historical_labeled_dirty;
# MAGIC 
# MAGIC CREATE TABLE sensor_readings_historical_labeled_dirty 
# MAGIC USING DELTA
# MAGIC AS (
# MAGIC   SELECT * FROM sensor_readings_historical_labeled
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now add the dirt to the dirty labeled historical table
# MAGIC 
# MAGIC MERGE INTO sensor_readings_historical_labeled_dirty AS TARGET
# MAGIC USING sensor_readings_historical_labeled_dirt AS SOURCE
# MAGIC ON SOURCE.id = TARGET.id
# MAGIC WHEN MATCHED THEN UPDATE SET *

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Taking a peek at the data
# MAGIC 
# MAGIC SELECT * FROM sensor_readings_historical_labeled_dirty
# MAGIC WHERE ID = 17000

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sensor_readings_historical_labeled_dirty

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sensor_readings_historical_unlabeled;
# MAGIC 
# MAGIC CREATE TABLE sensor_readings_historical_unlabeled AS (
# MAGIC   SELECT
# MAGIC     id,
# MAGIC     reading_time,
# MAGIC     device_type,
# MAGIC     device_id,
# MAGIC     reading_1,
# MAGIC     reading_2,
# MAGIC     reading_3
# MAGIC   FROM sensor_readings_historical_labeled
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sensor_readings_historical_unlabeled

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now create the same dirty records to use on the unlabeled version of the historical table
# MAGIC 
# MAGIC DROP TABLE IF EXISTS sensor_readings_historical_unlabeled_dirt;
# MAGIC 
# MAGIC CREATE TABLE sensor_readings_historical_unlabeled_dirt 
# MAGIC USING DELTA
# MAGIC AS (
# MAGIC   SELECT 
# MAGIC     id,
# MAGIC     reading_time,
# MAGIC     device_type,
# MAGIC     device_id,
# MAGIC     999.99 AS reading_1,
# MAGIC     999.99 AS reading_2,
# MAGIC     999.99 AS reading_3
# MAGIC   FROM sensor_readings_historical_unlabeled
# MAGIC   WHERE MOD(id, 17000) = 0 -- select every 17,000th row... 367 records
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create an unlabeled historical table 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS sensor_readings_historical_unlabeled;
# MAGIC 
# MAGIC CREATE TABLE sensor_readings_historical_unlabeled 
# MAGIC USING DELTA
# MAGIC AS (
# MAGIC   SELECT
# MAGIC     id,
# MAGIC     reading_time,
# MAGIC     device_type,
# MAGIC     device_id,
# MAGIC     reading_1,
# MAGIC     reading_2,
# MAGIC     reading_3
# MAGIC   FROM sensor_readings_historical_labeled
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Taking a peek at our data
# MAGIC 
# MAGIC SELECT * FROM sensor_readings_historical_unlabeled
# MAGIC ORDER BY reading_time ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now create an unlabled historical table that we can "dirty up"
# MAGIC 
# MAGIC DROP TABLE IF EXISTS sensor_readings_historical_unlabeled_dirty;
# MAGIC 
# MAGIC CREATE TABLE sensor_readings_historical_unlabeled_dirty 
# MAGIC USING DELTA
# MAGIC AS (
# MAGIC   SELECT * FROM sensor_readings_historical_unlabeled
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now add the dirt to the dirty unlabeled historical table
# MAGIC 
# MAGIC MERGE INTO sensor_readings_historical_unlabeled_dirty AS TARGET
# MAGIC USING sensor_readings_historical_unlabeled_dirt AS SOURCE
# MAGIC ON SOURCE.id = TARGET.id
# MAGIC WHEN MATCHED THEN UPDATE SET *

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Taking a peek at our data
# MAGIC 
# MAGIC SELECT * FROM sensor_readings_historical_unlabeled_dirty
# MAGIC WHERE ID = 17000

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Taking a peek at our data
# MAGIC 
# MAGIC SELECT * FROM sensor_readings_historical_unlabeled_dirty

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now create a table of more current readings
# MAGIC -- We'll be using this for streaming
# MAGIC -- We start with the historical labeled data, then...
# MAGIC -- - take a fraction of the rows
# MAGIC -- - generate new row numbers
# MAGIC -- - generate new timestamps by adding 2 days and also benefitting from a rounding error on Seconds
# MAGIC 
# MAGIC DROP TABLE IF EXISTS sensor_readings_current_labeled;
# MAGIC 
# MAGIC CREATE TABLE sensor_readings_current_labeled 
# MAGIC USING DELTA
# MAGIC AS (
# MAGIC   SELECT 
# MAGIC     row_number() OVER (ORDER BY reading_time ASC) AS id,
# MAGIC     TIMESTAMP(DATE_ADD(reading_time, 2) || 'T' || HOUR(reading_time) || ':' || MINUTE(reading_time) || ':' || SECOND(reading_time)) AS reading_time, -- build new timestamp with 2 days added to the original date
# MAGIC                                                                                                                                                      -- also note that seconds will change from original due to rounding
# MAGIC     device_type,
# MAGIC     device_id,
# MAGIC     device_operational_status,
# MAGIC     reading_1,
# MAGIC     reading_2,
# MAGIC     reading_3
# MAGIC   FROM sensor_readings_historical_labeled
# MAGIC   WHERE MOD(id, 500) = 0  -- only take every 500th record
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Taking a peek at our data
# MAGIC 
# MAGIC SELECT * FROM sensor_readings_current_labeled

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now generate an unlabeled version of the current readings table
# MAGIC 
# MAGIC DROP TABLE IF EXISTS sensor_readings_current_unlabeled;
# MAGIC 
# MAGIC CREATE TABLE sensor_readings_current_unlabeled 
# MAGIC USING DELTA
# MAGIC AS (
# MAGIC   SELECT 
# MAGIC     id,
# MAGIC     reading_time,
# MAGIC     device_type,
# MAGIC     device_id,
# MAGIC     reading_1,
# MAGIC     reading_2,
# MAGIC     reading_3
# MAGIC   FROM sensor_readings_current_labeled
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Taking a peek at our data
# MAGIC 
# MAGIC SELECT * FROM sensor_readings_current_unlabeled

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE sensor_readings_historical_labeled_dirty

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now let's create the table that will be the starting point for students in Assignment 1
# MAGIC -- It's the historical table, labeled, dirty, with row numbers removed
# MAGIC 
# MAGIC DROP TABLE IF EXISTS assignment_1_ingest;
# MAGIC 
# MAGIC CREATE TABLE assignment_1_ingest AS (
# MAGIC   SELECT 
# MAGIC     UUID() AS id,
# MAGIC     reading_time,
# MAGIC     device_type,
# MAGIC     device_id,
# MAGIC     device_operational_status,
# MAGIC     reading_1,
# MAGIC     reading_2,
# MAGIC     reading_3
# MAGIC   FROM sensor_readings_historical_labeled_dirty
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Taking a peek at our data
# MAGIC 
# MAGIC SELECT * FROM assignment_1_ingest

# COMMAND ----------

# Taking a peek at our data to make sure dirt is present in every 17,000th row

df = spark.sql("""SELECT 
  ROW_NUMBER() OVER (ORDER BY reading_time ASC) AS temp_row,
  * 
FROM assignment_1_ingest""")

df.createOrReplaceTempView("tv")

df = spark.sql("SELECT * FROM tv WHERE MOD(temp_row, 17000) = 0")

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MIN(reading_time) FROM assignment_1_ingest

# COMMAND ----------

# Create a table with a small number of rows to be used for MERGE INTO against assignment_1_ingest
# We'll grab just 100 rows, then make 50 of them for update, 50 for insert

df = spark.sql("SELECT * FROM assignment_1_ingest LIMIT 100")
df.createOrReplaceTempView("backfill_vw")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM backfill_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS assignment_1_backfill;
# MAGIC 
# MAGIC CREATE TABLE assignment_1_backfill 
# MAGIC USING DELTA 
# MAGIC AS (
# MAGIC   SELECT * FROM backfill_vw
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM assignment_1_backfill

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create some NEW records for the backfill
# MAGIC UPDATE assignment_1_backfill
# MAGIC SET reading_time = TIMESTAMP(DATE_SUB(reading_time, 2) || 'T' || HOUR(reading_time) || ':' || MINUTE(reading_time) || ':' || SECOND(reading_time)),
# MAGIC     id = 'ZZZ' || id
# MAGIC WHERE device_type = 'TRANSFORMER'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM assignment_1_backfill

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create some UPDATE rows for the backfill
# MAGIC 
# MAGIC UPDATE assignment_1_backfill
# MAGIC SET reading_1 = reading_1 + 0.000000001
# MAGIC WHERE device_type = 'RECTIFIER'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from assignment_1_backfill

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create downloadable flat-file versions of our tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Define default database.  This is useful if we are skipping the above table generation, and just generating flat files.
# MAGIC 
# MAGIC USE flight_school_assignment

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sensor_readings_raw

# COMMAND ----------

# Here is our first download generation.  
# - Delete the file if it exists from a previous run
# - Read in the raw table to a dataframe
# - coalesce the dataframe to a single file and write it

dbutils.fs.rm("dbfs:/FileStore/bk/sensor_readings_raw.csv", True)
df = spark.sql("SELECT * FROM sensor_readings_raw")
df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/FileStore/bk/sensor_readings_raw.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Now you can download the coalesced file from UI with a URL something like this:
# MAGIC 
# MAGIC https://adb-8245268741408838.18.azuredatabricks.net/files/bk/sensor_raw.csv/part-00000-tid-1081972225373532573-356772f8-4c56-45fc-996d-1527985c7f6c-4908-1-c000.csv?o=8245268741408838 
# MAGIC 
# MAGIC NOTE that "files" replaces the UI name "FileStore"
# MAGIC NOTE that you must download the part-0 file, not the parent directory.  After downloading, you can change the file name to something shorter.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM assignment_1_backfill

# COMMAND ----------

# Now save a downloadable version of sensor_readings_historical

dbutils.fs.rm("dbfs:/FileStore/bk/sensor_readings_historical_labeled.csv", True)
df = spark.sql("SELECT * FROM sensor_readings_historical_labeled")
df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/FileStore/bk/sensor_readings_historical_labeled.csv")

# COMMAND ----------

# Now save a downloadable version of sensor_readings_historical_unlabeled

dbutils.fs.rm("dbfs:/FileStore/bk/sensor_readings_historical_unlabeled.csv", True)
df = spark.sql("SELECT * FROM sensor_readings_historical_unlabeled")
df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/FileStore/bk/sensor_readings_historical_unlabeled.csv")

# COMMAND ----------

# Now save a downloadable version of sensor_readings_historical_labeled_dirt

dbutils.fs.rm("dbfs:/FileStore/bk/sensor_readings_historical_labeled_dirt.csv", True)
df = spark.sql("SELECT * FROM sensor_readings_historical_labeled_dirt")
df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/FileStore/bk/sensor_readings_historical_labeled_dirt.csv")

# COMMAND ----------

# Now save a downloadable version of sensor_readings_historical_unlabeled_dirt

dbutils.fs.rm("dbfs:/FileStore/bk/sensor_readings_historical_unlabeled_dirt.csv", True)
df = spark.sql("SELECT * FROM sensor_readings_historical_unlabeled_dirt")
df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/FileStore/bk/sensor_readings_historical_unlabeled_dirt.csv")

# COMMAND ----------

# Now save a downloadable version of sensor_readings_current_labeled

dbutils.fs.rm("dbfs:/FileStore/bk/sensor_readings_current_labeled.csv", True)
df = spark.sql("SELECT * FROM sensor_readings_current_labeled")
df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/FileStore/bk/sensor_readings_current_labeled.csv")

# COMMAND ----------

# Now save a downloadable version of sensor_readings_current_unlabeled

dbutils.fs.rm("dbfs:/FileStore/bk/sensor_readings_current_unlabeled.csv", True)
df = spark.sql("SELECT * FROM sensor_readings_current_unlabeled")
df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/FileStore/bk/sensor_readings_current_unlabeled.csv")

# COMMAND ----------

# Now save a downloadable version of sensor_readings_historical_unlabeled_dirty

dbutils.fs.rm("dbfs:/FileStore/bk/sensor_readings_historical_unlabeled_dirty.csv", True)
df = spark.sql("SELECT * FROM sensor_readings_historical_unlabeled_dirty")
df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/FileStore/bk/sensor_readings_historical_unlabeled_dirty.csv")

# COMMAND ----------

# Now save a downloadable version of sensor_readings_historical_labeled_dirty

dbutils.fs.rm("dbfs:/FileStore/bk/sensor_readings_historical_labeled_dirty.csv", True)
df = spark.sql("SELECT * FROM sensor_readings_historical_labeled_dirty")
df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/FileStore/bk/sensor_readings_historical_labeled_dirty.csv")

# COMMAND ----------

# Now save a downloadable version of assignment_1_ingest

dbutils.fs.rm("dbfs:/FileStore/bk/assignment_1_ingest.csv", True)
df = spark.sql("SELECT * FROM assignment_1_ingest")
df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/FileStore/bk/assignment_1_ingest.csv")

# COMMAND ----------

# Now save a downloadable version of assignment_1_backfill

dbutils.fs.rm("dbfs:/FileStore/bk/assignment_1_backfill.csv", True)
df = spark.sql("SELECT * FROM assignment_1_backfill")
df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("dbfs:/FileStore/bk/assignment_1_backfill.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Stuff below belongs in assignment 1 notebook

# COMMAND ----------

# MAGIC %sh
# MAGIC ls

# COMMAND ----------

# MAGIC %sh
# MAGIC wget -v https://www.dropbox.com/s/qsv8atng7gsuzk6/assignment_1_ingest.csv

# COMMAND ----------

dbutils.fs.cp("file:/databricks/driver/assignment_1_ingest.csv", "dbfs:/FileStore/flight/assignment_1_ingest.csv")

# COMMAND ----------

dataPath = "dbfs:/FileStore/flight/assignment_1_ingest.csv"

df = spark.read\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("inferSchema", "true")\
  .csv(dataPath)

# COMMAND ----------

display(df)

# COMMAND ----------

