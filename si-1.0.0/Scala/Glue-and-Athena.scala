// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC # Amazon Glue and Athena
// MAGIC 
// MAGIC ## Learning Objectives
// MAGIC By the end of this lesson, you should be able to:
// MAGIC * Use Glue as an external metastore in the Databricks Runtime
// MAGIC * Generate manifests from a Delta Lake table using Databricks
// MAGIC * Configure Athena to read generated manifests
// MAGIC * Recommend workflows with known limitations
// MAGIC * Query Delta tables with Athena
// MAGIC 
// MAGIC You can configure Databricks Runtime to use the AWS Glue Data Catalog as its metastore. This can serve as a drop-in replacement for a Hive metastore, with some limitations. It also enables multiple Databricks workspaces to share the same metastore.
// MAGIC 
// MAGIC Athena supports reading from external tables when the list of data files to process is read from a _manifest file_, which is a text file containing the list of data files to read for querying a table. When an external table is defined in the Hive metastore using manifest files, Athena uses the list of files in the manifest rather than finding the files by directory listing.
// MAGIC 
// MAGIC ### Technical Requirements
// MAGIC AWS Glue support for Delta is in Public Preview as of DBR 5.5. Detailed instructions for setting up IAM roles to support this functionality are [here](https://docs.databricks.com/user-guide/advanced/aws-glue-metastore.html).
// MAGIC 
// MAGIC This demo assumes the following:
// MAGIC 1. An IAM role has been properly configured to allow Databricks to access Glue.
// MAGIC 1. The IAM role has been properly configured to access an S3 bucket.
// MAGIC 1. The IAM role has been mounted to a cluster running DBR 5.5+.
// MAGIC 1. The Spark Configuration `spark.databricks.hive.metastore.glueCatalog.enabled` has been set to `true` before cluster startup.
// MAGIC 
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> This configuration option _cannot_ be modified in a running cluster.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This demo uses the Glue Catalog associated with the _same AWS account_ as the Databricks deployment. [Additional configurations are required to use a Glue Catalog in a different AWS account](https://docs.databricks.com/user-guide/advanced/aws-glue-metastore.html#step-2-create-a-policy-for-the-target-glue-catalog).

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC We'll run a setup script here to make sure that we have access to our source data.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This script also deletes any tables/databases that might be leftover from previous runs of this demo.

// COMMAND ----------

// MAGIC %run "./Includes/Glue-Setup"

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Register Database
// MAGIC 
// MAGIC Because we have enabled the external Glue Catalog on this cluster, the assets we'll be manipulating are created in Glue (rather than the default internal Hive metastore used by Databricks). 
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Databases and tables registered earlier in this class aren't present in this catalog as it is a separate metastore than the one we've been using.

// COMMAND ----------

// MAGIC %sql
// MAGIC SHOW DATABASES;

// COMMAND ----------

// MAGIC %md
// MAGIC We'll create a database called `glue_db`. We'll register this to the same bucket used earlier in our S3 demo. (We have attached the same policy granting read/write/delete privileges to our mounted IAM role).

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS glue_db
// MAGIC LOCATION "s3a://awscore-encrypted/glue_db";
// MAGIC 
// MAGIC USE glue_db;

// COMMAND ----------

// MAGIC %md
// MAGIC ### A note on the data
// MAGIC For this demo, we'll be revisiting the data and logic used in the [Delta Architecture]($./13-Delta-Architecture) notebook.
// MAGIC 
// MAGIC We will approach our transformations and table registrations as batch jobs here. Note that we could replicate the entire workflow with Glue as our metastore, with [a few additional requirements to make sure that our manifest stays up-to-date for querying from Athena](https://docs.databricks.com/delta/presto-compatibility.html#step-3-updating-manifests-when-table-data-changes).

// COMMAND ----------

// MAGIC %md
// MAGIC ### Register Geo Lookup Table
// MAGIC 
// MAGIC For tables backed by parquet/JSON/CSV files, once we have registered a table in a Glue-enabled Databricks cluster, we'll be able to query this table from Databricks or other Glue-enabled AWS resources such as Athena.

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import col
// MAGIC 
// MAGIC (spark
// MAGIC   .read
// MAGIC   .format("parquet")
// MAGIC   .load("/mnt/training/countries/ISOCountryCodes/ISOCountryLookup.parquet/")
// MAGIC   .select(col("EnglishShortName").alias("country"), 
// MAGIC           col("alpha3Code").alias("countryCode3"))
// MAGIC   .write
// MAGIC   .saveAsTable("geo_lookup"))

// COMMAND ----------

// MAGIC %md
// MAGIC When using `saveAsTable` with Glue, we'll write our data as parquet to the S3 bucket specified when we registered our database.

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE DETAIL geo_lookup

// COMMAND ----------

// MAGIC %md
// MAGIC ### Migrate JSON Data to Delta Lake
// MAGIC Here we'll go ahead and complete the steps required to load and flatten our data in a single call, migrating from JSON to Delta. We'll only complete this for a single batch of our data.

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.types import StructField, StructType, LongType, StringType, DoubleType
// MAGIC from pyspark.sql.functions import from_unixtime
// MAGIC 
// MAGIC schema = StructType([
// MAGIC   StructField("Arrival_Time",LongType()),
// MAGIC   StructField("Creation_Time",LongType()),
// MAGIC   StructField("Device",StringType()),
// MAGIC   StructField("Index",LongType()),
// MAGIC   StructField("Model",StringType()),
// MAGIC   StructField("User",StringType()),
// MAGIC   StructField("geolocation",StructType([
// MAGIC     StructField("city",StringType()),
// MAGIC     StructField("country",StringType())
// MAGIC   ])),
// MAGIC   StructField("gt",StringType()),
// MAGIC   StructField("id",LongType()),
// MAGIC   StructField("x",DoubleType()),
// MAGIC   StructField("y",DoubleType()),
// MAGIC   StructField("z",DoubleType())
// MAGIC ])
// MAGIC 
// MAGIC (spark
// MAGIC   .read
// MAGIC   .format("json")
// MAGIC   .schema(schema)
// MAGIC   .load("/mnt/training/definitive-guide/data/activity-json/batch-0")
// MAGIC    .select(from_unixtime(col("Arrival_Time")/1000).alias("Arrival_Time").cast("timestamp"),
// MAGIC           (col("Creation_Time")/1E9).alias("Creation_Time").cast("timestamp"),
// MAGIC           col("Device").alias("Device"),
// MAGIC           col("Index").alias("Index"),
// MAGIC           col("Model").alias("Model"),
// MAGIC           col("User").alias("User"),
// MAGIC           col("gt").alias("gt"),
// MAGIC           col("x").alias("x"),
// MAGIC           col("y").alias("y"),
// MAGIC           col("z").alias("z"),
// MAGIC           col("geolocation.country").alias("country"),
// MAGIC           col("geolocation.city").alias("city"))
// MAGIC   .write
// MAGIC   .format("delta")
// MAGIC   .mode("overwrite")
// MAGIC   .save("s3a://awscore-encrypted/activity-data"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Register Delta Table for Use by Databricks
// MAGIC 
// MAGIC When we register a Delta Table against these files, Databricks will maintain the current valid version of the files using the Glue Catalog.

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE IF NOT EXISTS activity_data_db
// MAGIC USING delta
// MAGIC LOCATION "s3a://awscore-encrypted/activity-data";

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Generate manifests from a Delta Lake table using Databricks Runtime
// MAGIC Athena requires manifest files to be generated in order to query a Delta table from the Glue Catalog.
// MAGIC 
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Do not use AWS Glue Crawler on the Delta Table file location to define the table in AWS Glue. Delta Lake maintains files corresponding to multiple versions of the table, and querying all the files crawled by Glue will generate incorrect results.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC GENERATE symlink_format_manifest FOR TABLE activity_data_db;

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Configure Athena to read generated manifests
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> You must supply the schema in DDL.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE EXTERNAL TABLE activity_data_athena (
// MAGIC   Arrival_Time timestamp,
// MAGIC   Creation_Time timestamp,
// MAGIC   Device string,
// MAGIC   `Index` long,
// MAGIC   Model string,
// MAGIC   User string,
// MAGIC   gt string,
// MAGIC   x double,
// MAGIC   y double,
// MAGIC   z double,
// MAGIC   country string,
// MAGIC   city string)
// MAGIC ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
// MAGIC STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
// MAGIC OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
// MAGIC LOCATION 's3a://awscore-encrypted/activity-data/_symlink_format_manifest/'  -- location of the generated manifest

// COMMAND ----------

// MAGIC %md
// MAGIC As we can see, we now have three tables registered in our database in glue.

// COMMAND ----------

// MAGIC %sql
// MAGIC SHOW TABLES;

// COMMAND ----------

// MAGIC %md
// MAGIC If we log into the AWS console, we can see this tables immediately loaded into Glue and accessible from Athena.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Did we just duplicate our data?
// MAGIC 
// MAGIC To be clear: **No**. 
// MAGIC 
// MAGIC Athena cannot currently utilize the metadata contained in the Delta logs to query the most recent state of the data. The manifest is generated from these log files, and ensures that Athena queries the valid state.
// MAGIC 
// MAGIC Databricks cannot query the data based on the table registered with the manifest.
// MAGIC 
// MAGIC Our data is not duplicated, we have just defined separate metadata to reflect the current valid state of our data. All queries and operations from either service should be directed toward their respective tables.
// MAGIC 
// MAGIC Refer to [the docs](https://docs.databricks.com/delta/presto-compatibility.html#workflow-with-databricks-and-prestoor-using-the-same-hive-metastore) for a more detailed discussion of this workflow.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Resources
// MAGIC - [Using AWS Glue Data Catalog as the Metastore for Databricks Runtime](https://docs.databricks.com/user-guide/advanced/aws-glue-metastore.html)
// MAGIC - [Presto and Athena Compatibility Support for Delta Lake](https://docs.databricks.com/delta/presto-compatibility.html)
// MAGIC - [Transform Your AWS Data Lake using Databricks Delta and the AWS Glue Data Catalog Service](https://databricks.com/blog/2019/09/03/transform-your-aws-data-lake-using-databricks-delta-and-aws-glue-data-catalog-service.html)
// MAGIC - [AWS Glue Documentation](https://docs.aws.amazon.com/glue/index.html)
// MAGIC - [Amazon Athena Documentation](https://docs.aws.amazon.com/athena/)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>