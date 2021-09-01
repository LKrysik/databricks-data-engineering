// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC # Amazon Redshift & Databricks
// MAGIC 
// MAGIC ## Learning Objectives
// MAGIC By the end of this lesson, you should be able to:
// MAGIC * Describe the connection architecture of Redshift and Spark
// MAGIC * Configure a connection between Databricks and Redshift using JDBC and S3
// MAGIC * Read data from Redshift
// MAGIC * Write data to Redshift
// MAGIC 
// MAGIC This notebook contains examples showing how to use Databricks' [spark-redshift](https://github.com/databricks/spark-redshift) library for bulk reading/writing of large datasets. This library comes automatically included in cluster image versions >= Spark 2.1.0-db2.
// MAGIC 
// MAGIC `spark-redshift` is a library to load data into Spark SQL DataFrames from Amazon Redshift, and write data back to Redshift tables. Amazon S3 is used to efficiently transfer data in and out of Redshift, and JDBC is used to automatically trigger the appropriate COPY and UNLOAD commands on Redshift.
// MAGIC 
// MAGIC For optimal functionality, install the AWS-maintained redshift driver that's the right version for SQL client tool; [download jar files here](https://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html#download-jdbc-driver).
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Installing this driver can cause conflicts with the default postgres driver in the Databricks runtime. The postgres driver can also be used to connect to Redshift, but may not provide optimal performance.
// MAGIC 
// MAGIC In general, the connector between Redshift and Spark is more suited to ETL than interactive queries, since large amounts of data could be extracted to S3 for each query execution. If you plan to perform many queries against the same Redshift tables then we recommend saving the extracted data in a format such as Parquet.

// COMMAND ----------

// MAGIC %run ./Includes/Classroom-Setup

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ## Configuration
// MAGIC 
// MAGIC There are four main configuration steps which are prerequisites to using `spark-redshift`:
// MAGIC 
// MAGIC - Create a S3 bucket to hold temporary data.
// MAGIC - Configure AWS credentials to access that S3 bucket.
// MAGIC - Configure Redshift access credentials.
// MAGIC - Install the latest Redshift JDBC driver jar. Download the latest driver from [here](http://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html#download-jdbc-driver) and upload the redshift-jdbc driver as a library to databricks and attach the library to the cluster
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> These configurations have already been completed for this classroom demo. Ample documentation is provided at the end of this lesson for configuring these yourself.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### Configuring a S3 bucket to hold temporary files
// MAGIC 
// MAGIC `spark-redshift` reads and writes data to S3 when transferring data to/from Redshift, so you'll need to specify a path in S3 where the library should write these temporary files. `spark-redshift` cannot automatically clean up the temporary files that it creates in S3. As a result, we recommend that you use a dedicated temporary S3 bucket with an [object lifecycle configuration](http://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html) to ensure that temporary files are automatically deleted after a specified expiration period.
// MAGIC 
// MAGIC In this example, we'll use a bucket named `partner-enablement-redshift` to hold our temporary directory.
// MAGIC 
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> You must choose an S3 bucket for the temp folder that is in the same region as your Databricks Cluster.
// MAGIC 
// MAGIC ### Authenticating to S3 and Redshift
// MAGIC 
// MAGIC The use of this library involves several connections which must be authenticated / secured, 
// MAGIC which are illustrated in the following diagram:
// MAGIC 
// MAGIC <img src= "https://s3.ca-central-1.amazonaws.com/westca-collateral/spark_redshift.png" width="700">
// MAGIC 
// MAGIC 
// MAGIC This library reads and writes data to S3 when transferring data to/from Redshift. As a result, it requires AWS credentials with read and write access to a S3 bucket (specified using the tempdir configuration parameter).
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> An IAM role with correct permissions has been mounted to the instructor's cluster for this demo.

// COMMAND ----------

val jdbcUsername = dbutils.secrets.get(scope = "demo", key = "dbuser")
val jdbcPassword = dbutils.secrets.get(scope = "demo", key = "dbpasswd")

val tempDir = "s3a://partner-enablement-redshift/temp"
val jdbcUrl = "jdbc:redshift://redshift-awscore.cyyewh0t7hhq.us-west-2.redshift.amazonaws.com:5439/db"

// COMMAND ----------

// MAGIC %md
// MAGIC While our S3 bucket is set to expire and delete old files, we'll explicitly remove these now, and make sure our target directory is present.

// COMMAND ----------

dbutils.fs.rm(tempDir, true)

dbutils.fs.mkdirs(tempDir)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Reading data from Redshift
// MAGIC 
// MAGIC `spark-redshift` executes a Redshift [UNLOAD](http://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html) command (using JDBC) which copies the Redshift table in parallel to a temporary S3 bucket provided by the user. Next it reads these S3 files in parallel using the Hadoop InputFormat API and maps it to an RDD instance. Finally, it applies the schema of the table (or query), retrieved using JDBC metadata retrieval capabilities, to the RDD generated in the prior step to create a DataFrame instance.
// MAGIC 
// MAGIC ![](https://databricks.com/wp-content/uploads/2015/10/image01.gif "Redshift Data Ingestion")

// COMMAND ----------

// MAGIC %md
// MAGIC While there are many additional options available, at minimum you must provide the following options to load data:

// COMMAND ----------

val redshift_table = spark.read
  .format("com.databricks.spark.redshift")
  .option("url", jdbcUrl)                
  .option("dbtable", "people")
  .option("tempdir", tempDir)           
  .option("user", jdbcUsername) 
  .option("password", jdbcPassword)
  .option("forward_spark_s3_credentials", "true")
  .load()

redshift_table.printSchema

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> At this point, no data has been moved; we've only tested the connection and accessed the table metadata. Because no data has been moved, our tempDir will still be empty.

// COMMAND ----------

dbutils.fs.ls(tempDir).length

// COMMAND ----------

// MAGIC %md
// MAGIC As soon as we call an action, we will trigger data transfer through our temp directory.

// COMMAND ----------

redshift_table.count()

// COMMAND ----------

// MAGIC %md
// MAGIC We can confirm that data has been loaded to S3.

// COMMAND ----------

dbutils.fs.ls(tempDir)

// COMMAND ----------

// MAGIC %md
// MAGIC Let's take a quick look at our data:

// COMMAND ----------

display(redshift_table.limit(10))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Obscure SSN
// MAGIC 
// MAGIC We'll do a simple transformation to obscure the Social Security Number before writing this back out as a table.

// COMMAND ----------

import org.apache.spark.sql.functions.{regexp_replace, col, count}

// COMMAND ----------

val obscuredDF = redshift_table.withColumn("ssn", regexp_replace(col("ssn"), "([0-9]{3}-[0-9]{2})", "***-**")).alias("ssn")

display(obscuredDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Writing Data to Redshift
// MAGIC `spark-redshift` will first create the table in Redshift using JDBC. It then copies the partitioned RDD encapsulated by the source DataFrame instance to the temporary S3 folder. Finally, it executes the Redshift [COPY](http://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html) command that performs a high performance distributed copy of S3 folder contents to the newly created Redshift table.
// MAGIC 
// MAGIC ![](https://databricks.com/wp-content/uploads/2015/10/image00.gif "Redshift Data Egression")

// COMMAND ----------

// MAGIC %md
// MAGIC Let's save our obscured data into a Redshift table, which we'll name `people_no_ssn`.

// COMMAND ----------

obscuredDF
  .write 
  .format("com.databricks.spark.redshift")
  .option("url", jdbcUrl)                
  .option("tempdir", tempDir)           
  .option("user", jdbcUsername) 
  .option("password", jdbcPassword)
  .option("forward_spark_s3_credentials", "true")
  .option("dbtable", "people_no_ssn")    
  .mode("overwrite")
  .save()

// COMMAND ----------

// MAGIC %md
// MAGIC We can register tables or views directly against Redshift as well:

// COMMAND ----------

spark.read
  .format("com.databricks.spark.redshift")
  .option("url", jdbcUrl)                
  .option("dbtable", "people_no_ssn")
  .option("tempdir", tempDir)           
  .option("user", jdbcUsername) 
  .option("password", jdbcPassword)
  .option("forward_spark_s3_credentials", "true")
  .load()
  .createOrReplaceTempView("people_no_ssn")

// COMMAND ----------

// MAGIC %md
// MAGIC Let's look at a few rows:

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM people_no_ssn LIMIT 10

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC `spark-redshift` automatically creates a Redshift table with the appropriate schema determined from the table/DataFrame being written.
// MAGIC 
// MAGIC The default behavior is to create a new table and to throw an error message if a table with the same name already exists. 
// MAGIC 
// MAGIC Here we're using `overwrite` mode to drop a table if it already exists. If we change to `append`, we'll be able to add new data.

// COMMAND ----------

obscuredDF
  .write 
  .format("com.databricks.spark.redshift")
  .option("url", jdbcUrl)                
  .option("tempdir", tempDir)           
  .option("user", jdbcUsername) 
  .option("password", jdbcPassword)
  .option("forward_spark_s3_credentials", "true")
  .option("dbtable", "people_no_ssn")
  .mode("append")
  .save()

// COMMAND ----------

// MAGIC %md
// MAGIC We can query the count of this table now to see that we've doubled from our original table.

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(*) FROM people_no_ssn

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC `spark-redshift` is usable from SQL, Scala, Python, R, and Java.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The SQL API only supports the creation of new tables and not overwriting or appending; this corresponds to the default save mode of the other language APIs.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Additional Resources
// MAGIC - [Databricks Redshift Docs](https://docs.databricks.com/spark/latest/data-sources/aws/amazon-redshift.html)
// MAGIC - [Getting Started with Amazon Redshift - AWS](https://docs.aws.amazon.com/redshift/latest/gsg/getting-started.html)
// MAGIC - [Permissions to Access Other AWS Resources](https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-access-permissions.html)
// MAGIC - [Configure a JDBC Connection - AWS](https://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html#download-jdbc-driver)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>