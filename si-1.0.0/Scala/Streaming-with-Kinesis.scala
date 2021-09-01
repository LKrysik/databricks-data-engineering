// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC 
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
// MAGIC </div>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <img src="https://files.training.databricks.com/images/Apache-Spark-Logo_TM_200px.png" style="float: left: margin: 20px"/>
// MAGIC 
// MAGIC # Structured Streaming with AWS Kinesis Streams
// MAGIC 
// MAGIC ## Learning Objectives
// MAGIC By the end of this lesson, you should be able to:
// MAGIC * Establish a connection with Kinesis in Spark
// MAGIC * Subscribe to and configure a Kinesis stream
// MAGIC * Understand the Schema that Kinesis returns
// MAGIC * Parse JSON records from Kinesis
// MAGIC 
// MAGIC We have another server that reads Wikipedia edits in real time, with a multitude of different languages. 
// MAGIC 
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> AWS charges for Kinesis provisioning AND throughput. So, even if your Kinesis stream is inactive, you pay for provisioning 
// MAGIC (<a href="https://aws.amazon.com/kinesis/data-streams/pricing/" target="_blank">Kinesis Pricing</a>)

// COMMAND ----------

// MAGIC %run "./Includes/Classroom-Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Kinesis Streams </h2>
// MAGIC 
// MAGIC 
// MAGIC Kinesis can be used in a variety of applications such as
// MAGIC * Analytics pipelines, such as clickstreams
// MAGIC * Anomaly detection (fraud/outliers)
// MAGIC * Application logging
// MAGIC * Archiving data
// MAGIC * Data collection from IoT devices
// MAGIC * Device telemetry streaming
// MAGIC * Transaction processing
// MAGIC * User telemetry processing
// MAGIC * <b>Live dashboarding</b>
// MAGIC 
// MAGIC In this notebook, we will show you how to use Kinesis to produce LIVE Dashboards.

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Run Wikipedia Stream Producer </h2>
// MAGIC 
// MAGIC The following code runs a producer that fetches continuous streaming data from the Wikipedia
// MAGIC and stages it in a Kinesis Stream.
// MAGIC 
// MAGIC ### Note on Runs
// MAGIC 
// MAGIC Here we demo using `dbutils.notebook.run` to run another notebook. Note that we're passing a variable to `dbutils.widgets` which we'll be able to unpack and use during our run.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> You can re-run this notebook manually at any point to fetch more records.

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.notebook.run("./Includes/Kinesis-Wiki-Stream-Producer", 0, {"batches": "4"})

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Kinesis Configuration</h2>
// MAGIC 
// MAGIC We will also need to define these variables and pass them in as options i.e. `option("key", "value")`
// MAGIC * `kinesisRegion`: closest AWS region i.e. `us-west-2` (Oregon)
// MAGIC * `maxRecordsPerFetch`: maximum records per fetch
// MAGIC * `kinesisStreamName`: the name of your stream exactly as on the AWS Kinesis portal (image below)
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/eLearning/Structured-Streaming/kinesis-streams-dashboard.png"/>

// COMMAND ----------

val kinesisStreamName = "Wikipedia-kinesis-stream"       
val kinesisRegion = "us-west-2"
val maxRecordsPerFetch = 10           // Throttle to 10 records per batch (slow)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> The Kinesis Schema</h2>
// MAGIC 
// MAGIC Reading from Kinesis returns a `DataFrame` with the following fields:
// MAGIC 
// MAGIC | Field             | Type   | Description |
// MAGIC |------------------ | ------ |------------ |
// MAGIC | **partitionKey**  | string | A unique identifier to a partition which is used to group data by shard within a stream |
// MAGIC | **data**          | binary | Our JSON payload. We'll need to cast it to STRING |
// MAGIC | **stream**     | string | A sequence of data records |
// MAGIC | **shardID**        | string | An identifier to a uniquely identified sequence of data records in a stream|
// MAGIC | **sequenceNumber**     | long   | A unique identifier for a packet of data (alternative to a timestamp) |
// MAGIC | **approximateArrivalTimestamp** 	| timestamp | Time when data arrives |
// MAGIC 
// MAGIC 
// MAGIC Kinesis terminology is explained <a href="https://docs.aws.amazon.com/streams/latest/dev/key-concepts.html" target="_blank">HERE</a>.
// MAGIC 
// MAGIC In the example below, the only column we want to keep is `data`.
// MAGIC * The code below casts this field to string type and renames it to `body`.
// MAGIC * We will then extract fields from the JSON in subsequent transformations.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> The default of `spark.sql.shuffle.partitions` is 200.
// MAGIC This setting is used in operations like `groupBy`.
// MAGIC In this case, we should be setting this value to match the current number of cores.

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

val editsDF = spark.readStream                      // Get the DataStreamReader
  .format("kinesis")                                // Specify the source format as "kinesis"
  .option("streamName", kinesisStreamName)          // Stream name exactly as in AWS Kinesis portal
  .option("region", kinesisRegion)                  // AWS region like us-west-2 (Oregon)
  .option("maxRecordsPerFetch", maxRecordsPerFetch) // Maximum records per fetch
  .option("initialPosition", "EARLIEST")            // Specify starting position of stream that hasn't expired yet (up to 7 days)
  .load()                                           // Load the DataFrame
  .selectExpr("CAST(data as STRING) as body")       // Cast the "data" column to STRING, rename to "body"

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC Let's display some data.
// MAGIC 
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Make sure the Wikipedia stream server is running at this point.

// COMMAND ----------

display(editsDF,  streamName = "edits")

// COMMAND ----------

// MAGIC %md
// MAGIC Make sure to stop the stream before continuing.

// COMMAND ----------

for (s <- spark.streams.active) { // Iterate over all active streams
  if (s.name == "edits") {        // Look for our specific stream
    println("Stopping "+s.name)   // A little extra feedback
    s.stop                        // Stop the stream
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Use Kinesis to Display the Raw Data</h2>
// MAGIC 
// MAGIC The Kinesis server acts as a sort of asynchronous buffer and displays raw data.
// MAGIC 
// MAGIC Since raw data coming in from a stream is transient, we'd like to save it to a more permanent data structure.
// MAGIC 
// MAGIC The first step is to define the schema for the JSON payload.

// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType}

lazy val schema = StructType(List(
  StructField("bot", BooleanType, true),
  StructField("comment", StringType, true),
  StructField("id", IntegerType, true),                  // ID of the recentchange event 
  StructField("length",  StructType(List( 
    StructField("new", IntegerType, true),               // Length of new change
    StructField("old", IntegerType, true)                // Length of old change
  )), true), 
  StructField("meta", StructType(List(  
	StructField("domain", StringType, true),
	StructField("dt", StringType, true),
	StructField("id", StringType, true),
	StructField("request_id", StringType, true),
	StructField("schema_uri", StringType, true),
	StructField("topic", StringType, true),
	StructField("uri", StringType, true),
	StructField("partition", StringType, true),
	StructField("offset", StringType, true)
  )), true),
  StructField("minor", BooleanType, true),                 // Is it a minor revision?
  StructField("namespace", IntegerType, true),             // ID of relevant namespace of affected page
  StructField("parsedcomment", StringType, true),          // The comment parsed into simple HTML
  StructField("revision", StructType(List(                 
    StructField("new", IntegerType, true),                 // New revision ID
    StructField("old", IntegerType, true)                  // Old revision ID
  )), true),
  StructField("server_name", StringType, true),
  StructField("server_script_path", StringType, true),
  StructField("server_url", StringType, true),
  StructField("timestamp", IntegerType, true),             // Unix timestamp 
  StructField("title", StringType, true),                  // Full page name
  StructField("type", StringType, true),                   // Type of recentchange event (rc_type). One of "edit", "new", "log", "categorize", or "external".
  StructField("geolocation", StructType(List(              // Geo location info structure
    StructField("PostalCode", StringType, true),
    StructField("StateProvince", StringType, true),
    StructField("city", StringType, true), 
    StructField("country", StringType, true),
    StructField("countrycode3", StringType, true)          // Really, we only need the three-letter country code in this exercise
   )), true),
  StructField("user", StringType, true),                   // User ID of person who wrote article
  StructField("wiki", StringType, true)                    // wfWikiID
))

// COMMAND ----------

// MAGIC %md
// MAGIC Next we can use the function `from_json` to parse out the full message with the schema specified above.

// COMMAND ----------

import org.apache.spark.sql.functions.from_json

val jsonEdits = editsDF.select(
  from_json($"body", schema).as("json"))   // Parse the column "value" and name it "json"

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC When parsing a value from JSON, we end up with a single column containing a complex object.
// MAGIC 
// MAGIC We can clearly see this by simply printing the schema.

// COMMAND ----------

jsonEdits.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC The fields of a complex object can be referenced with a "dot" notation as in:
// MAGIC 
// MAGIC `$"json.wiki"`
// MAGIC 
// MAGIC A large number of these fields/columns can become unwieldy.
// MAGIC 
// MAGIC For that reason, it is common to extract the sub-fields and represent them as first-level columns as seen below:

// COMMAND ----------

val wikiDF = jsonEdits
  .select($"json.wiki".as("wikipedia"),                         // Promoting from sub-field to column
          $"json.namespace".as("namespace"),                    //     "       "      "      "    "
          $"json.title".as("page"),                             //     "       "      "      "    "
          $"json.server_name".as("pageURL"),                    //     "       "      "      "    "
          $"json.user".as("user"),                              //     "       "      "      "    "
          $"json.geolocation.countrycode3".as("countryCode3"),  //     "       "      "      "    "
          $"json.timestamp".cast("timestamp"))                  // Promoting and converting to a timestamp
  .filter($"wikipedia".isNotNull)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Mapping Anonymous Editors' Locations</h2>
// MAGIC 
// MAGIC When you run the query, the default is a [live] html table.
// MAGIC 
// MAGIC The geocoded information allows us to associate an anonymous edit with a country.
// MAGIC 
// MAGIC We can then use that geocoded information to plot edits on a [live] world map.
// MAGIC 
// MAGIC In order to create a slick world map visualization of the data, you'll need to click on the item below.
// MAGIC 
// MAGIC Under <b>Plot Options</b>, use the following:
// MAGIC * <b>Keys:</b> `countryCode3`
// MAGIC * <b>Values:</b> `count`
// MAGIC 
// MAGIC In <b>Display type</b>, use <b>World map</b> and click <b>Apply</b>.
// MAGIC 
// MAGIC <img src="https://files.training.databricks.com/images/eLearning/Structured-Streaming/plot-options-map-04.png"/>
// MAGIC 
// MAGIC By invoking a `display` action on a DataFrame created from a `readStream` transformation, we can generate a LIVE visualization!
// MAGIC 
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Make sure the Wikipedia stream server is running at this point.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Keep an eye on the plot for a minute or two and watch the colors change.

// COMMAND ----------

val mappedDF = wikiDF
  .groupBy("countryCode3")   // Aggregate by country (code)
  .count()                   // Produce a count of each aggregate

display(mappedDF, streamName = "mapped_edits")

// COMMAND ----------

// MAGIC %md
// MAGIC Stop the streams.

// COMMAND ----------

for (s <- spark.streams.active)  // Iterate over all active streams
  s.stop()          

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Additional Topics &amp; Resources</h2>
// MAGIC 
// MAGIC * <a href="https://www.tutorialspoint.com/amazon_web_services/amazon_web_services_kinesis.htm" target="_blank">Introduction to Kinesis</a>
// MAGIC * <a href="https://docs.aws.amazon.com/streams/latest/dev/introduction.html" target="_blank">AWS Kinesis Streams Developer Guide</a>
// MAGIC * <a href="https://docs.databricks.com/spark/latest/structured-streaming/kinesis.html" target="_blank">Databricks Integration with Kinesis Guide</a>
// MAGIC * <a href="https://pypi.org/project/sseclient/" target="_blank">Python SSE Client</a>
// MAGIC * <a href="https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/kinesis.html" target="_blank">Python Boto3 library for Kinesis</a>

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
// MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
// MAGIC <br/>
// MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>