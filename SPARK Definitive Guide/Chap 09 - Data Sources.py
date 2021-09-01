# Databricks notebook source
# MAGIC %md
# MAGIC Spark has **six core** data sources and hundreds of external data sources
# MAGIC 
# MAGIC * CSV
# MAGIC * JSON
# MAGIC * Parquet
# MAGIC * ORC
# MAGIC * JDBC/ODBC connections
# MAGIC * Plain-text files
# MAGIC 
# MAGIC The ability to read and write from all different kinds of data sources and for the community to create its own contributions is arguably one of Spark’s greatest strengths.

# COMMAND ----------

spark.read.

# COMMAND ----------

# MAGIC %md
# MAGIC ###### The Structure of the Data Sources API:
# MAGIC 
# MAGIC **Read API structure:**
# MAGIC The core structure for reading data is as follows
# MAGIC 
# MAGIC DataFrameReader.format(...).option("key","value").schema(...).load()
# MAGIC 
# MAGIC **format** is optional because by default Spark will use the Parquet format.
# MAGIC 
# MAGIC **option** allows you to set key-value configurations to parameterize how you will read data.
# MAGIC 
# MAGIC **schema** is optional if the data source provides a schema/ if you intend to use schema inference.

# COMMAND ----------

# MAGIC %md
# MAGIC **Basics of Reading Data:**
# MAGIC 
# MAGIC The foundation for reading data in Spark is the **DataFrameReader**. We access this through the SparkSession via the read attribute:
# MAGIC  
# MAGIC  spark.read
# MAGIC  
# MAGIC After we have a DataFrame reader, we specify several values:
# MAGIC 
# MAGIC * The format
# MAGIC * The schema
# MAGIC * The read mode
# MAGIC * A series of options
# MAGIC 
# MAGIC Example:
# MAGIC               
# MAGIC               spark.read.format("csv")\
# MAGIC               
# MAGIC               .option("mode", "FAILFAST")\
# MAGIC               
# MAGIC               .option("inferSchema", "true")\
# MAGIC               
# MAGIC               .option("path", "path/to/file(s)")\
# MAGIC               
# MAGIC               .schema(someSchema)\
# MAGIC               
# MAGIC               .load()

# COMMAND ----------

# MAGIC %md
# MAGIC **Read Modes:**
# MAGIC 
# MAGIC Reading data from an external source naturally entails encountering malformed data, especially when working with only semi-structured data sources.
# MAGIC 
# MAGIC * Permissive - Sets all fields to null when it encounters a corrupted record and places all corrupted records in a string column called _corrupt_record
# MAGIC * dropMalformed - Drops the row that contains malformed records
# MAGIC * failFast - Fails immediately upon encountering malformed records

# COMMAND ----------

# MAGIC %md
# MAGIC **Write API Structure:**
# MAGIC 
# MAGIC DataFrameWriter.format(...).option(...).partitionBy(...).bucketBy(...).sortBy(...).save()
# MAGIC 
# MAGIC **format** is optional because by default,Spark will use the parquet format.
# MAGIC 
# MAGIC **option**, again, allows us to configure how to write out our given data. 
# MAGIC 
# MAGIC **PartitionBy, bucketBy, and sortBy** work only for file-based data sources

# COMMAND ----------

# MAGIC %md
# MAGIC **Basics of Writing Data:**
# MAGIC 
# MAGIC DataFrameWriter on a per-DataFrame basis via the write attribute
# MAGIC 
# MAGIC         dataframe.write.format("csv")
# MAGIC         .option("mode", "OVERWRITE")
# MAGIC         .option("dateFormat", "yyyy-MM-dd")
# MAGIC         .option("path", "path/to/file(s)")
# MAGIC         .save()
# MAGIC 
# MAGIC Spark’s save modes
# MAGIC 
# MAGIC * append - Appends the output files to the list of files that already exist at that location
# MAGIC * overwrite - Will completely overwrite any data that already exists there
# MAGIC * errorIfExists - Throws an error and fails the write if data or files already exist at the specified location
# MAGIC * ignore - If data or files exist at the location, do nothing with the current DataFrame
# MAGIC 
# MAGIC default is errorIfExists

# COMMAND ----------

# MAGIC %md
# MAGIC ###### CSV files:
# MAGIC 
# MAGIC CSV files, while seeming well structured, are actually one of the trickiest file formats you will encounter because not many assumptions can be made in production scenarios about what they contain or how they are structured. For this reason, the CSV reader has a large number of options
# MAGIC 
# MAGIC Ex: 
# MAGIC 
# MAGIC * commas inside of columns when the file is also comma-delimited or
# MAGIC * null values labeled in an unconventional way.

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, LongType, StringType

myManualSchema = StructType([
  StructField("DEST_COUNTRY_NAME", StringType(), True),
  StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
  StructField("count", LongType(), False)])

spark.read.format("csv")\
.option("header","true")\
.option("mode","FAILFAST")\
.schema(myManualSchema)\
.load("/FileStore/tables/FileStore/tables/2015_summary-ebaee.csv")\
.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Things get tricky when we don’t expect our data to be in a certain format, but it comes in that way.
# MAGIC 
# MAGIC Eg: let’s take our current schema and change all column types to **LongType**. This will fail when SPARK job reads the Data

# COMMAND ----------

myManualSchema = StructType([
  StructField("DEST_COUNTRY_NAME", LongType(), True),
  StructField("ORIGIN_COUNTRY_NAME", LongType(), True),
  StructField("count", LongType(), False)])

spark.read.format("csv")\
.option("header", "true")\
.option("mode", "FAILFAST")\
.schema(myManualSchema)\
.load("/FileStore/tables/FileStore/tables/2015_summary-ebaee.csv")\
.take(5)

# COMMAND ----------

# MAGIC %md
# MAGIC **Writing CSV Files:**
# MAGIC * Just as with reading data, there are a variety of **options** for writing data when we write CSV files. 
# MAGIC * This is a subset of the reading options because many do not apply when writing data (like maxColumns and inferSchema).
# MAGIC 
# MAGIC For instance, we can take our CSV file and write it out as a TSV file quite easily

# COMMAND ----------

csvFile = spark.read.format("csv")\
          .option("header",True)\
          .option("inferSchema",True)\
          .option("mode","FAILFAST")\
          .load("/FileStore/tables/FileStore/tables/2015_summary-ebaee.csv")

# COMMAND ----------

csvFile.write.format("csv")\
             .mode("overwrite")\
             .option("sep","\t")\
             .save("/tmp/my-tsv-file.tsv")

# COMMAND ----------

# MAGIC %md
# MAGIC When you list the destination directory, you can see that my-tsv-file is actually a folder with numerous files within it.
# MAGIC 
# MAGIC This actually reflects the number of partitions in our DataFrame at the time we write it out. If we were to repartition our data before then, we would end up with a different number of files.

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /tmp/my-tsv-file.tsv

# COMMAND ----------

# MAGIC %md
# MAGIC **JSON Files:**
# MAGIC * In Spark, when we refer to JSON files, we refer to **line-delimited** JSON files. 
# MAGIC * This contrasts with files that have a large JSON object or array per file.
# MAGIC * The line-delimited versus multiline trade-off is controlled by a single **option: multiLine.**
# MAGIC * When you set this option to true, you can read an entire file as one json object and Spark will go through the work of parsing that into a DataFrame.
# MAGIC * Line-delimited JSON is actually a much more stable format because
# MAGIC   * it allows you to append to a file with a new record (rather than having to read in an entire file and then write it out)
# MAGIC   * JSON objects have structure, and JavaScript (on which JSON is based) has at least basic types. This makes it easier to work with because Spark can make more assumptions on our behalf about the data.
# MAGIC   
# MAGIC there are significantly less options than importing CSV files

# COMMAND ----------

jsonFile = spark.read.format("json")\
          .option("mode","FAILFAST")\
          .option("inferSchema","true")\
          .load("/FileStore/tables/2010_summary-506d8.json")
jsonFile.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Writing JSON file:

# COMMAND ----------

jsonFile.write.format("json").mode("overWrite").save("/tmp/my-json-file.json")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /tmp/my-json-file.json/

# COMMAND ----------

# MAGIC %md
# MAGIC **Parquet Files:**
# MAGIC * This is an open source column-oriented data store that provides variety of storage optimizations, especially for analytical workloads.
# MAGIC * It provides columnar compression, which saves storage space and allows for reading individual columns instead of entire files.
# MAGIC * It is the default format for Apache SPARK.
# MAGIC * Reading data will be so efficient  when we store data in Parquet format compared to CSV and JSON format.
# MAGIC * If the column is complex type(array, struct or Map), we can still be able to read and write using Parquet format. 

# COMMAND ----------

spark.read.format("parquet")\
     .load("/FileStore/tables/2010_summary-506d8.parquet").show(5)

# COMMAND ----------

spark.read.orc

# COMMAND ----------

# MAGIC %md
# MAGIC **Parquet Options:**
# MAGIC 
# MAGIC Parquet has only 2 options, because it has well defined specification that aligns closely with the concepts of SPARK.
# MAGIC 
# MAGIC * Write - (
# MAGIC            KEY - compression or codec\
# MAGIC            POTENTIAL VALUES - None, uncompressed, bzip2, deflate, gzip, lz4, or snappy\
# MAGIC            DEFAULT - None\
# MAGIC            DESCRIPTION - Declares what compression codec Spark should use to read and write the file)
# MAGIC            
# MAGIC * Read - (
# MAGIC           KEY - mergeSchema\
# MAGIC           POTENTIAL VALUES - true,false\
# MAGIC           DEFAULT - Value of the configuration spark.sql.parquet.mergeSchema\
# MAGIC           DESCRIPTION - You can incrementally add columns to newly written Parquet files in the same table/folder. use this option to\
# MAGIC           enable/disable this feature)

# COMMAND ----------

# MAGIC %md
# MAGIC **Writing Parquet Files:**

# COMMAND ----------

jsonFile.write.format("parquet")\
        .mode("overWrite")\
        .save("/tmp/my-parquet-file.parquet")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /tmp/my-parquet-file.parquet

# COMMAND ----------

# MAGIC %md
# MAGIC **ORC(Optimized Row Columnar) Files:**
# MAGIC 
# MAGIC * ORC is a self-describing, type aware columnar file format designed for Hadoop workloads.
# MAGIC * It is optimized for large streaming reads, but with integrated support for finding required rows quickly.
# MAGIC * ORC doesnt have any options to read data, because SPARK understands the file format quite well.
# MAGIC * Mostly Parquet and ORC are similar but the fundamental difference is Parquet is further optimized for SPARK and ORC is optimized for HIVE.

# COMMAND ----------

spark.read.format("orc")\
  .load("/FileStore/tables/2010_summary-506d8.orc").show(3)

# COMMAND ----------

csvFile.write.format("orc").mode("overWrite").save("/tmp/my-json-file.orc")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /tmp/my-json-file.orc

# COMMAND ----------

# MAGIC %md
# MAGIC **SQL DataBases:**

# COMMAND ----------

driver = "org.sqlite.JDBC"
path = "/FileStore/tables/my_sqlite-e9c7d.db"
url = "jdbc:sqlite:" + path
tablename = "flight_info"

dbDataFrame = spark.read.format("jdbc").option("url", url).option("dbtable", tablename).option("driver", driver)

# COMMAND ----------

dbDataFrame.load()

# COMMAND ----------

# MAGIC %md
# MAGIC **Query Pushdown:**
# MAGIC 
# MAGIC * If we specify a filter on DataFrame, Spark will push that filter down into the database. When we run below code it specifies the push down filter
# MAGIC 
# MAGIC dbDataFrame.filter("DEST_COUNTRY_NAME in ('Anguilla', 'Sweden')").explain()
# MAGIC == Physical Plan ==
# MAGIC *Scan JDBCRel... PushedFilters: [*In(DEST_COUNTRY_NAME, [Anguilla,Sweden])],
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC * specify a SQL query instead of specifying a table name.
# MAGIC 
# MAGIC pushdownQuery = """(SELECT DISTINCT(DEST_COUNTRY_NAME) FROM flight_info)
# MAGIC AS flight_info"""
# MAGIC dbDataFrame = spark.read.format("jdbc")\
# MAGIC .option("url", url).option("dbtable", pushdownQuery).option("driver", driver)\
# MAGIC .load()

# COMMAND ----------

# MAGIC %md
# MAGIC ** Reading from databases in parallel:**
# MAGIC 
# MAGIC * Spark has an underlying algorithm that can read multiple files into one partition, or conversely, read multiple partitions out of one file, depending on the file size and the “splitability” of the file type and compression.
# MAGIC * this splitability exists with SQL as well but we need to configure it manually.
# MAGIC * It is the ability to specify a maximum number of partitions to allow you to limit how much you are reading and writing in parallel.
# MAGIC 
# MAGIC 
# MAGIC dbDataFrame = spark.read.format("jdbc")\
# MAGIC .option("url", url).option("dbtable", tablename).option("driver", driver)\
# MAGIC .option("numPartitions", 10).load()
# MAGIC 
# MAGIC dbDataFrame.select("DEST_COUNTRY_NAME").distinct().show()
# MAGIC 
# MAGIC * We can explicitly push predicates down into SQL databases through the connection itself. This optimization allows you to control the physical location of certain data in certain partitions by specifying predicates.
# MAGIC 
# MAGIC Eg: We need data related Anguilla and Sweden countries. We could filter these down and have them pushed into the database, 
# MAGIC     but we can also go further by having them arrive in their own partitions in Spark. 
# MAGIC     
# MAGIC     We do that by specifying a list of predicates when we create the data source
# MAGIC     
# MAGIC     # in Python
# MAGIC     props = {"driver":"org.sqlite.JDBC"}
# MAGIC     predicates = [
# MAGIC                   "DEST_COUNTRY_NAME = 'Sweden' OR ORIGIN_COUNTRY_NAME = 'Sweden'",
# MAGIC                   "DEST_COUNTRY_NAME = 'Anguilla' OR ORIGIN_COUNTRY_NAME = 'Anguilla'"]
# MAGIC 
# MAGIC     spark.read.jdbc(url, tablename, predicates=predicates, properties=props).show()
# MAGIC     
# MAGIC     spark.read.jdbc(url,tablename,predicates=predicates,properties=props).rdd.getNumPartitions()
# MAGIC     
# MAGIC If you specify predicates that are not disjoint, you can end up with lots of duplicate rows. an example set of predicates that will result in duplicate rows
# MAGIC 
# MAGIC     # in Python
# MAGIC     props = {"driver":"org.sqlite.JDBC"}
# MAGIC     
# MAGIC     predicates = [
# MAGIC                   "DEST_COUNTRY_NAME != 'Sweden' OR ORIGIN_COUNTRY_NAME != 'Sweden'",
# MAGIC                   "DEST_COUNTRY_NAME != 'Anguilla' OR ORIGIN_COUNTRY_NAME != 'Anguilla'"]
# MAGIC                   
# MAGIC     spark.read.jdbc(url, tablename, predicates=predicates, properties=props).count()

# COMMAND ----------

# MAGIC %md
# MAGIC **Partitioning based on a sliding window:**
# MAGIC 
# MAGIC * we can partition based on predicates.
# MAGIC * We will partition based on the "COUNT" column which is numerictype.
# MAGIC * specify a minimum and a maximum for both the first partition and last partition. Anything outside of these bounds will be in the first partition or final partition.
# MAGIC * Then, we set the number of partitions we would like total (this is the level of parallelism).
# MAGIC * Spark then queries our database in parallel and returns numPartitions partitions.
# MAGIC *  We simply modify the upper and lower bounds in order to place certain values in certain partitions.
# MAGIC * No filtering is taking place like we saw in the previous example
# MAGIC 
# MAGIC       colName = "count"
# MAGIC       lowerBound = 0L
# MAGIC       upperBound = 348113L # this is the max count in our database
# MAGIC       numPartitions = 10
# MAGIC       
# MAGIC This will distribute the intervals equally from low to high:
# MAGIC     
# MAGIC       spark.read.jdbc(url, tablename, column=colName, properties=props,
# MAGIC       lowerBound=lowerBound, upperBound=upperBound,
# MAGIC       numPartitions=numPartitions).count() # 255

# COMMAND ----------

# MAGIC %md
# MAGIC **Writing to SQL Databases:**
# MAGIC 
# MAGIC specify the URI and write out the data according to the specified write mode that you want.

# COMMAND ----------

# in Python
newPath = "jdbc:sqlite://tmp/my-sqlite.db"
props = {"driver":"org.sqlite.JDBC"}
csvFile.write.jdbc(newPath, tablename, mode="overwrite", properties=props)

# COMMAND ----------

# Let’s look at the results:
spark.read.jdbc(newPath, tablename, properties=props).count()

# COMMAND ----------

#we can append to the table this new table and see the increase in the count:
csvFile.write.jdbc(newPath, tablename, mode="append", properties=props)
spark.read.jdbc(newPath, tablename, properties=props).count()

# COMMAND ----------

# MAGIC %md
# MAGIC **Text Files:**
# MAGIC * Each line in the file becomes a record in the DataFrame.
# MAGIC * With **textFile**, partitioned directory names are ignored.
# MAGIC * To read and write text files according to partitions, we should use **text**, which respects partitioning on reading and writing

# COMMAND ----------

spark.read.text("/FileStore/tables/FileStore/tables/2015_summary-ebaee.csv")\
.selectExpr("split(value, ',') as rows").show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Writing Text Files:**

# COMMAND ----------

csvFile.select("DEST_COUNTRY_NAME").write.mode("overWrite").text("/tmp/simple-text-file.txt")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /tmp/

# COMMAND ----------

# MAGIC %md
# MAGIC * If you perform some partitioning when performing write you can write more columns. 
# MAGIC * However, those columns will manifest as directories in the folder to which you’re writing out to, instead of columns on every single file

# COMMAND ----------

csvFile.limit(10).select("DEST_COUNTRY_NAME", "count")\
.write.partitionBy("count").text("/tmp/five-csv-files2py.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC **Advanced I/O concepts:**
# MAGIC * we can control the parallelism of files that we write by controlling the partitions prior to writing.
# MAGIC * We can also control specific data layout by controlling two things
# MAGIC     * Bucketing
# MAGIC     * Partitioning
# MAGIC     
# MAGIC Splittable File Types and Compression:
# MAGIC * File splitting can improve speed because it avoids SPARK to read entire file and access parts of the file necessary to satisfy your query.
# MAGIC * If a file is on HDFS, splitting a file can provide further optimization if that file spans multiple blocks.
# MAGIC * Recommendation is Parquet with gzip compression.
# MAGIC 
# MAGIC Reading Data in Parallel:
# MAGIC * when you read from a folder with multiple files in it, each one of those files will become a partition in your DataFrame and be read in by available executors in parallel (with the remaining queueing up behind the others)
# MAGIC 
# MAGIC Writing Data in Parallel:
# MAGIC * The number of files or data written is dependent on the number of partitions the DataFrame has at the time you write out the data.
# MAGIC * By default, one file is written per partition of the data.
# MAGIC * This means that although we specify a “file,” it’s actually 
# MAGIC     * a number of files within a folder, 
# MAGIC     * with the name of the specified file, 
# MAGIC     * with one file per each partition that is written.

# COMMAND ----------

csvFile.repartition(5).write.format("csv").save("/tmp/multiple.csv")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /tmp/multiple.csv

# COMMAND ----------

# MAGIC %md
# MAGIC **Partitioning:**
# MAGIC 
# MAGIC * Partitioning is a tool that allows you to control what data is stored (and where) as you write it.
# MAGIC * When you write a file to a partitioned directory (or table), you basically encode a column as a folder.
# MAGIC * this allows you to do is skip lots of data when you go to read it in later, allowing you to read in only the data relevant to your problem instead of having to scan the complete dataset.
# MAGIC * These are supported for all file-based data sources.

# COMMAND ----------

csvFile.limit(10).write.mode("overWrite").partitionBy("DEST_COUNTRY_NAME")\
       .save("/tmp/partitioned-files.parquet")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /tmp/partitioned-files.parquet

# COMMAND ----------

# MAGIC %md
# MAGIC Each of these will contain Parquet files that contain that data where the previous predicate was true:

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /tmp/partitioned-files.parquet/DEST_COUNTRY_NAME=Senegal/

# COMMAND ----------

# MAGIC %md
# MAGIC This is the lowest hanging optimization that you can use when you have a table that readers frequently filter by before manipulating.
# MAGIC Eg: 
# MAGIC * Partition by Date is if we want to look at previous week's data(Instead of scanning entire list of records). This provides massive speedup for readers.

# COMMAND ----------

# MAGIC %md
# MAGIC **Bucketing:**
# MAGIC * one organization approach that helps control the data that is specifically written to each file.
# MAGIC * This helps t0 avoid **shuffles** later when we read the data because data with the same bucket ID will all be grouped together into one physical partition.
# MAGIC * This means that the data is prepartitioned according to how you expect to use that data later on.
# MAGIC * Avoids expensive shuffles when joining and aggregating.
# MAGIC 
# MAGIC Rather than partitioning on a specific column (which might write out a ton of directories), it’s probably worthwhile to explore bucketing the data instead. This will create a certain number of files and organize our data into those “buckets”

# COMMAND ----------

numberBuckets = 10
columnToBucketBy = "count"

csvFile.write.format("parquet").mode("overwrite")\
.bucketBy(numberBuckets, columnToBucketBy).saveAsTable("bucketedFiles")

# COMMAND ----------

# MAGIC %md
# MAGIC **Managing File Size:**
# MAGIC 
# MAGIC * "small file problem" - When we’re writing lots of small files, there’s a significant metadata overhead that you incur managing all of those files.
# MAGIC * opposite is also true- we don’t want files that are too large either, because it becomes inefficient to have to read entire blocks of data when you need only a few rows.
# MAGIC * Instead of having the number of files based on the number of partition at the time of writing, SPARK 2.2 introduces a new method to limit output file sizes.
# MAGIC * **maxRecordsPerFile** allows you to better control file sizes by controlling the number of records that are written to each file.
# MAGIC   
# MAGIC   df.write.option("maxRecordsPerFile", 5000) - Spark will ensure that files will contain at most 5,000 records.

# COMMAND ----------

