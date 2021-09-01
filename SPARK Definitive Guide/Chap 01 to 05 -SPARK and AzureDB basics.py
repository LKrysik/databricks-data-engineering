# Databricks notebook source
# MAGIC %md
# MAGIC ## DATABRICKS
# MAGIC 
# MAGIC Databricks is a zero-management cloud platform that provides:
# MAGIC 
# MAGIC * Fully managed Spark clusters
# MAGIC * An interactive workspace for exploration and visualization
# MAGIC * A production pipeline scheduler
# MAGIC * A platform for powering your favorite Spark-based applications

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Databricks File System
# MAGIC **Databricks File System(DBFS)** is a layer over Azure's blob storage
# MAGIC * Files in DBFS persist to the blob store, so data is not lost even after clusters are terminated
# MAGIC 
# MAGIC **Databricks Utilities - dbutils** 
# MAGIC * Access the DBFS through the Databricks utilities class (and other file IO routines)
# MAGIC * An instansce of DBUtils is already declared as dbutils
# MAGIC 
# MAGIC * **Note:** 
# MAGIC     * Please go through the "**The-Databricks-Environment.dbc**" and "**Databricks-Setup.pdf**" before proceeding further.
# MAGIC     * This dbc file is created only with **Python** code

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## APACHE SPARK ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png)
# MAGIC SPARK is a unified processing engine that helps in managing and coordinating the execution of tasks on data across a cluster of computers. 
# MAGIC 
# MAGIC ![Spark Engines](https://files.training.databricks.com/images/wiki-book/book_intro/spark_4engines.png)
# MAGIC <br/>
# MAGIC <br/>
# MAGIC * At its core is the Spark Engine.
# MAGIC * The DataFrames API provides an abstraction above RDDs while simultaneously improving performance 5-20x over traditional RDDs with its Catalyst Optimizer.
# MAGIC * Spark ML provides high quality and finely tuned machine learning algorithms for processing big data.
# MAGIC * The Graph processing API gives us an easily approachable API for modeling pairwise relationships between people, objects, or nodes in a network.
# MAGIC * The Streaming APIs give us End-to-End Fault Tolerance, with Exactly-Once semantics, and the possibility for sub-millisecond latency.
# MAGIC 
# MAGIC **RDDs**
# MAGIC * The primary data abstraction of Spark engine is the RDD: Resilient Distributed Dataset
# MAGIC   * Resilient, i.e., fault-tolerant with the help of RDD lineage graph and so able to recompute missing or damaged partitions due to node failures.
# MAGIC   * Distributed with data residing on multiple nodes in a cluster.
# MAGIC   * Dataset is a collection of partitioned data with primitive values or values of values, e.g., tuples or other objects.
# MAGIC * The original paper that gave birth to the concept of RDD is <a href="https://cs.stanford.edu/~matei/papers/2012/nsdi_spark.pdf" target="_blank">Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing</a> by Matei Zaharia et al.
# MAGIC * Today, with Spark 2.x, we treat RDDs as the assembly language of the Spark ecosystem.
# MAGIC * DataFrames, Datasets & SQL provide the higher level abstraction over RDDs.
# MAGIC 
# MAGIC 
# MAGIC **SPARK** is a distributed programming model where the user specifies
# MAGIC * **Transformations**, which build-up a directed acyclic- graph of instructions
# MAGIC * **Actions**, which begin the process of executing that graph of instructions, as a single job, by breaking it down into stages and tasks to execute across the cluster

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) The Cluster: Drivers, Executors, Slots & Tasks
# MAGIC ![Spark Physical Cluster, slots](https://files.training.databricks.com/images/105/spark_cluster_slots.png)
# MAGIC 
# MAGIC * The **Driver** is the JVM in which our application runs.
# MAGIC * The secret to Spark's awesome performance is parallelism.
# MAGIC   * Scaling vertically is limited to a finite amount of RAM, Threads and CPU speeds.
# MAGIC   * Scaling horizontally means we can simply add new "nodes" to the cluster almost endlessly.
# MAGIC * We parallelize at two levels:
# MAGIC   * The first level of parallelization is the **Executor** - a Java virtual machine running on a node, typically, one instance per node.
# MAGIC   * The second level of parallelization is the **Slot** - the number of which is determined by the number of cores and CPUs of each node.
# MAGIC * Each **Executor** has a number of **Slots** to which parallelized **Tasks** can be assigned to it by the **Driver**.

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Task,Jobs & Stages
# MAGIC * **Task** : A task is a series of operations that works on the same parition and operations in a task pipelined together.
# MAGIC * **Stage** : A group of tasks that operate on the same sequence of RDD is a stage.
# MAGIC * **Job** : A job is made up of all the stages in aquery that is a series of transformations completed by an action.
# MAGIC 
# MAGIC * Each parallelized action is referred to as a **Job**.
# MAGIC * The results of each **Job** (parallelized/distributed action) is returned to the **Driver**.
# MAGIC * Depending on the work required, multiple **Jobs** will be required.
# MAGIC * Each **Job** is broken down into **Stages**.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ![Spark Physical Cluster, tasks](https://files.training.databricks.com/images/105/spark_cluster_tasks.png)
# MAGIC <br/>
# MAGIC <br/>
# MAGIC * The JVM is naturally multithreaded, but a single JVM, such as our **Driver**, has a finite upper limit.
# MAGIC * By creating **Tasks**, the **Driver** can assign units of work to **Slots** for parallel execution.
# MAGIC * Additionally, the **Driver** must also decide how to partition the data so that it can be distributed for parallel processing (not shown here).
# MAGIC * Consequently, the **Driver** is assigning a **Partition** of data to each task - in this way each **Task** knows which piece of data it is to process.
# MAGIC * Once started, each **Task** will fetch from the original data source the **Partition** of data assigned to it.

# COMMAND ----------

dbutils.help()

# COMMAND ----------

spark

# COMMAND ----------

myrange = spark.range(1000).toDF("number")

# COMMAND ----------

myrange.printSchema()

# COMMAND ----------

divisBy2 = myrange.where("number % 2 = 0")

# COMMAND ----------

divisBy2.count()

# COMMAND ----------

# MAGIC %md
# MAGIC * This dbc file is created based on my understanding from ADB training and SPARK definitive guide from Databricks
# MAGIC * All the data that will be used as part of this can be downloaded from "https://github.com/databricks/Spark-The-Definitive-Guide"

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/FileStore/tables

# COMMAND ----------

# MAGIC %fs head /FileStore/tables/FileStore/tables/flight_csv.txt

# COMMAND ----------

flight_csv.txtflightData2015 = spark\
            .read\
            .option("inferSchema","true")\
            .option("header","true")\
            .csv("/FileStore/tables/FileStore/tables/flight_csv.txt")

# COMMAND ----------

flightData2015.take(3)

# COMMAND ----------

flightData2015.show(3)

# COMMAND ----------

flightData2015.printSchema()

# COMMAND ----------

flightData2015.sort('count').explain()

# COMMAND ----------

spark.conf.get('spark.sql.shuffle.partitions')

# COMMAND ----------

spark.conf.set('spark.sql.shuffle.partitions',"5")

# COMMAND ----------

flightData2015.sort('count').show()

# COMMAND ----------

flightData2015.sort('count').take(2)

# COMMAND ----------

# MAGIC %md
# MAGIC Any dataFrame can be made into a table or view with a simple method called **createOrReplaceTempView**

# COMMAND ----------

flightData2015.createOrReplaceTempView("flight_data_2015")

# COMMAND ----------

# MAGIC %md
# MAGIC To execute a SQL query, we’ll use the **spark.sql** function (remember spark is our SparkSession variable?) that conveniently, returns a new DataFrame

# COMMAND ----------

sqlWay = spark.sql("SELECT DEST_COUNTRY_NAME, count(1) FROM flight_data_2015 GROUP BY DEST_COUNTRY_NAME")

# COMMAND ----------

dataFrameWay = flightData2015\
              .groupBy("DEST_COUNTRY_NAME")\
              .count()

# COMMAND ----------

# MAGIC %md
# MAGIC We can see that these plans compile to the exact same underlying plan!

# COMMAND ----------

sqlWay.explain()
dataFrameWay.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC use the **max function** to find out what the maximum number of flights to and from any given location

# COMMAND ----------

spark.sql("SELECT max(count) from flight_data_2015").take(1)

# COMMAND ----------

from pyspark.sql.functions import max

flightData2015.select(max("count")).take(1)

# COMMAND ----------

display(flightData2015)

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

flightData2015.groupBy('DEST_COUNTRY_NAME').agg(F.sum('count').alias('sum')).orderBy(F.col('sum').desc()).limit(5).show()

# COMMAND ----------

flightData2015.groupBy('DEST_COUNTRY_NAME').agg(F.sum('count').alias('sum')).orderBy(F.col('sum').desc()).limit(5).explain()

# COMMAND ----------

# MAGIC %md
# MAGIC To get top five destination countries using SPARK SQL

# COMMAND ----------

maxSql = spark.sql("select DEST_COUNTRY_NAME, sum(count) as destination_total \
           from flight_data_2015\
           GROUP By DEST_COUNTRY_NAME\
           ORDER By sum(count) DESC\
           LIMIT 5")
maxSql.collect()

# COMMAND ----------

# MAGIC %md 
# MAGIC Writing the same using DataFrames

# COMMAND ----------

from pyspark.sql.functions import desc

dataFrameMax = flightData2015\
              .groupBy('DEST_COUNTRY_NAME')\
              .sum('count')\
              .withColumnRenamed("sum(count)","destination_total")\
              .sort(desc("destination_total"))\
              .limit(5)

dataFrameMax.collect()

# COMMAND ----------

maxSql.explain()
dataFrameMax.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## DATASET
# MAGIC 
# MAGIC ** DataFrames** are
# MAGIC a distributed collection of objects of type Row but Spark also allows JVM users to create their own objects (via case classes or java beans) and manipulate them using function programming concepts.
# MAGIC 
# MAGIC Amazing thing about Datasets is that we can use them only when we need or want to.ability to manipulate arbitrary case classes with arbitrary functions makes expressing business logic simple.
# MAGIC 
# MAGIC 
# MAGIC I’ll define my own object and manipulate it via arbitrary map and filter functions. Once we’ve
# MAGIC performed our manipulations, Spark can automatically turn it back into a DataFrame and we can manipulate it
# MAGIC further using the hundreds of functions that Spark includes. This makes it easy to drop down to lower level, type
# MAGIC secure coding when necessary, and move higher up to SQL for more rapid analysis

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC case class ValueAndDouble(value:Long, valueDoubled:Long)
# MAGIC 
# MAGIC spark.range(2000)
# MAGIC       .map(value => ValueAndDouble(value, value*2))
# MAGIC       .filter(vAndD => vAndD.valueDoubled % 2 == 0)
# MAGIC       .where("value%3 =0")
# MAGIC       .count()

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Caching Data for Faster Access
# MAGIC 
# MAGIC Sometimes we are going to access a dataFrame multiple times in the same data flow and we want to avoid performing expensive joins over and over again. To avoid that we can use **CACHE()** function and to cache the data in memory

# COMMAND ----------

DF1 = spark.read.format("csv")\
      .option("inferschema","true")\
      .option("header","true")\
      .load("/FileStore/tables/FileStore/tables/2015_summary-ebaee.csv")


# COMMAND ----------

DF2 = DF1.groupBy("DEST_COUNTRY_NAME").count().collect()
DF3 = DF1.groupBy("ORIGIN_COUNTRY_NAME").count().collect()
DF4 = DF1.groupBy("count").count().collect()

# COMMAND ----------

# MAGIC %md
# MAGIC The time taken to process these commands is to 3.07 seconds

# COMMAND ----------

# MAGIC %md 
# MAGIC Now using cache to estimate the time taken

# COMMAND ----------

DF1.cache()
DF1.count()

# COMMAND ----------

DF2 = DF1.groupBy("DEST_COUNTRY_NAME").count().collect()
DF3 = DF1.groupBy("ORIGIN_COUNTRY_NAME").count().collect()
DF4 = DF1.groupBy("count").count().collect()

# COMMAND ----------

# MAGIC %md
# MAGIC This may not seem that wild but picture a large data set or one that requires a lot of computation in order to create. The savings can be immense.

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Structured Streaming
# MAGIC 
# MAGIC The best thing about Structured Streaming is that it allows you to rapidly and quickly get value out of streaming systems with simple switches, it also makes it easy to reason about because you can write your batch job as a way to prototype it and then you can convert it to streaming job.
# MAGIC 
# MAGIC 
# MAGIC The way all of this works is by incrementally processing that data.
# MAGIC 
# MAGIC 
# MAGIC * Note: Upload the **retail_data** found in "https://github.com/databricks/Spark-The-Definitive-Guide"

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/retail-data/by-day/

# COMMAND ----------

staticDataFrame = spark.read.format("csv")\
                  .option("header","true")\
                  .option("inferSchema","true")\
                  .load("/FileStore/tables/retail-data/by-day/*.csv")

# COMMAND ----------

staticDataFrame.createOrReplaceTempView("retail_data")

# COMMAND ----------

staticSchema = staticDataFrame.schema
staticSchema

# COMMAND ----------

staticDataFrame.show(5)

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

(staticDataFrame.select(F.col('CustomerID'),(F.col('Quantity')*F.col('UnitPrice')).alias("total_cost"),F.col('InvoiceDate'))
                .groupBy(F.col('CustomerID'),F.window(F.col('InvoiceDate'),"1 day")).agg(F.sum('total_cost').alias("sum"))
                .orderBy(F.col('sum').desc()).show(5))

# COMMAND ----------

# MAGIC %md 
# MAGIC To find the largest sale hours where given customer makes large purchase 

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "5")

# COMMAND ----------

staticDataFrame.select(F.e)

# COMMAND ----------

from pyspark.sql.functions import window, column, desc, col

staticDataFrame\
  .selectExpr(
  "CustomerID",
  "(UnitPrice * Quantity) as total_cost",
  "InvoiceDate")\
  .groupBy(
  col("CustomerID"),window(col("InvoiceDate"),"1 day"))\
  .sum("total_cost")\
  .orderBy(desc("sum(total_cost)"))\
  .show(5)


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC sum(total_cost),
# MAGIC CustomerId,
# MAGIC to_date(InvoiceDate) AS Invoice_date
# MAGIC FROM
# MAGIC (SELECT
# MAGIC CustomerId,
# MAGIC (UnitPrice * Quantity) as total_cost,
# MAGIC InvoiceDate
# MAGIC FROM
# MAGIC retail_data)
# MAGIC GROUP BY
# MAGIC CustomerId,to_date(InvoiceDate)
# MAGIC ORDER By
# MAGIC sum(total_cost) DESC

# COMMAND ----------

# MAGIC %md
# MAGIC That’s the static DataFrame version. To make this as **streaming** use **readstream** instead of **read**
# MAGIC 
# MAGIC We are also using **maxFilesPerTrigger** which simply specifies the number of file we should read in at once. This is to make
# MAGIC our demonstration more “streaming” and in a production scenario this would be omitted.
# MAGIC 
# MAGIC The number of partitions that should be created after a shuffle, by default the value is **200** but since there aren’t many executors on my local machine it’s worth reducing this to **five**.

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "5")

streamingDataFrame = spark.readStream\
                    .schema(staticSchema)\
                    .option("maxFilesPerTrigger",1)\
                    .format("csv")\
                    .option("header","true")\
                    .load("/FileStore/tables/retail-data/by-day/*.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can see the DataFrame is streaming **streamingDataFrame.isStreaming // returns true**
# MAGIC 
# MAGIC This is still a lazy operation, so we need to call streaming action to start the execution flow. So we will run the same query. It is expected Spark will only read in **one file at a time**.

# COMMAND ----------

streamingDataFrame.isStreaming

# COMMAND ----------

purchaseByCustomerPerHour = streamingDataFrame\
                            .selectExpr(
                            "CustomerId",
                            "(UnitPrice * Quantity) as total_cost",
                            "InvoiceDate")\
                            .groupBy(col("CustomerId"),window(col("InvoiceDate"),"1 day"))\
                            .sum("total_cost")

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Now let’s kick off the stream! We’ll write it out to an in-memory table that we will update after each trigger. In this
# MAGIC case, each trigger is based on an individual file (the read option that we set). Spark will mutate the data in the inmemory
# MAGIC table such that we will always have the highest value.

# COMMAND ----------

purchaseByCustomerPerHour.writeStream\
                        .format("memory")\
                        .queryName("customer_purchases")\
                        .outputMode("complete")\
                        .start()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now we can run queries against this table. Note to take a minute before doing so, this will allow the values to change
# MAGIC over time.

# COMMAND ----------

spark.sql("SELECT * FROM customer_purchases\
          ORDER BY 'sum(total_cost)' DESC")\
          .take(5)

# COMMAND ----------

# MAGIC %md
# MAGIC You’ll notice that as we read in more data - the composition of our table changes! With each file the results may or
# MAGIC may not be changing based on the data. Since we’re grouping customers we hope to see an increase in the
# MAGIC top customer purchase amounts over time

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## STRUCTURED API OVERVIEW
# MAGIC 
# MAGIC Structured APIs are a way of manipulating all sorts of data, from unstructured log files, to semi-structured CSV files, and highly structured Parquet files. 
# MAGIC These APIs refer to three core types of distributed collection APIs.
# MAGIC 
# MAGIC * Datasets
# MAGIC * DataFrames
# MAGIC * SQL Views and Tables

# COMMAND ----------

# MAGIC %md 
# MAGIC The way we store data on which to perform transformations and actions are DataFrames and Datasets. 
# MAGIC 
# MAGIC 
# MAGIC * To create a new DataFrame or Dataset, we call a **transformation**.
# MAGIC * To start computation or convert to native language types, we call an **action**.
# MAGIC * DataFrames and Datasets are (distributed) table like collections with well-defined rows and columns. Both are **immutable** and **lazily-evaluated**
# MAGIC * Each column must have the same number of rows as all the other columns (although you can use null to specify the lack of a value) and columns have type information that must be consistent for every row in the collection

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Structured SPARK Types
# MAGIC * Spark is effectively a programming language of its own. It use **Catalyst Optimizer** to effectively execute the code. 
# MAGIC * Spark types map directlyto the different language APIs that Spark maintains and there exists a lookup table for each of these in each of Scala,Java, Python, SQL, and R.
# MAGIC     * Even if we use Spark’s Structured APIs from Python/R, the majority of our manipulations will operate strictly on Spark types, not   Python types. 
# MAGIC     For example, the below code does not perform addition in Scala or Python, it actually performs addition purely in Spark.

# COMMAND ----------

df = spark.range(1000).toDF("number")
df.select(df["number"]+10)

# COMMAND ----------

from pyspark.sql.types import 

# COMMAND ----------

# MAGIC %md
# MAGIC This addition operation happens because Spark will convert an expression expressed in an input language to Spark’s
# MAGIC internal Catalyst representation of that same type information.

# COMMAND ----------

# MAGIC %md
# MAGIC ###### DataFrame Vs DataSet
# MAGIC * DataFrames are **untyped** by definition, but they have types internally but Spark maintains them completely and only checks whether those types line up to those specified in the schema at runtime
# MAGIC * DataSets check whether or not **types** conform to the specification at compile time. Datasets are only available to JVM based languages (Scala and Java) and we specify types with case classes or Java beans.
# MAGIC * To Spark in Scala, DataFrames are simply Datasets of Type Row. 
# MAGIC 
# MAGIC **The “Row” type is Spark’s internal representation of its optimized in memory format for computation. This format makes for highly specialized and efficient computation because rather than leveraging JVM types which can cause high garbage collection and object instantiation costs, Spark can operate on its own internal format without incurring any of those costs**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Structured API Execution
# MAGIC 
# MAGIC The execution of a single structured API query from user code to executed code in following steps.
# MAGIC 1. Write DataFrame/Dataset/SQL Code
# MAGIC 2. If valid code, Spark converts this to a **Logical Plan**
# MAGIC 3. Spark transforms this Logical Plan to a **Physical Plan**
# MAGIC 4. Spark then executes this Physical Plan on the cluster
# MAGIC 
# MAGIC ![Catalyst-Optimizer-diagram](https://databricks.com/wp-content/uploads/2018/05/Catalyst-Optimizer-diagram.png)
# MAGIC <br/>
# MAGIC <br/>
# MAGIC 
# MAGIC 
# MAGIC written code is submitted to Spark either through the console or via a submitted job.This code then passes through the **Catalyst Optimizer** which decides how the code should be executed and lays out a plan for doing so, before finally the code is run and the result is returned to the user.
# MAGIC 
# MAGIC ###### Logical Planning
# MAGIC * This logical plan only represents a set of abstract transformations that do not refer to executors or drivers, it’s purely to convert the user’s set of expressions into the most optimized version.
# MAGIC * It does this by converting user code into an unresolved logical plan. This unresolved because while your code may be valid, the tables or columns that it refers to may or may not exist. 
# MAGIC * Spark uses the catalog, a repository of all table and DataFrame information, in order to resolve columns and tables in the analyzer. The analyzer may reject the unresolved logical plan if it the required table or column name does not exist in the catalog. 
# MAGIC * If it can resolve it, this result is passed through the optimizer, a collection of rules, which attempts to optimize the logical plan by **pushing down predicates or selections**.
# MAGIC 
# MAGIC ###### Physical Planning
# MAGIC * The physical plan, often called a Spark plan, specifies how the logical plan will execute on the cluster by generating different physical execution strategies and comparing them through a cost model.  
# MAGIC   * An example of the cost comparison might be choosing how to perform a given join by looking at the physical attributes of a given table (how big the table is or how big its partitions are.)
# MAGIC * Physical planning results in a series of RDDs and transformations.
# MAGIC 
# MAGIC ###### Execution
# MAGIC * Upon selecting a physical plan, Spark runs all of this code over RDDs, the lower-level programming interface of Spark.
# MAGIC * Spark performs further optimizations by, at runtime, generating native Java Bytecode that can remove whole tasks or stages during execution. Finally the result is returned to the user.

# COMMAND ----------

# MAGIC %md
# MAGIC # Basic Structure Operations
# MAGIC ###### Schemas
# MAGIC * schema defines the column names and types of a DataFrame. 
# MAGIC * Users can define schemas manually or users can read a schema from a data source (often called schema on read)
# MAGIC 
# MAGIC ###### Partitioning
# MAGIC * The partitioning of the DataFrame defines the layout of the DataFrame or Dataset’s physical distribution across the cluster.
# MAGIC * The partitioning scheme defines how that is broken up, this can be set to be based on values in a certain column or non-deterministically.

# COMMAND ----------

df = spark.read.format("json")\
      .load("/FileStore/tables/2015_summary-ebaee.json")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC When using Spark for production ETL, it is often a good idea to define your schemas manually, especially when working with untyped data sources like csv and json because schema inference can vary depending on the type of data that you read in.
# MAGIC 
# MAGIC A schema is a 
# MAGIC * **StructType** made up of a number of fields
# MAGIC * **StructFields**, that have a name, type, and a boolean flag which specifies whether or not that column can contain missing or null values. Schemas can also contain other StructType (Spark’s complex types).

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, LongType

myManualSchema = StructType([
  StructField("DEST_COUNTRY_NAME",StringType(),True),
  StructField("ORIGIN_COUNTRY_NAME",StringType(),True),
  StructField("count",LongType(),False)
])
df = spark.read.format("json")\
      .schema(myManualSchema)\
      .load("/FileStore/tables/2015_summary-ebaee.json")

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType,StringType, LongType

myManualSchema = StructType([
  StructField("some",StringType(),True),
  StructField("col",StringType(),True),
  StructField("names",LongType(),False)
])

myRow = Row("Hello", None, 1)
myDf = spark.createDataFrame([myRow], myManualSchema)
myDf.show()

# COMMAND ----------

myDf.columns

# COMMAND ----------

# MAGIC %md
# MAGIC USE
# MAGIC * **select** method when you’re working with columns or expressions 
# MAGIC * **selectExpr** method when you’re working with expressions in strings.

# COMMAND ----------

df.select("DEST_COUNTRY_NAME","ORIGIN_COUNTRY_NAME").show(2)

# COMMAND ----------

df.selectExpr(
"*", # all original columns
"(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry" )\
.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### DataFrame Transformations
# MAGIC create DataFrames on the fly by taking a set of rows and converting them to a DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC ###### DataFrame Transformations
# MAGIC create DataFrames on the fly by taking a set of rows and converting them to a DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###### Adding Columns
# MAGIC * new column can be added to a DataFrame using the **withColumn** method on DataFrame. 
# MAGIC * withColumn function takes two arguments: 
# MAGIC   * the column name and 
# MAGIC   * the expression that will create the value for that given row in the DataFrame

# COMMAND ----------

from pyspark.sql.functions import lit
from pyspark.sql.functions import expr, col, column

df.withColumn("numberOne", lit(1)).show(2)

# COMMAND ----------

df.withColumn(
  "withinCountry",
  expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))\
  .show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Renaming Columns
# MAGIC Using **withCOlumn** name works to rename the column, it’s often much easier (and readable) to use the **withColumnRenamed** method.

# COMMAND ----------

df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Reserved Characters and Keywords in Column Names
# MAGIC 
# MAGIC One thing that you may come across is reserved characters like spaces or dashes in column names. Handling these means escaping column names appropriately. In Spark this is done with backtick (‘) characters. 
# MAGIC 
# MAGIC Let’s use the withColumn that we just learned about to create a Column with reserved characters.

# COMMAND ----------

dfWithLongColName = df.withColumn("This Long Column-Name",expr("ORIGIN_COUNTRY_NAME"))

dfWithLongColName.selectExpr("`This Long Column-Name`","`This Long Column-Name` as `new col`").show(2)
dfWithLongColName.createOrReplaceTempView("dfTableLong")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Removing Columns
# MAGIC 
# MAGIC we can remove columns using **select** method. However there is also a dedicated method called **drop**

# COMMAND ----------

dfWithLongColName.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###### Changing a Column’s Type (cast)
# MAGIC We can convert columns from one type to another by **casting** the column from one type to another.For instance let’s convert our count column from an integer to a Long type.

# COMMAND ----------

df.printSchema()
df.withColumn("count", col("count").cast("int")).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###### Filtering Rows
# MAGIC There are two methods to perform this operation, we can use where or filter and they both will perform the same operation and accept the same argument types when used with DataFrames

# COMMAND ----------

colCondition = df.filter(col("count") < 2).take(2)
conditional = df.where("count < 2").take(2)

# COMMAND ----------

from pyspark.sql import Row

schema = df.schema

newRows = [Row("New Country", "Other Country", 5), Row("New Country 2", "Other Country 3", 1)]

parallelizedRows = spark.sparkContext.parallelize(newRows)
newDF = spark.createDataFrame(parallelizedRows, schema)

# COMMAND ----------

df.union(newDF)\
  .where("count = 1")\
  .where(col("ORIGIN_COUNTRY_NAME") != "United States")\
  .show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Limit
# MAGIC Often times you may just want the top ten of some DataFrame. For example, you might want to only work with the top
# MAGIC 50 of some dataset. We do this with the limit method.

# COMMAND ----------

df.limit(5).show()
df.orderBy(expr("count desc")).limit(6).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Repartition and Coalesce
# MAGIC 
# MAGIC Repartition will incur a full shuffle of the data, regardless of whether or not one is necessary. This means that you should typically only repartition when the future number of partitions is greater than your current number of partitions or when you are looking to partition by a set of columns.

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df.repartition(5)

# COMMAND ----------

# MAGIC %md
# MAGIC If we know we are going to be filtering by a certain column often, it can be worth repartitioning based on that column.

# COMMAND ----------

df.repartition(col("DEST_COUNTRY_NAME"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We can optionally specify the number of partitions we would like too.

# COMMAND ----------

df.repartition(5, col("DEST_COUNTRY_NAME"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Coalesce** on the other hand will not incur a full shuffle and will try to combine partitions. This operation will shuffle
# MAGIC our data into 5 partitions based on the destination country name, then coalesce them (without a full shuffle).

# COMMAND ----------

df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###### Collecting Rows to the Driver
# MAGIC Spark has a Driver that maintains cluster information and runs user code. This means that when we call some method to collect data, this is collected to the Spark Driver.
# MAGIC 
# MAGIC * **collect** gets all data from the entire DataFrame
# MAGIC * **take** selects the first N rows
# MAGIC * **show** prints out a number of rows nicely.

# COMMAND ----------

collectDF = df.limit(10)
collectDF.take(5) # take works with an Integer count
collectDF.show() # this prints it out nicely

# COMMAND ----------

collectDF.show(5, False)
collectDF.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Please use ** SPARK Definitive Guide** to understand more