# Databricks notebook source
# MAGIC %md
# MAGIC ###### DataSets:
# MAGIC * Datasets are the foundational type of the Structured APIs.
# MAGIC * DataFrames are Datasets of type Row.
# MAGIC * DS are strictly JVM language feature that work only with Scala and Java.
# MAGIC * Using DS, we can define each row in your Dataset will consist of. This will define the schema
# MAGIC   * In Scala it will be Case class object 
# MAGIC   * In Java it is defined as Java Bean
# MAGIC * when we use DF API we are not creating string or integers, SPARK will convert the type using **Encoder**. The encoder maps the domain-specific type T to Spark’s internal type system.
# MAGIC   * Example: When we create a class **Person** with two fields, name(string) and age(int), an encoder directs SPARK to generate code at runtime to serialize to serialize the person object into binary structure.
# MAGIC * When you use the Dataset API, for every row it touches, this domain specifies type, Spark converts the Spark Row format to the object you specified (a case class or Java class). This conversion slows down your operations but can provide more flexibility.

# COMMAND ----------

# MAGIC %md
# MAGIC ###### When to Use Datasets:
# MAGIC when DataSet is making Operation slow, why should we use it ?
# MAGIC 
# MAGIC * When the Operations we woulld like to perform cannot be expressed Using DataFrame manipulations.
# MAGIC 
# MAGIC   Eg: we might have a large set of business logic that you’d like to encode in one specific function instead of in SQL or DataFrames.
# MAGIC   
# MAGIC * When we need type safety and willing to accept the cost of performance to acheive it. If correctness and bulletproof code is your highest priority, at the cost of some performance, this can be a great choice for you.
# MAGIC 
# MAGIC   Eg: Operations that are not valid for their types, say subtracting two string types, will fail at compilation time not at runtime.
# MAGIC 
# MAGIC * when you would like to reuse a variety of transformations of entire rows between single-node workloads and Spark workloads.one advantage of using Datasets is that if you define all of your data and transformations as accepting case classes it is trivial to reuse them for both distributed and local workloads.
# MAGIC 
# MAGIC * The Most popular use case is to use DataFrames and Datasets in tandem, manually trading off between performance and type safety when it is most relevant for your workload.
# MAGIC   * at the end of a large, DF-based ETL transformation when you’d like to collect data to the driver and manipulate it by using singlenode libraries
# MAGIC   * at the beginning of a transformation when you need to perform per- row parsing before performing filtering and further manipulation in Spark SQL.

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Creating Datasets:
# MAGIC 
# MAGIC Creating Datasets is somewhat of a manual operation, requiring you to know and define the schemas ahead of time.
# MAGIC 
# MAGIC **In Scala:Case Class**
# MAGIC 
# MAGIC To create Datasets in Scala, you define a Scala case class. A case class is a regular class that has following characterstics
# MAGIC 
# MAGIC * Immutable.
# MAGIC * Decomposable through pattern matching.
# MAGIC * Allows for comparison based on Structure instead of reference.
# MAGIC * Easy to use and manipulate.
# MAGIC 
# MAGIC Scala documentation describes it as :
# MAGIC * Immutability frees you from needing to keep track of where and when things are mutated.
# MAGIC * Comparison-by-value allows you to compare instances as if they were primitive values —no more uncertainty regarding whether instances of a class are compared by value or reference
# MAGIC * Pattern matching simplifies branching logic, which leads to less bugs and more readable code.

# COMMAND ----------

# MAGIC %scala
# MAGIC case class Flight(DEST_COUNTRY_NAME: String,
# MAGIC                   ORIGIN_COUNTRY_NAME: String, count: BigInt)
# MAGIC 
# MAGIC val flightsDF = spark.read.json("/FileStore/tables/2015_summary-ebaee.json")
# MAGIC val flights = flightsDF.as[Flight]

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Actions:
# MAGIC * actions like collect, take, and count apply to whether we are using Datasets or DataFrames.
# MAGIC * go to access one of the case classes, we don’t need to do any type coercion, we simply specify the named attribute of the case class and get back

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC flights.show(2)
# MAGIC flights.first.DEST_COUNTRY_NAME

# COMMAND ----------

# MAGIC %scala
# MAGIC flights.first

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Transformations:
# MAGIC Datasets allow us to specify more complex and strongly typed transformations than we could perform on DataFrames alone because we manipulate raw Java Virtual Machine (JVM) types. Eg: filtering the dataset we created.
# MAGIC 
# MAGIC **Filtering:**
# MAGIC creating a simple function that accepts a Flight and returns a Boolean value that describes whether the origin and destination are the same.
# MAGIC 
# MAGIC **Note:**
# MAGIC By specifying a function, we are forcing Spark to evaluate this function on every row in our Dataset. This can be very resource intensive. For simple filters it is always preferred to write SQL expressions. This will greatly reduce the cost of filtering out the data while still allowing you to manipulate it as a Dataset later on

# COMMAND ----------

# MAGIC %scala
# MAGIC def originIsDestination(flight_row: Flight): Boolean = {
# MAGIC   return flight_row.ORIGIN_COUNTRY_NAME == flight_row.DEST_COUNTRY_NAME
# MAGIC   }
# MAGIC 
# MAGIC flights.filter(flight_row => originIsDestination(flight_row)).first()

# COMMAND ----------

# MAGIC %md
# MAGIC this dataset is small enough for us to collect to the driver (as an Array of Flights) on which we can operate and perform the exact same filtering operation. 
# MAGIC 
# MAGIC We can see that we get the exact same answer as before.

# COMMAND ----------

# MAGIC %scala
# MAGIC flights.collect().filter(flight_row => originIsDestination(flight_row))

# COMMAND ----------

# MAGIC %md
# MAGIC **Mapping:**
# MAGIC to perform something more sophisticated like extract a value and compare a set of values

# COMMAND ----------

# MAGIC %scala
# MAGIC val destinations = flights.map(f => f.DEST_COUNTRY_NAME)
# MAGIC val localDestinations = destinations.take(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Even though we can perform the same using Dataframe so effectively, It is recommended to do this. You will gain advantages like code generation that are simply not possible with arbitrary user-defined functions. However, this can come in handy with much more sophisticated row-by-row manipulation.

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Joins:
# MAGIC 
# MAGIC * Datasets provide a more sophisticated method, the **joinWith** method.
# MAGIC * joinWith is roughly equal to a co-group (in RDD terminology) and you basically end up with two nested Datasets inside of one.
# MAGIC * Each column represents one Dataset and these can be manipulated accordingly.
# MAGIC * This gets handy, when we need to maintain more information in the join or perform some more sophisticated manipulation on the entire result, like an advanced map or filter.

# COMMAND ----------

# MAGIC %scala
# MAGIC case class FlightMetadata(count: BigInt, randomData: BigInt)
# MAGIC 
# MAGIC val flightsMeta = spark.range(500)
# MAGIC                   .map(x => (x,scala.util.Random.nextLong))
# MAGIC                   .withColumnRenamed("_1","count")
# MAGIC                   .withColumnRenamed("_2","randomData")
# MAGIC                   .as[FlightMetadata]
# MAGIC 
# MAGIC val flights2 = flights
# MAGIC                .joinWith(flightsMeta, flights.col("count") === flightsMeta.col("count"))

# COMMAND ----------

# MAGIC %md
# MAGIC now we have Dataset of a sort of key-value pair, in which each row represents a Flight and the Flight Metadata.
# MAGIC 
# MAGIC Querying it with complex types

# COMMAND ----------

# MAGIC %scala
# MAGIC flights2.selectExpr("_1.DEST_COUNTRY_NAME")

# COMMAND ----------

# MAGIC %scala
# MAGIC flights2.take(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Grouping and Aggregations:
# MAGIC * groupBy, rollup and cube still apply, but these return DataFrames instead of Datasets (We lose type information)
# MAGIC * If we want to keep type information there are other groupings and aggregations. Eg: groupByKey
# MAGIC * 

# COMMAND ----------

# MAGIC %scala
# MAGIC flights.groupBy("DEST_COUNTRY_NAME").count()

# COMMAND ----------

# MAGIC %scala
# MAGIC flights.groupByKey(x => x.DEST_COUNTRY_NAME).count()

# COMMAND ----------

# MAGIC %md
# MAGIC Although this provides flexibility, it’s a trade-off because now we are introducing JVM types as well as functions that cannot be optimized by Spark. This means that you will see a performance difference

# COMMAND ----------

# MAGIC %scala
# MAGIC flights.groupByKey(x => x.DEST_COUNTRY_NAME).count().explain

# COMMAND ----------

# MAGIC %md
# MAGIC After we perform a grouping with a key on a Dataset, we can operate on the Key Value Dataset with functions that will manipulate the groupings as raw objects

# COMMAND ----------

# MAGIC %scala
# MAGIC def grpSum(countryName:String, values: Iterator[Flight]) = {
# MAGIC   values.dropWhile(_.count < 5).map(x => (countryName, x))
# MAGIC   }
# MAGIC 
# MAGIC flights.groupByKey(x => x.DEST_COUNTRY_NAME).flatMapGroups(grpSum).show(5)

# COMMAND ----------

# MAGIC %scala
# MAGIC def grpSum2(f:Flight):Integer = {
# MAGIC   1
# MAGIC   }
# MAGIC 
# MAGIC flights.groupByKey(x => x.DEST_COUNTRY_NAME).mapValues(grpSum2).count().take(5)

# COMMAND ----------

# MAGIC %md
# MAGIC We can even create new manipulations and define how groups should be reduced
# MAGIC 
# MAGIC It should be straightfoward enough to understand that this is a more expensive process than aggregating immediately after scanning, especially because it ends up in the same end result.

# COMMAND ----------

# MAGIC %scala
# MAGIC def sum2(left:Flight, right:Flight) = {
# MAGIC   Flight(left.DEST_COUNTRY_NAME, null, left.count + right.count)
# MAGIC   }
# MAGIC 
# MAGIC flights.groupByKey(x => x.DEST_COUNTRY_NAME).reduceGroups((l, r) => sum2(l, r)).take(5)

# COMMAND ----------

# MAGIC %scala
# MAGIC flights.groupBy("DEST_COUNTRY_NAME").count().explain

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Conclusion:
# MAGIC This should motivate using Datasets only with user-defined encoding surgically and only where it makes sense. 
# MAGIC This might be at the beginning of a big data pipeline or at the end of one.