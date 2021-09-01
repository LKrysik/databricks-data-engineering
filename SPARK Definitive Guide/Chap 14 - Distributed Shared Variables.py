# Databricks notebook source
# MAGIC %md
# MAGIC The second kind of low-level API in SPARK is two types of distributed shared variables:
# MAGIC * **Broadcast Variables** - let us save a large value on all the worker nodes and reuse it across many Spark actions without re-sending it to the cluster.
# MAGIC * **Accumlators** - lets us to add data together from all the tasks into the shared result.

# COMMAND ----------

# MAGIC %md
# MAGIC **Broadcast Variables:**
# MAGIC * Broadcast variables are a to share an immutable value efficiently around the cluster without encapsulating that variable in a function closure.
# MAGIC * This concept is more useful when we are using LookUp table or machine learning model.
# MAGIC * Broadcast Variables are shared, immutable variables that are cached on every machine in the cluster instead of serialized with every single task.

# COMMAND ----------

# suppose we have a list of words or values:
my_collection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
words = spark.sparkContext.parallelize(my_collection, 2)

supplementalData = {"Spark":1000, "Defnitive":200,\
                   "Big":-300,"Simple":100}

# COMMAND ----------

# MAGIC %md
# MAGIC * We can broadcast this structure across Spark and reference it by using suppBroadcast.
# MAGIC * This value is immutable and is lazily replicated across all nodes in the cluster when we trigger an action

# COMMAND ----------

suppBroadcast = spark.sparkContext.broadcast(supplementalData)

# COMMAND ----------

# MAGIC %md
# MAGIC * We can reference this variable via **value** method. This method is accessible within serialized functions without having to serialize the data.
# MAGIC * Doing this way can save a great deal of serialization and deserialization costs.

# COMMAND ----------

suppBroadcast.value

# COMMAND ----------

words.map(lambda word: (word, suppBroadcast.value.get(word,0)))\
     .sortBy(lambda wordPair:wordPair[1])\
     .collect()

# COMMAND ----------

# MAGIC %md
# MAGIC * Although this smalldictionary probably is not too large of a cost, if you have a much larger value, the cost of serializing the data for every task can be quite significant.
# MAGIC * We also use Broadcast variable in UDF or in a Dataset and achieve the same result.

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Accumlators:
# MAGIC * This is a way of updating a value inside of a variety of transformations and propagating that value to the driver node in an efficient and fault-tolerant way.
# MAGIC * Accumlators provide a mutable variable that a Spark cluster can safely update on a per-row basis.
# MAGIC * Spark natively supports accumulators of numeric types, and programmers can add support for new types.
# MAGIC * For accumlators updates performed inside **actions**  only, Spark ensures that each task’s update to the accumulator will be applied only once meaning the restarted tasks will not update the value. In **transformations** each task's update can be applied more than once if task or job stages are reexecuted.
# MAGIC * Accumlators are lazy executed.accumulator updates are not guaranteed to be executed when made within a lazy transformation like map().
# MAGIC * Accumulators can be both **named and unnamed**. Named accumulators will display their running results in the Spark UI, whereas unnamed ones will not.

# COMMAND ----------

#let’s create an accumulator that will count the number of flights to or from China.
flights = spark.read.parquet("/FileStore/tables/2010_summary-506d8.parquet")

# creating an accumlator
accChina = spark.sparkContext.accumulator(0)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count from flights 
# MAGIC where DEST_COUNTRY_NAME = "China" or ORIGIN_COUNTRY_NAME = "China"

# COMMAND ----------

#defining function to add to our accumlator
def accChinaFunc(flight_row):
  destination = flight_row["DEST_COUNTRY_NAME"]
  origin = flight_row["ORIGIN_COUNTRY_NAME"]
  
  if(destination == "China"):
    accChina.add(flight_row["count"])
  if(origin == "China"):
    accChina.add(flight_row["count"])

# COMMAND ----------

# MAGIC %md
# MAGIC * Iterating over every row using foreach method since it is an action.
# MAGIC * SPARK can provide guarantees that perform only inside of actions. The foreach method will run once for each row in the input DataFrame (assuming that we didnot filter it) and will run our function against each row, incrementing the accumulatoraccordingly

# COMMAND ----------

flights.foreach(lambda flight_row: accChinaFunc(flight_row))

# COMMAND ----------

accChina.value

# COMMAND ----------

# MAGIC %md
# MAGIC **Custom Accumlators:**
# MAGIC * sometimes we might want to build your own custom accumulators.
# MAGIC * To do this you need to subclass the AccumulatorV2 class.
# MAGIC * use this for more detais
# MAGIC https://spark.apache.org/docs/1.1.0/api/python/pyspark.accumulators.AccumulatorParam-class.html