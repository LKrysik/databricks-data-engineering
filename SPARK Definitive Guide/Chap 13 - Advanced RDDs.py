# Databricks notebook source
# MAGIC %md
# MAGIC This chapter covers 
# MAGIC * Key-Value RDDs - a powerful abstraction for manipulating data.
# MAGIC * custom Partitioning - we can control exactly how data is laid out on the cluster and manipulate that individual partition accordingly.
# MAGIC * RDD joins

# COMMAND ----------

mycollection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
words = spark.sparkContext.parallelize(mycollection,2)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Key-Value Basics (Key-Value RDDs):
# MAGIC * the method will include <some-operation>ByKey. If we see **ByKey** in a method name, it means that you can perform this only on a PairRDD type.
# MAGIC * easiest way to create key-Value RDD is to map over the RDD and create a key to the value

# COMMAND ----------

words.map(lambda word: (word.lower(), 1)).take(2)

# COMMAND ----------

# MAGIC %md
# MAGIC **keyBy:**
# MAGIC * This function is used to key the values. Below example we are keying first letter for each word

# COMMAND ----------

keyword = words.keyBy(lambda word: word.lower()[0])
keyword.take(3)

# COMMAND ----------

# MAGIC %md
# MAGIC **Mapping over Values:**
# MAGIC * If we have a tuple, Spark will assume that the first element is the key, and the second is the value. 
# MAGIC * When in this format, you can explicitly choose to map-over the values (and ignore the individual keys).

# COMMAND ----------

keyword.mapValues(lambda word: word.upper()).take(2)

# COMMAND ----------

keyword.flatMapValues(lambda word: word.upper()).take(4)

# COMMAND ----------

# MAGIC %md
# MAGIC **Extracting Keys and Values:**
# MAGIC * key–value pair format 
# MAGIC 
# MAGIC **look-up:** we might want to do with an RDD is look up the result for a particular key.

# COMMAND ----------

keyword.keys().collect()
keyword.values().collect()

# COMMAND ----------

keyword.lookup("s")

# COMMAND ----------

# MAGIC %md
# MAGIC **sampleByKey:**
# MAGIC Two ways to sample an RDD by a set of keys.
# MAGIC * approximation
# MAGIC * exactly
# MAGIC 
# MAGIC * Both operations can do so with or without replacement as well as sampling by a fraction by a given key.
# MAGIC * This is done via simple random sampling with one pass over the RDD, which produces a sample of size that’s approximately equal to the sum of math.ceil(numItems * samplingRate) over all key values:

# COMMAND ----------

import random
distinctChars = words.flatMap(lambda word: list(word.lower())).distinct().collect()
sampleMap = dict(map(lambda c:(c, random.random()), distinctChars))
words.map(lambda word: (word.lower()[0], word)).sampleByKey(True,sampleMap,6).take(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Aggregations:
# MAGIC * can perform aggregations on plain RDDs or on PairRDDs, depending on the method
# MAGIC 
# MAGIC **CountByKey:**
# MAGIC * can count the number of elements for each key, collecting the results to a local Map.

# COMMAND ----------

chars = words.flatMap(lambda word: word.lower())
KVcharacters = chars.map(lambda letter:(letter,1))
def maxFunc(left,right):
  return max(left,right)

def addFunc(left,right):
  return left+right

nums = sc.parallelize(range(1,31),5)

# COMMAND ----------

KVcharacters.countByKey()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Understanding Aggregation Implementations:
# MAGIC 
# MAGIC **groupByKey:**
# MAGIC * groupByKey with a map over each grouping is mostly a wrong way to approach a problem. 
# MAGIC * Because each executor must hold all values for a given key in memory before appying the function. In this case we may have massive key skew, some partitions might be completely overloaded with ton of values for a given key and we might key OutOfMemory issues.
# MAGIC * If you have consistent value sizes for each key and know that they will fit in the memory of a given executor it is good to use groupByKey.
# MAGIC 
# MAGIC **reduceByKey:**
# MAGIC * This implementation is much more stable because the reduce happens within each partition and doesn’t need to put everything in memory.
# MAGIC * there is no incurred shuffle during this operation; everything happens at each worker individually before performing a final reduce.
# MAGIC * This greatly enhances the speed at which you can perform the operation as well as the stability of the operation.
# MAGIC * reduceByKey method returns an RDD of a group(the key) and sequence of elements that are not ordered. This workload is appropriate when our workload is assosciative and order doesn't matter.

# COMMAND ----------

from functools import reduce
KVcharacters.groupByKey().map(lambda row: (row[0], reduce(addFunc,row[1]))).take(2)

# COMMAND ----------

KVcharacters.reduceByKey(addFunc).take(2)

# COMMAND ----------

# MAGIC %md
# MAGIC **Aggregate:**
# MAGIC * This function requires a null and start value and then requires you to specify two different functions.
# MAGIC * The first aggregates within partitions, the second aggregates across partitions.
# MAGIC * The start value will be used at both aggregation levels.
# MAGIC * This performs final aggregation on the driver.
# MAGIC 
# MAGIC **treeAggregate:**
# MAGIC * It behaves similar to aggregate function, but basically “pushes down” some of the subaggregations (creating a tree from executor to executor) before performing the final aggregation on the driver.
# MAGIC * Having multiple levels can help you to ensure that the driver does not run out of memory in the process of the aggregation.

# COMMAND ----------

nums.aggregate(0,maxFunc, addFunc)

# COMMAND ----------

depth = 3
nums.treeAggregate(0,maxFunc,addFunc,depth)

# COMMAND ----------

# MAGIC %md
# MAGIC **aggregateByKey:**
# MAGIC * This function does the same as aggregate but instead of doing it partition by partition, it does it by key.
# MAGIC 
# MAGIC **combineByKey:**
# MAGIC * combiner operates on a given key and merges the values according to some function.
# MAGIC * It then goes to merge the different outputs of the combiners to give us our result. 
# MAGIC * We can specify the number of output partitions as a custom output partitioner as well

# COMMAND ----------

KVcharacters.aggregateByKey(0, addFunc, maxFunc).take(3)

# COMMAND ----------

def valToCombiner(value):
  return [value]

def mergeValuesFunc(vals,valToAppend):
  vals.append(valToAppend)
  return vals

def mergeCombinerFunc(vals1,vals2):
  return vals1 + vals2

outputPartitions = 6
KVcharacters.combineByKey(\
                         valToCombiner,mergeValuesFunc,mergeCombinerFunc,outputPartitions).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC **foldByKey:**
# MAGIC * foldByKey merges the values for each key using an associative function and a neutral “zero value,” which can be added to the result an arbitrary number of times, and must not change the result (e.g., 0 for addition, or 1 for multiplication)

# COMMAND ----------

KVcharacters.foldByKey(0,addFunc).take(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### CoGroups:
# MAGIC * CoGroups give you the ability to group together up to three key–value RDDs together in Scala and two in Python.
# MAGIC * The result is a group with our key on one side, and all of the relevant values on the other side.

# COMMAND ----------

distinctChars = words.flatMap(lambda word: word.lower()).distinct()
charRDD = distinctChars.map(lambda c: (c,random.random()))
charRDD2 = distinctChars.map(lambda c: (c, random.random()))
charRDD.cogroup(charRDD2).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Joins:
# MAGIC * all joins valid in Structured API are valid in RDD as well.
# MAGIC 
# MAGIC **zips:**
# MAGIC * zip allows you to “zip” together two RDDs, assuming that they have the same length. This creates a **PairRDD.**
# MAGIC * The two RDDs must have the **same number of partitions** as well as the **same number of elements.**

# COMMAND ----------

keyedChars = distinctChars.map(lambda c: (c, random.random()))
outputPartitions = 10
KVcharacters.join(keyedChars).count()
KVcharacters.join(keyedChars, outputPartitions).count()

# COMMAND ----------

numRange = sc.parallelize(range(10),2)
words.zip(numRange).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Controlling Partitions:
# MAGIC 
# MAGIC * With RDDs, you have control over how data is exactly physically distributed across the cluster.
# MAGIC * RDDs has a specific functionality **Custom Partitioner** to specify a partitioning function.
# MAGIC 
# MAGIC **Coalesce:**
# MAGIC * coalesce effectively collapses partitions on the **same worker** in order to avoid a shuffle of the data when repartitioning.
# MAGIC 
# MAGIC **Repartition:**
# MAGIC * The repartition operation allows you to repartition your data up or down but performs a shuffle across nodes in the process.
# MAGIC * Increasing the number of partitions can increase the level of parallelism when operating in map- and filter-type operations.
# MAGIC 
# MAGIC **repartitionAndSortWithinPartitions:**
# MAGIC * This operation gives you the ability to repartition as well as specify the ordering of each one of those output partitions.

# COMMAND ----------

words.getNumPartitions()

# COMMAND ----------

words.coalesce(1).getNumPartitions()

# COMMAND ----------

words.repartition(10)

# COMMAND ----------

# MAGIC %md
# MAGIC **Custom Partitioning:**
# MAGIC * This is the one of the main reasons to use RDD over structured API since this functionality is not available in DF/DS.
# MAGIC * The canonical example to motivate custom partition for this operation is PageRank whereby we seek to control the layout of the data on the cluster and avoid shuffles(partitioning by each customer ID).
# MAGIC * main goal of CP is to even out the distribution of your data across the cluster so that we can work around the problems such as **Data Skew.**
# MAGIC * To use CP we should convert Structured APIs to RDD apply custom partitioner and then convert it back to a DataFrame and Dataset.
# MAGIC * if we’re just looking to partition on a value or even a set of values (columns), it’s worth doing it in the DataFrame API.
# MAGIC 
# MAGIC 
# MAGIC * SPARK has two built in partitioners using RDD
# MAGIC   * HashPartitioner - for discrete values
# MAGIC   * RangePartitioner - for continous values

# COMMAND ----------

df = spark.read\
      .option("header","true")\
      .option("inferSchema","true")\
      .csv("/FileStore/tables/retail-data/by-day/*.csv")

myRDD = df.coalesce(10).rdd
df.printSchema()

# COMMAND ----------

def partitionFunc(key):
  import random
  if key == 17850 or key == 12583:
    return 0
  else:
    return random.randint(1,2)

keyedRDD = myRDD.keyBy(lambda row: row[6])
keyedRDD.partitionBy(3,partitionFunc)\
        .map(lambda x:x[0])\
        .glom()\
        .map(lambda x: len(set(x)))\
        .take(5)

# COMMAND ----------

# MAGIC %md
# MAGIC **Custom Serialization(Kryo serialization):**
# MAGIC 
# MAGIC * Any object that we want to parallelize must be serializable. The default serialization can be quite slow.
# MAGIC * Kryo is significantly faster and more compact than Java serialization (often as much as 10x), but does not support all serializable types and requires you to register the classes.
# MAGIC * use Kryo by initializing your job with a SparkConf and setting the value of "spark.serializer" to "org.apache.spark.serializer.KryoSerializer".
# MAGIC * This setting configures the serializer used for shuffling data between worker nodes and serializing RDDs to disk.