# Databricks notebook source
# MAGIC %md
# MAGIC ###### What Are the Low-Level APIs?
# MAGIC There are two types of low-level APIs
# MAGIC * Resilient Distributed Dataset - for manipulating distributed data
# MAGIC * broadcast variables and accumulators - for distributing and manipulating distributed shared variables.

# COMMAND ----------

# MAGIC %md
# MAGIC **When to Use the Low-Level APIs?**
# MAGIC 
# MAGIC * You need some functionality that you cannot find in the higher-level APIs 
# MAGIC   eg: if you need tight control over physical data placement across the cluster.
# MAGIC * You need to maintain some legacy code base written using RDDs.
# MAGIC * You need to do some custom shared variable manipulation.
# MAGIC 
# MAGIC When you’re calling a DataFrame transformation, it actually just becomes a set of RDD transformations.
# MAGIC * It is recommended to use Structured APIs, However some times we might want to “drop down” to some of the lower-level tools to complete your task.
# MAGIC * These tools give you more finegrained control at the expense of safeguarding you from shooting yourself in the foot.

# COMMAND ----------

# MAGIC %md
# MAGIC **How to Use the Low-Level APIs?**
# MAGIC * SparkContext is the entry point for **low-level API functionality.**
# MAGIC * we can access SC through **SparkSession,** which is the tool you use to perform computation across a Spark cluster.

# COMMAND ----------

spark.sparkContext

# COMMAND ----------

# MAGIC %md
# MAGIC **RDDs:**
# MAGIC * RDDs represent immutable, partitioned collection of records that can be operated in parallel.
# MAGIC * Records in RDD are java,scala or Python objects which gives a complete control.This also give potential issues when we try for optimizing 
# MAGIC * Every manipulation and interaction between values must be defined by hand. Spark’s Structured APIs automatically store data in an optimzied, compressed binary format, so to achieve the same space-efficiency and performance.
# MAGIC * RDDs are similar to Dataset, except that RDDs are not stored in, or manipulated with, the structured data engine.

# COMMAND ----------

# MAGIC %md
# MAGIC **Types of RDDs:** Two types 
# MAGIC 
# MAGIC * Generic RDDs which represent a collection of objects.
# MAGIC * key-value RDD that provides additional functions, such as 
# MAGIC     * aggregating by key.
# MAGIC     * partitioning by key.
# MAGIC * Internally, RDD is characterized by five main properties:
# MAGIC   * A list of Partitions.
# MAGIC   * A function for computing each split.
# MAGIC   * A list of dependencies on other RDDs.
# MAGIC   * Optionally, a Partitioner for key-value RDDs (e.g., to say that the RDD is hashpartitioned)
# MAGIC   * Optionally, a list of preferred locations on which to compute each split (e.g., block locations for a Hadoop Distributed File System [HDFS] file)
# MAGIC   
# MAGIC * They provide transformations, which evaluate lazily, and actions, which evaluate eagerly, to manipulate data in a distributed fashion.
# MAGIC * However, there is no concept of “rows” in RDDs;individual records are just raw Java/Scala/Python objects, and you manipulate those manually instead of tapping into the repository of functions that you have in the structured APIs.
# MAGIC * Running Python RDDs equates to running Python user-defined functions (UDFs) row by row.
# MAGIC * We serialize the data to the Python process, operate on it in Python, and then serialize it back to the Java Virtual Machine (JVM). This causes a high overhead for Python RDD manipulations.

# COMMAND ----------

# MAGIC %md
# MAGIC **what is the difference between RDDs of Case Classes and Datasets?**
# MAGIC * Datasets can take  advantage of the wealth of functions and optimizations that the Structured APIs have to offer.
# MAGIC * With Datasets, you do not need to choose between only operating on JVM types or on Spark types, you can choose whatever is either easiest to do or most flexible.

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Creating RDDs:
# MAGIC **Interoperating Between DataFrames, Datasets, and RDDs:**
# MAGIC 
# MAGIC * easiest ways to get RDDs is from an existing DataFrame or Dataset.
# MAGIC * conversion from a Dataset[T] to an RDD, we’ll get the appropriate native type T back in Scala and Java
# MAGIC * But in Python creating an RDD from DataFrame generates RDD of type ro

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.range(500).rdd

# COMMAND ----------

spark.range(10).rdd

# COMMAND ----------

# MAGIC %md
# MAGIC To operate on this data, you will need to convert this Row object to the correct data type or extract values out of it.

# COMMAND ----------

samplerdd = spark.range(10).toDF("id").rdd.map(lambda row: row[0])
samplerdd.take(3)

# COMMAND ----------

# MAGIC %md
# MAGIC We can create DF from rdd using toDF() function

# COMMAND ----------

spark.range(10).rdd.toDF()

# COMMAND ----------

# MAGIC %md
# MAGIC **From a Local Collection:**
# MAGIC * To create RDD from local collection, we need to use parallelize method on a SPARK context
# MAGIC * This turns single node collection in to parallel collection.
# MAGIC * We can also specify the number of partitions into which you would like to distribute this array
# MAGIC * we can name this RDD to show up in the SPARK UI according to the given name.

# COMMAND ----------

myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
words = spark.sparkContext.parallelize(myCollection,2)
words.setName("myWords")
words.name()

# COMMAND ----------

# MAGIC %md
# MAGIC **From Data Sources:**
# MAGIC * RDDs do not have a notion of “Data Source APIs” like DataFrames do.
# MAGIC * They primarily define their dependency structures and lists of partitions.Below code creates an RDD for which each record in the RDD represents a line in that text file or files.
# MAGIC   
# MAGIC       spark.sparkContext.textFile("/some/path/withTextFiles")
# MAGIC * if we want to read in data for which each text file should become a single record. Eg: each file is a file that consists of a large JSON object or some document that you will operate on as an individual
# MAGIC 
# MAGIC       spark.sparkContext.wholeTextFiles("/some/path/withTextFiles")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Transformations:
# MAGIC * We specify transformations on one RDD to create another. 
# MAGIC * In doing so, we define an RDD as a dependency to another along with some manipulation of the data contained in that RDD.
# MAGIC 
# MAGIC **distinct:**
# MAGIC * A distinct method call on an RDD removes duplicates from the RDD
# MAGIC 
# MAGIC **filter:**
# MAGIC * Filtering is equivalent to creating a SQL-like where clause.
# MAGIC * This function just needs to return a Boolean type to be used as a filter function.
# MAGIC 
# MAGIC **map:**
# MAGIC * specify a function that returns the value that you want, given the correct input.
# MAGIC * You then apply that, record by record.

# COMMAND ----------

words.distinct().count()

# COMMAND ----------

#writing the filter code 
def startsWithS(individual):
  return individual.startswith("S")

words.filter(lambda word: startsWithS(word)).collect()

# COMMAND ----------

#code for map function
words2 = words.map(lambda word: (word, word[0], word.startswith("S")))
words2.filter(lambda record: not record[2]).take(5)

# COMMAND ----------

# MAGIC %md
# MAGIC **FlatMap:**
# MAGIC * flatMap provides a simple extension of the map function.
# MAGIC * For example, you might want to take your set of words and flatMap it into a set of characters.

# COMMAND ----------

list = ['this','is a text','we','are splitting','using FlatMap','function']
words = spark.sparkContext.parallelize(list,2)
words.flatMap(lambda word: word.split(" ")).take(10)

# COMMAND ----------

# MAGIC %md
# MAGIC **Sort:**
# MAGIC * sorting can be done using **SortBy** method.
# MAGIC * We can do this by specifying a function to extract a value from the objects in your RDDs and then sort based on that.

# COMMAND ----------

words.sortBy(lambda word: len(word.split(" ")) * -1).take(5)

# COMMAND ----------

# MAGIC %md
# MAGIC **Random Splits:**
# MAGIC * We can also randomly split an RDD into an Array of RDDs by using the randomSplit method, which accepts an Array of weights and a random seed
# MAGIC * This returns an array of RDDs that you can manipulate individually.
# MAGIC 
# MAGIC       fiftyFiftySplit = words.randomSplit([0.5, 0.5])

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Actions:
# MAGIC * we specify actions to kick off our specified transformations. 
# MAGIC * Actions either collect data to the driver or write to an external data source.
# MAGIC 
# MAGIC **reduce:**
# MAGIC * We can use reduce method to specify a function to "reduce" an RDD of any kind of value to one value.

# COMMAND ----------

spark.sparkContext.parallelize(range(1,21)).reduce(lambda x,y: x + y)

# COMMAND ----------

#the same logic can be used to find the longest word of the text in words
myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
words = spark.sparkContext.parallelize(myCollection,2)

def wordLengthReducer(leftWord,rightWord):
  if(len(leftWord) > len(rightWord)):
    return leftWord
  else:
    return rightWord
  
words.reduce(wordLengthReducer)

# COMMAND ----------

list = ['this','is a text','we','are splitting','using FlatMap','function']
newRDD = spark.sparkContext.parallelize(list,6)

newRDD.flatMap(lambda word: word.split(" ")).reduce(wordLengthReducer)

# COMMAND ----------

# MAGIC %md
# MAGIC **Count:**
# MAGIC 
# MAGIC count the number of rows in the RDD
# MAGIC 
# MAGIC     words.count()
# MAGIC   
# MAGIC **countApprox:**
# MAGIC * confidence is the probability that the error bounds of the result will contain the true value.
# MAGIC   
# MAGIC       val confidence = 0.95
# MAGIC       val timeoutMilliseconds = 400
# MAGIC       words.countApprox(timeoutMilliseconds, confidence)
# MAGIC       
# MAGIC **countByValue:**
# MAGIC * counts the number of values in a given RDD by loading the result set into the memory of the driver.
# MAGIC * use this method only if the resulting map is expected to be small because the entire thing is loaded into the driver’s memory.
# MAGIC   
# MAGIC       words.countByValue()
# MAGIC 
# MAGIC **countByValueApprox:**
# MAGIC * similar to countByValue but with some approximation.
# MAGIC * This must execute within the specified timeout (first parameter) (and can return incomplete results if it exceeds the timeout).
# MAGIC 
# MAGIC       words.countByValueApprox(1000, 0.95)
# MAGIC       
# MAGIC **first:**
# MAGIC * The first method returns the first value in the dataset:
# MAGIC 
# MAGIC       words.first()
# MAGIC       
# MAGIC **max and min:**
# MAGIC * max and min return the maximum values and minimum values, respectively:
# MAGIC 
# MAGIC       spark.sparkContext.parallelize(1 to 20).max()
# MAGIC       spark.sparkContext.parallelize(1 to 20).min()

# COMMAND ----------

# MAGIC %md
# MAGIC **take:**
# MAGIC * This works by first scanning one partition and then using the results from that partition to estimate the number of additional partitions needed to satisfy the limit.
# MAGIC * many variations on this function
# MAGIC   * takeOrdered -
# MAGIC   * takeSample - specify a fixed-size random sample from your RDD. (using withReplacement, the number of values, as well as the random seed.) 
# MAGIC   * top - opposite of takeOrdered in that it selects the top values according to the implicit ordering

# COMMAND ----------

words.take(5)
words.takeOrdered(5)
words.top(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Saving Files:
# MAGIC * Saving files means writing to plain-text files.
# MAGIC 
# MAGIC       #saveAsTextFile
# MAGIC       words.saveAsTextFile("file:/tmp/bookTitle")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Caching:
# MAGIC 
# MAGIC * We can either Cache or Persist an RDD. Both handle data in memory.
# MAGIC * We can specify a storage level as any of the storage levels in the singleton object
# MAGIC * org.apache.spark.storage.StorageLevel, which are combinations of
# MAGIC     * memory only
# MAGIC     * disk only and
# MAGIC     * separately, off heap.
# MAGIC     
# MAGIC         
# MAGIC           words.getStorageLevel()

# COMMAND ----------

words.getStorageLevel()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### CheckPointing:
# MAGIC * It is the act of saving RDD to disk so that future references to this RDD point to those intermediate partitions on disk rather than recomputing the RDD from its original source.
# MAGIC * This feature is not available in  DataFrame API.
# MAGIC * This can be helpful in iterative computation, similar to the use case in Caching.
# MAGIC 
# MAGIC       spark.sparkContext.setCheckpointDir("/some/path/for/checkpointing")
# MAGIC       words.checkPoint()
# MAGIC       
# MAGIC * when we reference this RDD, it will derive from the checkpoint instead of the source data. This can be a helpful optimization

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Pipe RDDs to System Commands:
# MAGIC * With pipe, we can return an RDD created by piping elements to a forked external process.
# MAGIC * The resulting RDD is computed by executing the given process once per partition.
# MAGIC * All elements of each input partition are written to a process’s stdin as lines of input separated by a newline.
# MAGIC * The resulting partition consists of the process’s stdout output, with each line of stdout resulting in one element of the output partition.
# MAGIC * A process is invoked even for empty partitions.

# COMMAND ----------

words.pipe("wc -l").collect()

# COMMAND ----------

newRDD.pipe("wc -l").collect()

# COMMAND ----------

newRDD.take(5)

# COMMAND ----------

# MAGIC %md
# MAGIC **mapPartitions:**
# MAGIC 
# MAGIC * Spark operates on a per-partition basis when it comes to actually executing code.
# MAGIC * The return signature of a map function on an RDD is actually **MapPartitionsRDD.** This is because map is just a row-wise alias for mapPartitions, which makes it possible for you to map an individual partition (represented as an iterator)
# MAGIC * That’s because physically on the cluster we operate on each partition individually (and not a specific row).
# MAGIC * we operate on a per-partition basis and allows us to perform an operation on that entire partition.
# MAGIC * We can gather all values of a partition class or group into one partition and then operate on that entire group using arbitrary functions and controls.

# COMMAND ----------

words.mapPartitions(lambda part: [1]).sum()
testWord = ['testing','new','words']
defRDD = spark.sparkContext.parallelize(testWord,5)
defRDD.mapPartitions(lambda part:[2]).sum()

# COMMAND ----------

# MAGIC %md
# MAGIC **mapPartitionsWithIndex:**
# MAGIC * with this we specify a function that accepts an index(within the partition) and an iterator that goes through all items within the partition.
# MAGIC * PartitionIndex is the partition number in yur RDD, which identifies where each record in our dataset sits(and potentially allows you to debug).
# MAGIC * we can use this to test whether your map functions are behaving correctly.

# COMMAND ----------

def indexedFunc(partitionIndex,withinPartIterator):
  return ["partition: {} => {}".format(partitionIndex, x) for x in withinPartIterator]

words.mapPartitionsWithIndex(indexedFunc).collect()

# COMMAND ----------

newRDD.mapPartitionsWithIndex(indexedFunc).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC **foreachPartition:**
# MAGIC 
# MAGIC * mapPartitions needs a return value to work properly, However foreachPartition simply iterates over all the partitions of the data.
# MAGIC * The difference is that the function has no return value.
# MAGIC * This makes it great for doing something with each partition like writing it out to a database. This is how many data source connectors are written.

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
# MAGIC val words = spark.sparkContext.parallelize(myCollection, 2)
# MAGIC 
# MAGIC words.foreachPartition { 
# MAGIC   iter =>
# MAGIC   import java.io._
# MAGIC   import scala.util.Random
# MAGIC   val randomFileName = new Random().nextInt()
# MAGIC   val pw = new PrintWriter(new File(s"/tmp/random-file-${randomFileName}.txt"))
# MAGIC   while (iter.hasNext) {
# MAGIC     pw.write(iter.next())
# MAGIC     }
# MAGIC   pw.close()
# MAGIC   }

# COMMAND ----------

# MAGIC %md
# MAGIC **gloom:**
# MAGIC * It takes every partition in your dataset and converts them to arrays.
# MAGIC * It is useful if we want to collect data to the driver and want to have an array for each partition.
# MAGIC * However if we have a large partitions in size or large number of partitions, it may crash the driver.

# COMMAND ----------

newRDD.glom().collect()