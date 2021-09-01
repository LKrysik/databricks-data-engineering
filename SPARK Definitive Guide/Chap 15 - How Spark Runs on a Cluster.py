# Databricks notebook source
# MAGIC %md
# MAGIC This chapter covers several key topics:
# MAGIC * The architecture and components of a SPARK application.
# MAGIC * The Life cycle of a SPARK application inside and outside of SPARK.
# MAGIC * Important low-level execution properties, such as pipeling.

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Architecture of SPARK application :
# MAGIC **SPARK driver:**
# MAGIC * This process is the controller of execution of a SPARK app and maintains all of the state of the SPARK cluster(the state and task of the executors).
# MAGIC * It interacts with cluster manager to get physical resources and launch executors.
# MAGIC 
# MAGIC **SPARK executor:**
# MAGIC * They are the one that performs the tasks assigned by Spark driver.
# MAGIC * Take the tasks assigned by the driver, run them, and report back their state (success or failure) and results. 
# MAGIC * Each Spark Application has its own separate executor processes.
# MAGIC 
# MAGIC **Cluster Manager:**
# MAGIC * The cluster manager is responsible for maintaining a cluster of machines that will run your Spark Application(s).
# MAGIC * A cluster will have its own "driver"(sometimes called as master) and "worker" abstractions.
# MAGIC * The core difference is that these are tied to physical machines rather than processes (as they are in Spark).
# MAGIC * The cluster manager will be responsible for managing the underlying machines that our application is running on.

# COMMAND ----------

# MAGIC %md
# MAGIC As per SPARK 2.4.4, Spark currently supports three cluster managers: 
# MAGIC * Standalone - A simple cluster manager included with SPARK that makes it easy to setup a cluster.
# MAGIC * Apache Mesos - A general cluster manager that can also run Hadoop MapReduce and service applications.
# MAGIC * Hadoop YARN  - the resource manager in Hadoop 2
# MAGIC * Kubernetes - an open source system for automating deployment, scaling and management of containerized applications.

# COMMAND ----------

# MAGIC %md
# MAGIC **Execution Modes:** Execution Mode gives us the idea on where the aforementioned resources are physically located when we run SPARK application.
# MAGIC 
# MAGIC * **Cluster Mode: **
# MAGIC   * This is the most common way of running SPARK production application.
# MAGIC   * Once the job is submitted to Cluster Manager, It will launch the driver process on a worker node inside the cluster, in addition to executor process.
# MAGIC   * Cluster Manager(which is responsible for driver process) will place driver on a worker node and executors on other worker nodes.
# MAGIC   
# MAGIC * **Client Mode:**
# MAGIC   * It is almost similar to cluster mode, but driver remains on the client machine that submitted the application.
# MAGIC   * This means the client machine is responsible for maintaining the SPARK driver process and cluster manager maintains the executor process.
# MAGIC   * SPARK application is running from a machine that is not colocated on the cluster. These machines are called **gateway machines or Edge Nodes.**
# MAGIC   
# MAGIC * **Local Mode:**
# MAGIC   * It runs the entire SPARK application on a single machine. It achives parallelism through threads on that single machine.
# MAGIC   * This mode is used only to learn SPARK, to test application or experiment iteratively with local development.
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC ###### The Life Cycle of a Spark Application (Outside Spark):
# MAGIC 
# MAGIC **Client Request:**
# MAGIC * Cluster manager accepts the job submit request and places the driver onto a node in the cluster.
# MAGIC 
# MAGIC **Launch:**
# MAGIC * Once the driver has been placed on the cluster, it begins running the user code.
# MAGIC * The SparkSession will subsequently communicate with the cluster manager, asking it to launch Spark executor processes across the cluster.
# MAGIC * The number of executors and their relevant configurations are set by the user via the command-line arguments in the original spark-submit call.
# MAGIC 
# MAGIC **Execution:**
# MAGIC * The driver and the workers communicate among themselves, executing code and moving data around. 
# MAGIC * The driver schedules tasks onto each worker, and each worker responds with the status of those tasks and success or failure.
# MAGIC 
# MAGIC **Completion:**
# MAGIC * After a Spark Application completes, the driver processs exits with either success or failure.
# MAGIC * The cluster manager then shuts down the executors in that Spark cluster for the driver.

# COMMAND ----------

# MAGIC %md
# MAGIC ###### The Life Cycle of a Spark Application (Inside Spark):
# MAGIC 
# MAGIC * Each application is made up of one or more SPARK jobs.
# MAGIC * SPARK jobs within an application executed serially (unless you use to launch multiple actions in parallel).
# MAGIC 
# MAGIC **SparkSession:**
# MAGIC * The first step of any Spark Application is creating a SparkSession.
# MAGIC * From the SparkSession, We can access all of low-level and legacy contexts and configurations accordingly, as well.
# MAGIC * SparkSession class was only added in Spark 2.X.

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("Word Count")\
.config("spark.some.config.option", "some-value")\
.getOrCreate()

spark

# COMMAND ----------

# MAGIC %md
# MAGIC **Spark Context:**
# MAGIC * A SparkContext object within SparkSession represents the connection to the SPARK cluster.
# MAGIC * Through a SparkContext, we can create RDDs, accumulators, and broadcast variables, and we can run code on the cluster.
# MAGIC 
# MAGIC 
# MAGIC Spark 1.X had effectively two contexts. The SparkContext and the SQLContext. These two each performed different things. The former focused on more fine-grained control of Spark’s central abstractions, whereas the latter focused on the higher-level tools like Spark SQL.
# MAGIC 
# MAGIC Spark 2.X, the communtiy combined the two APIs into the centralized SparkSession. However, both of these APIs still exist and you can access them via the SparkSession. It is important to note that you should never need to use the SQLContext and rarely need to use the SparkContext.

# COMMAND ----------

from pyspark.sql import SQLContext


# COMMAND ----------

# MAGIC %md
# MAGIC **Logical Instructions:**
# MAGIC Understanding how we take declarative instructions like DataFrames and convert them into physical execution plans is an important step
# MAGIC to understanding how Spark runs on a cluster.
# MAGIC 
# MAGIC **Logical instructions to physical execution:**
# MAGIC Take a dataframe 
# MAGIC   * repartition it
# MAGIC   * perform a value by value manipulation
# MAGIC   * aggregate some values and collect the final result

# COMMAND ----------

spark.

# COMMAND ----------

df1 = spark.range(2,10000000, 2)
df2 = spark.range(2,10000000, 4)
step1 = df1.repartition(5)
step12 = df2.repartition(6)
step2 = step1.selectExpr("id * 5 as id")
step3 = step2.join(step12,["id"])
step4 = step3.selectExpr("sum(id)")

step4.collect()


# COMMAND ----------

step4.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC **A SPARK Job:**
# MAGIC * Generally there is one SPARK job for one action. 
# MAGIC * Each Job breaks down into series of stages, the number of which depends on how many shuffle operations need to take place.
# MAGIC 
# MAGIC ** Stages:**
# MAGIC * Stages represent groups of **tasks** that can be executed togethe to compute the same operation on multiple machines.
# MAGIC * A shuffle represents a physical repartitioning of the data. SPARK starts new stages after every **shuffle** operation.
# MAGIC   * Eg shuffle operation: sorting of DataFrame, grouping data that was loaded from a file by key
# MAGIC * In the job we have looked at
# MAGIC   * stage 1 and 2 created for range. By default creating a dataframe with range has 8 partitions.
# MAGIC   * repartition will change the number of partitions by shuffling the data. This creates Stage 3 
# MAGIC   * Join will be performed in one more stage. It has 200 tasks reason being **spark.sql.shuffle.partitions** default value is 200. We can change this value by spark.conf.set("spark.sql.shuffle.partitions", 50)
# MAGIC   * The final result aggregates those partitions individually, brings them all to a single partition before finally sending the final result to the driver.
# MAGIC   
# MAGIC   **A good rule of thumb is that the number of partitions should be larger than the number of executors on your cluster**
# MAGIC   
# MAGIC **Tasks:**
# MAGIC * Each task corresponds to a combination of blocks of data and a set of transformations that will run on a single executor.
# MAGIC * If we have only one partition, we will have only one task. 1000 little partion --> 1000 tasks executed in parallel.
# MAGIC * Partition your data into greater number of partitions means that more can be executed in parallel.

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Execution Details:
# MAGIC 
# MAGIC * SPARK automatically pipelines stages and tasks that can be done together, such as map operation followed by another map operation.
# MAGIC * For all shuffle operations, Spark writes the data to stable storage(e.g., disk) and can reuse it across multiple jobs.
# MAGIC 
# MAGIC 
# MAGIC **Pipelining:**
# MAGIC * pipelining occurs at or below the RDD level.
# MAGIC * with pipeling, any sequence of operations that feed data directly into each other, without needing to move it across nodes, is collapsed into single stage of tasks that do all the operations together.
# MAGIC     Eg: RDD having map -->filter --> map. this results in single stage of tasks that do all operations together.
# MAGIC * This pipelined version of the computation is much faster than writing the intermediate results to memory or disk after each step.
# MAGIC 
# MAGIC 
# MAGIC **Shuffle Persistence:**
# MAGIC * SPARK always executes shuffles by first having the "source" tasks(those sendig data) write shuffle files to their local disks during their execution stage.
# MAGIC * Then the stage that does the grouping and reduction launches and runs tasks that fetch their corresponding records from each shuffle file and performs that computation.
# MAGIC * Saving the shuffle files to disk lets Spark run this stage later in time than the source stage (e.g., if there are not enough executors to run both at the same time), and also lets the engine re-launch reduce tasks on failure without rerunning all the input tasks.
# MAGIC * One side effect with this is that running a new job over data that’s already been shuffled does not rerun the “source” side of the shuffle.
# MAGIC * In the Spark UI and logs, we will see the preshuffle stages marked as “skipped”. This automatic optimization can save time in a workload that runs multiple jobs over the same data.

# COMMAND ----------

