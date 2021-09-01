# Databricks notebook source
# MAGIC %md
# MAGIC ###### Join Expressions:
# MAGIC * Join brings two datasets together by comparing by comparing the value of one or more keys of the left and right and evaluating the result of a join expression
# MAGIC * Join expression determines whether Spark should bring together the left set of data with the right set of data.
# MAGIC * an **equi-join expression**, compares whether the specified keys in your left and right datasets are equal.
# MAGIC * Spark also allows for much more sophsticated join policies in addition to equi-joins. We can even use **complex types and perform something like checking whether a key exists within an array when you perform a join.**

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Join Types:
# MAGIC join type determines what should be in the result set. There are a variety of different join types available in Spark
# MAGIC 
# MAGIC * Inner Joins - Keep rows with keys that exist in the left and right datasets
# MAGIC * Outter Joins - Keep rows with keys in either the left or right datasets
# MAGIC * Left Outer Joins - Keep rows with keys in the left dataset
# MAGIC * Right Outer Joins - Keep rows with keys in the right dataset
# MAGIC * Left Semi Join - Keep the rows in the left, and only the left, dataset where the key appears in the right dataset.
# MAGIC * Left anti Join - keep the rows in the left, and only the left, dataset where they do not appear in the right dataset.
# MAGIC * Natural joins - Perform a join by implicitly matching the columns between the two datasets with the same names.
# MAGIC * Cross (or Cartesian) joins - Match every row in the left dataset with every row in the right dataset.
# MAGIC 
# MAGIC let’s create some simple datasets to understand more with examples.

# COMMAND ----------

person = spark.createDataFrame([
        (0, "Bill Chambers", 0, [100]),
        (1, "Matei Zaharia", 1, [500, 250, 100]),
        (2, "Michael Armbrust", 1, [250, 100])])\
        .toDF("id", "name", "graduate_program", "spark_status")

graduateProgram = spark.createDataFrame([
          (0, "Masters", "School of Information", "UC Berkeley"),
          (2, "Masters", "EECS", "UC Berkeley"),
          (1, "Ph.D.", "EECS", "UC Berkeley")])\
          .toDF("id", "degree", "department", "school")

sparkStatus = spark.createDataFrame([
              (500, "Vice President"),
              (250, "PMC Member"),
              (100, "Contributor")])\
              .toDF("id", "status")

# COMMAND ----------

person.createOrReplaceTempView("person")
graduateProgram.createOrReplaceTempView("graduateProgram")
sparkStatus.createOrReplaceTempView("sparkStatus")

# COMMAND ----------

person.show()
graduateProgram.show()
sparkStatus.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Inner Joins:
# MAGIC Inner joins evaluate the keys in both of the DataFrames or tables and include (and join together) only the rows that evaluate to true.
# MAGIC 
# MAGIC Eg: we join the graduateProgram DataFrame with the person DataFrame to create a new DataFrame

# COMMAND ----------

joinExpression = person["graduate_program"] == graduateProgram["id"]

# COMMAND ----------

wrongJoinExpression = person["name"] == graduateProgram["school"]

# COMMAND ----------

# MAGIC %md
# MAGIC Inner joins are the default join, so we just need to specify our left DataFrame and join the right in the JOIN expression:

# COMMAND ----------

person.join(graduateProgram, joinExpression).show()

# COMMAND ----------

joinType = "inner"
person.join(graduateProgram, joinExpression, joinType).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM person INNER JOIN graduateProgram ON person.graduate_program = graduateProgram.id

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Outer Join:
# MAGIC * Outer joins evaluate the keys in both of the DataFrames or tables and includes (and joins together) the rows that evaluate to true or false. 
# MAGIC * If there is no equivalent row in either the left or right DataFrame, Spark will insert null

# COMMAND ----------

joinType = "outer"
person.join(graduateProgram, joinExpression, joinType).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Left Outer Joins:
# MAGIC * Left outer joins evaluate the keys in both of the DataFrames or tables and includes all rows from the left DataFrame as well as any rows in the right DataFrame that have a match in the left DataFrame.
# MAGIC * If there is no equivalent row in the right DataFrame, Spark will insert null

# COMMAND ----------

joinType = "left_outer"
graduateProgram.join(person, joinExpression, joinType).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Right Outer Joins:
# MAGIC * Right outer joins evaluate the keys in both of the DataFrames or tables and includes all rows from the right DataFrame as well as any rows in the left DataFrame that have a match in the right DataFrame.
# MAGIC * If there is no equivalent row in the left DataFrame, Spark will insert null

# COMMAND ----------

joinType = "right_outer"
person.join(graduateProgram, joinExpression, joinType).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Left Semi Join:
# MAGIC * This join actually include doesnt any values from the right DataFrame.
# MAGIC * They only compare to see if value exists in second dataframe.
# MAGIC * If the value does exist, those rows will be kept in the result, even if there are duplicate keys in the left DataFrame.

# COMMAND ----------

joinType = "left_semi"
graduateProgram.join(person, joinExpression, joinType).show()

# COMMAND ----------

#Adding duplicate row to check how the duplicate works

gradProgram2 = graduateProgram.union(spark.createDataFrame([
                      (0, "Masters", "Duplicated Row", "Duplicated School")]))

gradProgram2.createOrReplaceTempView("gradProgram2")
gradProgram2.join(person, joinExpression, joinType).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Left Anti Joins:
# MAGIC * This join is exactly opposite to Left Semi Join.
# MAGIC * This also compare values to see if the value exists in the second DataFrame. However, rather than keeping the values that exist in the second DataFrame, they keep only the values that do not have a corresponding key in the second DataFrame.
# MAGIC * Think of anti joins as a NOT IN SQL-style filter

# COMMAND ----------

joinType = "left_anti"
graduateProgram.join(person, joinExpression, joinType).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Cross (Cartesian) Joins:
# MAGIC * Cross joins will join every single row in the left DataFrame to ever single row in the right DataFrame.
# MAGIC * This will cause an absolute explosion in the number of rows contained in the resulting DataFrame.

# COMMAND ----------

joinType = "cross"
graduateProgram.join(person, joinExpression, joinType).show()

# COMMAND ----------

person.crossJoin(graduateProgram).show()

# COMMAND ----------

# MAGIC %md
# MAGIC * You should use cross-joins only if you are absolutely, 100 percent sure that this is the join you need.Reason being we need to be explicit when defining a cross-join in Spark.
# MAGIC 
# MAGIC * Advanced users can set the session-level configuration **spark.sql.crossJoin.enable** to true in order to allow cross-joins without warnings or without Spark trying to perform another join for you.

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Challenges When Using Joins:
# MAGIC **Joining Complex Types:**
# MAGIC * Any expression is a valid join expression, assuming that it returns a Boolean

# COMMAND ----------

from pyspark.sql.functions import expr

person.withColumnRenamed("id", "personId").join(sparkStatus, expr("array_contains(spark_status, id)")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Handling Duplicate Column Names:**
# MAGIC 
# MAGIC * In a DataFrame, each column has a unique ID within Spark’s SQL Engine,Catalyst. This unique ID is purely internal and not something that you can directly reference. 
# MAGIC * Duplicate column names occur in two distinct situations:
# MAGIC   * The Join expression that you specify does not remove one key from one of the input DataFrames and the keys have the same column name.
# MAGIC   * Two columns on which we are not performing the join have the same name.. 

# COMMAND ----------

person.show()
graduateProgram.show()

# COMMAND ----------

gradProgramDupe = graduateProgram.withColumnRenamed("id", "graduate_program")
joinExpr = gradProgramDupe["graduate_program"] == person["graduate_program"]

#joinExpression = person["graduate_program"] == graduateProgram["id"]

# COMMAND ----------

person.join(gradProgramDupe, joinExpr).show()

# COMMAND ----------

# MAGIC %md
# MAGIC The challenge arises when we refer to one of these columns

# COMMAND ----------

person.join(gradProgramDupe, joinExpr).select("graduate_program").show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Approach 1: Different join expression**
# MAGIC The easiest fix is to change the join expression from a Boolean expression to a string or sequence. This automatically removes one of
# MAGIC the columns for you during the join

# COMMAND ----------

person.join(gradProgramDupe,"graduate_program").show()

# COMMAND ----------

person.join(gradProgramDupe,"graduate_program").select("graduate_program").show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Approach 2: Dropping the column after the join**
# MAGIC * When doing this, we need to refer to the column via the original source DataFrame.
# MAGIC * We can do this if the join uses the same key names or if the source DataFrames have columns that simply have the same name

# COMMAND ----------

person.join(gradProgramDupe, joinExpr).drop(person["graduate_program"]).select("graduate_program").show()

# COMMAND ----------

# MAGIC %md
# MAGIC This is an artifact of Spark’s SQL analysis process in which an explicitly referenced column will pass analysis because Spark has no need to resolve the column. 

# COMMAND ----------

# MAGIC %md
# MAGIC **Approach 3: Renaming a column before the join:**
# MAGIC 
# MAGIC We can avoid this issue altogether if we rename one of our columns before the join

# COMMAND ----------

gradProgram3 = graduateProgram.withColumnRenamed("id", "grad_id")
joinExpr = person["graduate_program"] == gradProgram3["grad_id"]
person.join(gradProgram3, joinExpr).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### How Spark Performs Joins:
# MAGIC To understand how Spark performs joins, we need to understand the two core resources at play:
# MAGIC * node-to-node communication strategy
# MAGIC * per node computation stratergy
# MAGIC 
# MAGIC Spark approaches cluster communication in two different ways during joins. It either incurs a
# MAGIC * shuffle join, which results in an all-to-all communication. When you join a big table to another big table, you end up with a shuffle join. 
# MAGIC * a broadcast join
# MAGIC 
# MAGIC The core foundation of simplified view of joins is that in Spark we will have either a **big table** or a **small table.**

# COMMAND ----------

# MAGIC %md
# MAGIC **Big table to Big table:**
# MAGIC * when you join big table to big table, we end up in **Shuffle Join**
# MAGIC * In Shuffle join, every node talks to other node and they share data according to which node has a certain key or set of keys(on which you are joining).
# MAGIC * These joins are expensive because the network can be become congested with traffic, especially when the data is not partitioned well.
# MAGIC 
# MAGIC ![Big table to Bigtable](files/tables/Bigtable_to_bigtable-44cdf.PNG)

# COMMAND ----------

# MAGIC %md
# MAGIC **Big table–to–small table:**
# MAGIC * This uses Broadcast Join.
# MAGIC * When the table is small enough to fit into the memory of a single worker node, with some breathing room of course, we can optimize our join.
# MAGIC * replicate our small DataFrame onto every worker node in the cluster (be it located on one machine or many).
# MAGIC * Even it looks expensive initially, but this prevents us from performing the all-to-all communication during the entire join process.
# MAGIC * we perform it only once at the beginning and then let each individual worker node perform the work without having to wait or communicate with any other worker node.
# MAGIC * This means that joins will be performed on every single node individually, making CPU the biggest bottleneck.
# MAGIC 
# MAGIC ![Broad Cast Join](files/tables/BroadCast_Join-1b827.PNG)
# MAGIC 
# MAGIC For our current set of data, we can see that Spark joining type by looking at the explain plan:

# COMMAND ----------

joinExpr = person["graduate_program"] == graduateProgram["id"]
person.join(graduateProgram, joinExpr).explain()

# COMMAND ----------

# MAGIC %md
# MAGIC With DataFrame API, we can also explicitly give the optimizer a hint that we would like to use a broadcast join by using the correct function around the small DataFrame in question.

# COMMAND ----------

from pyspark.sql.functions import broadcast

joinExpr = person["graduate_program"] == graduateProgram["id"]
person.join(broadcast(graduateProgram), joinExpr).explain()

# COMMAND ----------

# MAGIC %md 
# MAGIC * The SQL interface also includes the ability to provide hints to perform joins. 
# MAGIC * These are not enforced, however, so the optimizer might choose to ignore them.
# MAGIC * We can set one of these hints by using a special comment syntax.
# MAGIC     * MAPJOIN
# MAGIC     * BROADCAST &
# MAGIC     * BROADCASTJOIN 
# MAGIC    
# MAGIC    all do the same thing and are all supported

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT /*+ MAPJOIN(graduateProgram) */ * FROM person JOIN graduateProgram
# MAGIC ON person.graduate_program = graduateProgram.id

# COMMAND ----------

# MAGIC %md
# MAGIC if you try to broadcast something too large, you can crash your driver node (because that collect is expensive).
# MAGIC 
# MAGIC **Little table–to–little table:**
# MAGIC When performing joins with small tables, it’s usually best to let Spark decide how to join them. You can always force a broadcast join if you’re noticing strange behavior.

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Conclusion:
# MAGIC It is important to consider is if you **partition** your data correctly prior to a join, you can end up with much more efficient execution because even if a shuffle is planned, if data from two different DataFrames is already located on the same machine, Spark can avoid the
# MAGIC shuffle.
# MAGIC 
# MAGIC There are additional implications when you decide what order joins should occur in. Because some joins act as filters, this can be a low-hanging improvement in your workloads, as you are guaranteed to reduce data exchanged over the network.