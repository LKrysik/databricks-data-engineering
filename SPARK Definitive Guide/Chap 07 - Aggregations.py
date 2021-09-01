# Databricks notebook source
# MAGIC %md
# MAGIC * **Aggregation** function must produce one result for each group, given multiple input values.
# MAGIC * Spark also allows us to create the following groupings types
# MAGIC   * The **simplest grouping** is to just summarize a complete DataFrame by performing an aggregation in a select statement.
# MAGIC   * A **group by** allows you to specify one or more keys as well as one or more aggregation functions to transform the value columns.
# MAGIC   * A **window** gives you the ability to specify one or more keys as well as one or more aggregation functions to transform the value columns. However, the rows input to the function are somehow related to the current row.
# MAGIC   * A **grouping set,** which you can use to aggregate at multiple different levels. Grouping sets are available as a primitive in SQL and via rollups and cubes in DataFrames.
# MAGIC   * A **rollup** makes it possible for you to specify one or more keys as well as one or more aggregation functions to transform the value columns, which will be summarized hierarchically.
# MAGIC   * A **cube** allows you to specify one or more keys as well as one or more aggregation functions to transform the value columns, which will be summarized across all combinations of columns.
# MAGIC 
# MAGIC * Each grouping returns a RelationalGroupedDataset on which we specify our aggregations.

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC Reading in our data on purchases, repartitioning the data to have far fewer partitions (because we know it’s a small volume of data stored in a lot of small files), and caching the results for rapid access

# COMMAND ----------

df = spark.read.format("csv")\
.option("header","true")\
.option("inferSchema","true")\
.load("/FileStore/tables/retail-data/by-day/*.csv")\
.coalesce(5)

df.cache()
df.createOrReplaceTempView("dfTable")

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Count
# MAGIC * specify a specific column to count, or 
# MAGIC * all the columns by using count(*) or count(1) to represent that we want to count every row as the literal one

# COMMAND ----------

from pyspark.sql.functions import count
df.select(count("StockCode")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Note:**
# MAGIC 
# MAGIC when performing a count(*), Spark will count null values (including rows containing all nulls). 
# MAGIC However, when counting an individual column, Spark will not count the null values.

# COMMAND ----------

# MAGIC %md
# MAGIC **countDistinct:** To get the number of unique groups

# COMMAND ----------

from pyspark.sql.functions import countDistinct

df.select(countDistinct("StockCode")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **approx_count_distinct:**
# MAGIC 
# MAGIC * use this function when an approximation to a certain degree of accuracy will work just fine
# MAGIC * Another parameter with which you can specify the maximum estimation error allowed.
# MAGIC * We can see much performance with larger datasets. specifying the large error makes an answer that is quite far off but does complete more quickly than countDistinct

# COMMAND ----------

from pyspark.sql.functions import approx_count_distinct
df.select(approx_count_distinct("StockCode",0.1)).show()

# COMMAND ----------

df.select(approx_count_distinct("StockCode")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **first and last:**
# MAGIC 
# MAGIC * Used to get the first and last values from a DataFrame.
# MAGIC * This will be based on the rows in the DataFrame, not on the values in the DataFrame.

# COMMAND ----------

from pyspark.sql.functions import first,last

df.select(first("StockCode"),last("StockCode")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **min and max:**
# MAGIC 
# MAGIC use min and max to extract the minimum and maximum values from a DataFrame

# COMMAND ----------

from pyspark.sql.functions import min,max

df.select(min("Quantity"),max("Quantity")).show()

# COMMAND ----------

df.select(min("StockCode"),max("StockCode")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **SUM:**
# MAGIC 
# MAGIC add all the values in a row using the sum function

# COMMAND ----------

from pyspark.sql.functions import sum

df.select(sum("Quantity")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **sumDistinct:**
# MAGIC By using this function, we can sum a distinct set of values

# COMMAND ----------

from pyspark.sql.functions import sumDistinct

df.select(sumDistinct("Quantity")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **avg:**
# MAGIC Although you can calculate average by dividing sum by count, Spark provides an easier way to get that value via the avg or mean functions.

# COMMAND ----------

from pyspark.sql.functions import avg, expr

df.select(
         count("Quantity").alias("total_transactions"),
         sum("Quantity").alias("total_purchases"),
         avg("Quantity").alias("avg_purchases"),
         expr("mean(Quantity)").alias("mean_purchases"))\
       .selectExpr(
                  "total_purchases/total_transactions",
                  "avg_purchases",
                  "mean_purchases").show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Variance and Standard Deviation:**
# MAGIC 
# MAGIC Spark has both the formula for the sample standard deviation as well as the formula for the population standard deviation.

# COMMAND ----------

from pyspark.sql.functions import var_pop, stddev_pop
from pyspark.sql.functions import var_samp, stddev_samp

df.select(var_pop("Quantity"), var_samp("Quantity"),stddev_pop("Quantity"), stddev_samp("Quantity")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **skewness and kurtosis:**
# MAGIC 
# MAGIC * Skewness measures the asymmetry of the values in your data around the mean.
# MAGIC * kurtosis is a measure of the tail of data.

# COMMAND ----------

from pyspark.sql.functions import skewness, kurtosis

df.select(skewness("Quantity"), kurtosis("Quantity")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Covariance and Correlation:**
# MAGIC 
# MAGIC * Correlation measures the Pearson correlation coefficient, which is scaled between –1 and +1.
# MAGIC * The covariance is scaled according to the inputs in the data
# MAGIC * Covariance can be calculated either as the sample covariance or the population covariance.

# COMMAND ----------

from pyspark.sql.functions import corr, covar_pop, covar_samp

df.select(corr("InvoiceNo", "Quantity"), covar_samp("InvoiceNo", "Quantity"),covar_pop("InvoiceNo", "Quantity")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Aggregating to Complex Types:**
# MAGIC In Spark, you can perform aggregations not just of numerical values using formulas, you can also perform them on complex types. 
# MAGIC 
# MAGIC For example, we can collect a list of values present in a given column or only the unique values by collecting to a set.

# COMMAND ----------

from pyspark.sql.functions import collect_set, collect_list

df.agg(collect_set("Country"), collect_list("Country")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Grouping
# MAGIC This will be done as group our data on one column and perform some calculations on the other columns that end up in that group.
# MAGIC We will group by each unique invoice number and get the count of items on that invoice. This will be done in 2 phases
# MAGIC 
# MAGIC * First we specify the column(s) on which we would like to group. This returns **Relatational Grouped Dataset.**
# MAGIC * then we specify the aggregation(s).This step returns **DataFrame.**

# COMMAND ----------

df.groupBy("InvoiceNo","CustomerId")

# COMMAND ----------

df.groupBy("InvoiceNo","CustomerId").count().show(4)

# COMMAND ----------

# MAGIC %md
# MAGIC **Grouping with Expressions:**
# MAGIC Rather than passing count() function as an expression into a select statement, we specify it as within agg. This makes it possible for you to pass-in arbitrary expressions that just need to have some aggregations specified

# COMMAND ----------

df.groupBy("InvoiceNo").agg(\
                           count("Quantity").alias("quan"),
                           expr("count(Quantity)")).show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC **Grouping with Maps:**
# MAGIC Sometimes it can be easier to specify your transformations as a series of Maps for which the key is the column and the value is the aggregation function. 
# MAGIC 
# MAGIC You can reuse multiple column names if you specify them inline.

# COMMAND ----------

df.groupBy("InvoiceNo").agg(\
                           expr("avg(Quantity)"),
                           expr("stddev_pop(Quantity)"),
                           expr("count(Quantity)")).show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Window Functions:
# MAGIC 
# MAGIC window functions are used to carry out some unique aggregations by either computing some aggregation on a specific **window** of data, which you define by using a reference to the current data.
# MAGIC 
# MAGIC This window specification determines which rows will be passed in to this function.
# MAGIC 
# MAGIC A groupBy takes data, and every row can go only into one grouping. A window function calculates a return value for every input row of a table based on group of rows, called a frame. Each row fall into one or more frames.
# MAGIC 
# MAGIC Spark supports three kinds of window functions:
# MAGIC * ranking functions - Rank, Dense_Rank, Row_Number etc..,
# MAGIC * analytic functions - Lead, Lag,First_value,Last_value etc..,
# MAGIC * aggregate functions- sum,avg,count,min,max etc..,
# MAGIC 
# MAGIC OVER clause defines the partitioning and ordering of rows(i.e a window) for the above functions to operate on. Hence these functions are called window functions. The OVER clause accepts the following three arguments to define a window for these functions to operate on.
# MAGIC 
# MAGIC * ORDERBY - Defines the logical order of the rows
# MAGIC * PARTITION BY - Divides the query result set in to partitions. The window function is applied to each partition separately.
# MAGIC * ROWS or RANGE Clause - Further limits the rows within the partition by specifying start and end points within the partition

# COMMAND ----------

from pyspark.sql.functions import to_date,col

dfWithDate = df.withColumn("date",to_date(col("InvoiceDate"),"MM/d/YYYY H:mm"))
dfWithDate.createOrReplaceTempView("dfWithDate")

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc

windowSpec = Window.partitionBy("CustomerId","date")\
                   .orderBy(desc("Quantity"))\
                   .rowsBetween(Window.unboundedPreceding, Window.currentRow)


# COMMAND ----------

# MAGIC %md
# MAGIC We need aggregate function to learn more about each specific customer. ex: maximum purchase quantity over all time

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

from pyspark.sql.functions import max
maxPurchaseQuantity = F.max(F.col("Quantity")).over(windowSpec)
maxPurchaseQuantity

# COMMAND ----------

# MAGIC %md
# MAGIC create the purchase quantity rank. For that, we use the **dense_rank** function to determine which date had the maximum purchase quantity
# MAGIC for every customer.
# MAGIC 
# MAGIC We use dense_rank as opposed to rank to avoid gaps in the ranking sequence when there are tied values

# COMMAND ----------

from pyspark.sql.functions import dense_rank, rank

purchaseDenseRank = dense_rank().over(windowSpec)
purchaseRank = rank().over(windowSpec)

# COMMAND ----------

dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")\
.select(\
       col("CustomerId"),
       col("date"),
       col("Quantity"),
       purchaseRank.alias("quantityRank"),
       purchaseDenseRank.alias("quantityDenseRank"),
       maxPurchaseQuantity.alias("maxPurchaseQuantity")).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CustomerId, date, Quantity,
# MAGIC rank(Quantity) OVER (PARTITION BY CustomerId, date
# MAGIC ORDER BY Quantity DESC NULLS LAST
# MAGIC ROWS BETWEEN
# MAGIC UNBOUNDED PRECEDING AND
# MAGIC CURRENT ROW) as rank,
# MAGIC dense_rank(Quantity) OVER (PARTITION BY CustomerId, date
# MAGIC ORDER BY Quantity DESC NULLS LAST
# MAGIC ROWS BETWEEN
# MAGIC UNBOUNDED PRECEDING AND
# MAGIC CURRENT ROW) as dRank,
# MAGIC max(Quantity) OVER (PARTITION BY CustomerId, date
# MAGIC ORDER BY Quantity DESC NULLS LAST
# MAGIC ROWS BETWEEN
# MAGIC UNBOUNDED PRECEDING AND
# MAGIC CURRENT ROW) as maxPurchase
# MAGIC FROM dfWithDate WHERE CustomerId IS NOT NULL ORDER BY CustomerId

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Grouping Sets
# MAGIC * an aggregation across multiple groups.
# MAGIC * These are a low-level tool for combining sets of aggregations together.
# MAGIC * 
# MAGIC 
# MAGIC total quantity of all stock codes and customers.

# COMMAND ----------

dfNoNull = dfWithDate.drop()
dfNoNull.createOrReplaceTempView("dfNoNull")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
# MAGIC GROUP BY customerId, stockCode
# MAGIC ORDER BY CustomerId DESC, stockCode DESC
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
# MAGIC GROUP BY GROUPING SETS((customerId, stockCode))
# MAGIC ORDER BY CustomerId DESC, stockCode DESC
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC But if we want to include the total number of items, regardless of customer or stockcode ? with conventiopnal group-by statement this impossible. But, its simple with grouping sets.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
# MAGIC GROUP BY GROUPING SETS((customerId, stockCode),())
# MAGIC ORDER BY CustomerId DESC, stockCode DESC
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The **GROUPING SETS** operator is only available in SQL. To perform the same in DataFrames, we use the rollup and cube operators - which will allow us to get the same results.

# COMMAND ----------

# MAGIC %md
# MAGIC Grouping sets depend on **null values** for aggregation levels. If you do not filter-out null values, you will get incorrect results. 
# MAGIC This applies to cubes, rollups, and grouping sets.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
# MAGIC GROUP BY customerId, stockCode GROUPING SETS(())
# MAGIC ORDER BY CustomerId DESC, stockCode DESC

# COMMAND ----------

# MAGIC %md
# MAGIC The GROUPING SETS operator is only available in SQL. To perform the same in DataFrames, we need to  use the **rollup** and **cube** operators—which allow us to get the same results.
# MAGIC 
# MAGIC **ROLLUP:** 
# MAGIC * is used to do aggregate Opereation on multiple levels in a heirarchy.  
# MAGIC * is a multidimensional aggregation that performs a variety of group-by style calculations

# COMMAND ----------

rolledUpDF = dfNoNull.rollup("Date","Country")\
                     .agg(sum("Quantity"))\
                     .selectExpr("Date","Country", "`sum(Quantity)` as total_quantity")\
                     .orderBy("Date")
rolledUpDF.count()

# COMMAND ----------

rolledUpDF.show(10)

# COMMAND ----------

rolledUpDF.where("Country IS NULL" and "Date IS NULL").show()
#rolledUpDF.where().show()

# COMMAND ----------

# MAGIC %md
# MAGIC **CUBE:**
# MAGIC * Produces the result set by generating all combinations of columns specified in GroupBy CUBE()
# MAGIC * Rather than treating elements hierarchically, a cube does the same thing across all dimensions. This means that it won’t just go by date over the entire time period, but also the country.
# MAGIC * To createa a table that has below combinations
# MAGIC   * The total across all dates and countries
# MAGIC   * The total for each date across all countries
# MAGIC   * The total for each country on each date
# MAGIC   * The total for each country across all dates

# COMMAND ----------

cubeDF = dfNoNull.cube("Date","Country")\
        .agg(sum("Quantity"))\
        .select("Date","Country","sum(Quantity)")\
        .orderBy("Date")
cubeDF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ** Difference Between CUBE and ROLLUP**
# MAGIC * CUBE generates a result that shows aggregation for all combinations of values in the selected columns.
# MAGIC * ROLLUP generates a result that shows aggregation for hierarchy of values in the selected columns.
# MAGIC 
# MAGIC ROLLUP("Date","Country") gives combinations of
# MAGIC   * "Date","Country"
# MAGIC   * "Date"
# MAGIC   * ()
# MAGIC It gave a count of 2022
# MAGIC 
# MAGIC cube("Date","Country") gives combination of
# MAGIC   * "Date","Country"
# MAGIC   * "Date"
# MAGIC   * "Country"
# MAGIC   * ()
# MAGIC It gave a count of 2060

# COMMAND ----------

# MAGIC %md
# MAGIC **GROUPING METADATA:**
# MAGIC Sometimes when using cubes and rollups, you want to be able to query the aggregation levels so that you can easily filter them down accordingly.
# MAGIC We can do this by using the grouping_id, which gives us a column specifying the level of aggregation that we have in our result set.

# COMMAND ----------

from pyspark.sql.functions import grouping_id,desc

dfNoNull.cube("customerId", "stockCode")\
        .agg(grouping_id(), sum("Quantity"))\
        .orderBy(desc("grouping_id()"))\
        .show()

# COMMAND ----------

# MAGIC %md
# MAGIC **PIVOT:**
# MAGIC * Used to turn unique values from one column, into multiple columns in the output.
# MAGIC * With a pivot, we can aggregate according to some function for each of those given countries and display them in an easy-to-query way.

# COMMAND ----------

pivoted = dfWithDate.groupBy("date").pivot("Country").sum()
pivoted.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC This DataFrame will now have a column for every combination of country, numeric variable, and a column specifying the date. 
# MAGIC 
# MAGIC For example, for USA we have columns:USA_sum(Quantity), USA_sum(UnitPrice), USA_sum(CustomerID).
# MAGIC 
# MAGIC This represents one for each numeric column in our dataset (because we just performed an aggregation over all of them).

# COMMAND ----------

pivoted.where("date > '2011-12-05'").select("date","`USA_sum(CustomerID)`","`USA_sum(CAST(Quantity AS BIGINT))`").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### User-Defined Aggregation Functions(UDAF):
# MAGIC 
# MAGIC * UDAFs to compute custom calculations over groups of input data (as opposed to single rows).
# MAGIC * they are a way for users to define their own aggregation functions based on custom formulae or business rules.
# MAGIC * UDAFs are currently available only in Scala or Java. But these can be called in Python.
# MAGIC * To create a UDAF, you must inherit from the **UserDefinedAggregateFunction** base class and implement the following methods:
# MAGIC    * **inputSchema** represents input arguments as a StructType
# MAGIC    * **bufferSchema** represents intermediate UDAF results as a StructType
# MAGIC    * **dataType** represents the return DataType
# MAGIC    * **deterministic** is a Boolean value that specifies whether this UDAF will return the same result for a given input
# MAGIC    * **initialize** allows you to initialize values of an aggregation buffer
# MAGIC    * **update** describes how you should update the internal buffer based on a given row
# MAGIC    * **merge** describes how two aggregation buffers should be merged
# MAGIC    * **evaluate** will generate the final result of the aggregation
# MAGIC    
# MAGIC  The following example implements a BoolAnd, which will inform us whether all the rows (for a given column) are true; if they’re not, it will return false.

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.expressions.MutableAggregationBuffer
# MAGIC import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
# MAGIC import org.apache.spark.sql.Row
# MAGIC import org.apache.spark.sql.types._
# MAGIC 
# MAGIC class BoolAnd extends UserDefinedAggregateFunction {
# MAGIC   def inputSchema: org.apache.spark.sql.types.StructType = StructType(StructField("value", BooleanType) :: Nil)
# MAGIC   
# MAGIC   def bufferSchema: StructType = StructType(StructField("result", BooleanType) :: Nil)
# MAGIC   
# MAGIC   def dataType: DataType = BooleanType
# MAGIC   
# MAGIC   def deterministic: Boolean = true
# MAGIC   
# MAGIC   def initialize(buffer: MutableAggregationBuffer): Unit = {
# MAGIC     buffer(0) = true
# MAGIC   }
# MAGIC   
# MAGIC   def update(buffer: MutableAggregationBuffer, input: Row):Unit = {
# MAGIC     buffer(0) = buffer.getAs[Boolean](0) && input.getAs[Boolean](0)
# MAGIC   }
# MAGIC   
# MAGIC   def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
# MAGIC     buffer1(0) = buffer1.getAs[Boolean](0) && buffer2.getAs[Boolean](0)
# MAGIC     }
# MAGIC   
# MAGIC   def evaluate(buffer: Row): Any = {
# MAGIC     buffer(0)
# MAGIC     }
# MAGIC 
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC val ba = new BoolAnd
# MAGIC spark.udf.register("booland", ba)
# MAGIC import org.apache.spark.sql.functions._
# MAGIC spark.range(1)
# MAGIC .selectExpr("explode(array(TRUE, TRUE, TRUE)) as t")
# MAGIC .selectExpr("explode(array(TRUE, FALSE, TRUE)) as f", "t")
# MAGIC .select(ba(col("t")), expr("booland(f)"))
# MAGIC .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Conclusion:
# MAGIC We have learnt about simple grouping-to window functions as well as rollups and cubes. Next chapterdiscusses how to perform joins to combine different data sources together.