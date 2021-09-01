# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC ## Chapter 6. Working with Different Types of Data
# MAGIC 
# MAGIC This chapter covers building expressions, which are the bread and butter of Spark’s structured operations.
# MAGIC 
# MAGIC All SQL and DataFrame functions are found at "https://spark.apache.org/docs/latest/api/python/pyspark.sql.html"

# COMMAND ----------

df = spark.read.format("csv")\
     .option("header","true")\
     .option("inferSchema","true")\
     .load("/FileStore/tables/retail-data/by-day/2010_12_01-ec65d.csv")

df.printSchema()
df.createOrReplaceTempView("dftable")

# COMMAND ----------

# MAGIC %md
# MAGIC Converting native types to SPARK types

# COMMAND ----------

from pyspark.sql.functions import lit
df.select(lit(5),lit("five"),lit(5.0))

# COMMAND ----------

# MAGIC %sql
# MAGIC select 5,"five",5.0

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ###### working with Booleans
# MAGIC * Boolean statements consist of four elements: and, or, true, and false. We use these simple structures to build logical statements that evaluate to either true or false.
# MAGIC * We can specify Boolean expressions with multiple parts when you use **and** or **or** . 
# MAGIC * In spark we should always chain together **and** filters as a sequential filter. The reason for this is that even if Boolean statements are expressed serially (one after the other),Spark will flatten all of these filters into one statement and perform the filter at the same time, creating the and statement for us.
# MAGIC * **or** statements need to be specified in the same statement

# COMMAND ----------

from pyspark.sql.functions import col, instr
priceFilter = col("UnitPrice") > 600
descripFIlter = col("Description").contains("POSTAGE")

df.where(df.StockCode.isin("DOT")).where(priceFilter | descripFIlter).show()

# COMMAND ----------

from pyspark.sql.functions import instr
priceFilter = col("UnitPrice") > 600
descripFilter = instr(df.Description, "POSTAGE") >= 1

# COMMAND ----------

df.filter(col('StockCode').isin('DOT')).filter(priceFilter | descripFilter).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM dftable 
# MAGIC WHERE StockCode in ("DOT")
# MAGIC AND (UnitPrice > 660 OR instr(Description,"POSTAGE") >= 1)

# COMMAND ----------

# MAGIC %md
# MAGIC To filter a DataFrame, you can also just specify a Boolean column. Here is code to 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT UnitPrice, (StockCode = 'DOT' AND
# MAGIC (UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1)) as isExpensive
# MAGIC FROM dfTable
# MAGIC WHERE (StockCode = 'DOT' AND
# MAGIC (UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1))

# COMMAND ----------

DOTCodeFilter = col("StockCode") == "DOT"
priceFilter = col("UnitPrice") > 600
descripFilter = instr(col("Description"), "POSTAGE") >= 1

df.withColumn("isExpensive",DOTCodeFilter & (priceFilter | descripFilter))\
  .where("isExpensive")\
  .select("unitPrice","isExpensive").show(5)

# COMMAND ----------

from pyspark.sql.functions import expr
df.withColumn("isExpensive",expr("NOT UnitPrice <= 250"))\
  .filter("isExpensive")\
  .select("Description","UnitPrice").show(5)

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Working with Numbers

# COMMAND ----------

from pyspark.sql.functions import expr, pow
fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"),2)+5
df.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(2)

# COMMAND ----------

df.selectExpr(
"CustomerId",
"(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity").show(2)

# COMMAND ----------

#round to a whole number
from pyspark.sql.functions import round,bround
df.select(round(col("UnitPrice"),1).alias("rounded"),col("UnitPrice")).show(5)

# COMMAND ----------

df.select(round(F.lit("2.5")), bround(F.lit("2.5"))).show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC correlation of two columns

# COMMAND ----------

from pyspark.sql.functions import corr
df.stat.corr("Quantity", "UnitPrice")
df.select(corr("Quantity", "UnitPrice")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC To compute summary statistics for a column or set of columns we can use **describe()** function. This will take all numeric columns and
# MAGIC calculate the count, mean, standard deviation, min, and max.

# COMMAND ----------

df.describe().show()

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

# COMMAND ----------

df.select(F.monotonically_increasing_id()).show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Working with Strings
# MAGIC 
# MAGIC * **initcap** function will capitalize every word in a given string when that word is separated from another by a space.

# COMMAND ----------

from pyspark.sql.functions import initcap,lower,upper

df.select(initcap(col("Description"))).show(2)

# COMMAND ----------

df.select(col("Description").alias("DESC"),
          lower(col("Description")).alias("LOWER DESC"),
          upper(col("Description")).alias("UPPER DESC")).show(2)

# COMMAND ----------

from pyspark.sql.functions import lit, ltrim, rtrim, rpad, lpad, trim
df.select(ltrim(lit(" HELLO ")).alias("ltrim"),
          rtrim(lit(" HELLO ")).alias("rtrim"),
          trim(lit(" HELLO ")).alias("trim"),
          lpad(lit("HELLO"), 3, " ").alias("lp"),
          rpad(lit("HELLO"), 10, " ").alias("rp")).show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Regular Expression
# MAGIC There are two key functions in Spark to perform regular expression tasks
# MAGIC * **regexp_extract** functions extract values
# MAGIC * **regexp_replace** replace values

# COMMAND ----------

from pyspark.sql.functions import regexp_replace
regex_string = "BLACK|WHITE|RED|GREEN|BLUE"
df.select(regexp_replace(df.Description,regex_string,'COLOR').alias("color_clean"),col("Description")).show(2)

# COMMAND ----------

from pyspark.sql.functions import regexp_extract
extract_string = "(BLACK|WHITE|RED|GREEN|BLUE)"

df.select(regexp_extract(df.Description,extract_string,1).alias("color_clean"),col("Description")).show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC When we want to check if the string exists in the column, use **Contains()c** function. This will returns a Boolean declaring whether the value you specify is in the column's string

# COMMAND ----------

containsBlack = df.Description.contains("BLACK")
containsWhite = instr(col("Description"),"WHITE") >= 1

df.withColumn("hasBlackNWhite", containsBlack | containsWhite).where("hasBlackNWhite").select("Description").show(3)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Description from dftable
# MAGIC WHERE instr(Description, 'BLACK') >= 1 OR instr(Description, 'WHITE') >= 1
# MAGIC LIMIT 3

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC When we convert a list of values into a set of arguments and pass them into a function, we use a language feature called **varargs**. Using this feature, we can effectively unravel an array of arbitrary length and pass it as arguments to a function. 
# MAGIC 
# MAGIC **locate** that returns the integer location
# MAGIC 
# MAGIC Locate the position of the first occurrence of substr in a string column, after position pos.
# MAGIC 
# MAGIC **Note** The position is not zero based, but 1 based index. Returns 0 if substr could not be found in str.
# MAGIC Parameters:
# MAGIC substr – a string
# MAGIC str – a Column of pyspark.sql.types.StringType
# MAGIC pos – start position (zero based)

# COMMAND ----------

from pyspark.sql.functions import expr, locate

simpleColors = ["black", "white", "red", "green", "blue"]

def color_locator(column, color_string):
  return locate(color_string.upper(), column).cast("boolean").alias("is_" + color_string)

selectColumns = [color_locator(df.Description, c) for c in simpleColors]
selectColumns.append(expr("*")) # has to a be Column type

df.select(*selectColumns).where(expr("is_white OR is_red")).select("Description").show(3, False)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC If you use **col()** and want to perform transformations on that column, you must perform those on that column reference. 
# MAGIC When using an expression, the **expr** function can actually **parse transformations** and **column references** from a string and can subsequently be passed into further transformations.
# MAGIC Key-Points
# MAGIC 
# MAGIC * Columns are just expressions.
# MAGIC * Columns and transformations of those columns compile to the same logical plan as parsed expressions.

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Working with Dates and Timestamps
# MAGIC 
# MAGIC You can set a session local timezone if necessary by **setting spark.conf.sessionLocalTimeZone** in the SQL configurations.

# COMMAND ----------

from pyspark.sql.functions import current_date, current_timestamp

dateDf = spark.range(10)\
     .withColumn("TodayDate",current_date())\
     .withColumn("Now",current_timestamp())

dateDf.createOrReplaceTempView("dateTable")
dateDf.printSchema()

# COMMAND ----------

#To add or substract five days from today
from pyspark.sql.functions import date_add, date_sub

dateDf.select(date_add("TodayDate",5),date_sub("TodayDate",5)).show(1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date_add(TodayDate,5),date_sub(TodayDate,5)
# MAGIC FROM dateTable
# MAGIC LIMIT 1

# COMMAND ----------

from pyspark.sql.functions import datediff, months_between, to_date, col, lit

dateDf.withColumn("week_ago", date_sub(col("TodayDate"),7))\
      .select(datediff(col("week_ago"),col("TodayDate"))).show(1)

# COMMAND ----------

dateDf.select(
to_date(lit("2019-06-23")).alias("start"),
  to_date(lit("2019-11-23")).alias("end")
).select(months_between(col("start"),col("end"))).show(1)

# COMMAND ----------

# MAGIC %md
# MAGIC Spark will not throw an error if it cannot parse the date; rather, it will just return **null.**
# MAGIC 
# MAGIC Example:

# COMMAND ----------

dateDf.select(to_date(lit("2016-20-12")),to_date(lit("2017-12-11"))).show(1)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC notice how the second date appears as Decembers 11th instead of the correct day, November 12th. Spark doesn’t throw an
# MAGIC error because it cannot know whether the days are mixed up or that specific row is incorrect. To fix this we use **to_date** and **to_timestamp**

# COMMAND ----------

dateFormat = "YYYY-dd-MM"

cleanDF = spark.range(1).select(
          to_date(lit("2017-12-11"),dateFormat).alias("date"),
          to_date(lit("2017-20-12"),dateFormat).alias("date2"))

cleanDF.createOrReplaceTempView("dataTable2")

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

cleanDF.select(to_timestamp(col("date"),dateFormat)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Working with Nulls in Data
# MAGIC Spark can optimize working with null values more than it can if you use empty strings or other values. 
# MAGIC * use .na subpackage on a DataFrame
# MAGIC * **Spark includes a function to allow you to select the first non-null value from a set of columns by using the coalesce function.**

# COMMAND ----------

from pyspark.sql.functions import coalesce
df.select(coalesce(col("Description"), col("CustomerId"))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The simplest function is drop, which removes rows that contain nulls. The default is to drop any row in which any value is null:
# MAGIC * df.na.drop() / df.na.drop("any") - drops a row if any of the values are null.
# MAGIC * df.na.drop("all") - drops the row only if all values are null or NaN for that row

# COMMAND ----------

# MAGIC %md
# MAGIC **fill:**
# MAGIC 
# MAGIC Using the fill function, you can fill one or more columns with a set of values. This can be done by specifying a map—that is a particular value and a set of columns.

# COMMAND ----------

df.na.fill("all", subset=["StockCode", "InvoiceNo"])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We can also do this with with a Scala Map, where the key is the column name and the value is the
# MAGIC value we would like to use to fill null values

# COMMAND ----------

fill_cols_vals = {"StockCode": 5, "Description" : "No Value"}
df.na.fill(fill_cols_vals)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### replace
# MAGIC To replace all values in a certain column according to their current value. The only requirement is that this value be the same type as the original value

# COMMAND ----------

df.na.replace([""], ["UNKNOWN"], "Description")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Working with Complex Types
# MAGIC three kinds of complex types: **structs, arrays,& maps.**
# MAGIC 
# MAGIC **structs:** You can think of structs as DataFrames within DataFrames. 

# COMMAND ----------

df.selectExpr("(Description, InvoiceNo) as complex", "*")
df.selectExpr("struct(Description, InvoiceNo) as complex", "*")

# COMMAND ----------

from pyspark.sql.functions import struct
complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
complexDF.createOrReplaceTempView("complexDF")

# COMMAND ----------

# MAGIC %md
# MAGIC We now have a DataFrame with a column complex. We can query it just as we might another DataFrame, the only difference is that we use a dot syntax to do so, or the column method **getField**

# COMMAND ----------

complexDF.select("complex.InvoiceNo").show(2)
complexDF.select(col("complex").getField("Description"))

# COMMAND ----------

#We can also query all values in the struct by using *. This brings up all the columns to the toplevel DataFrame
complexDF.select("complex.*")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Arrays
# MAGIC to explain it in more details lets take every single word in our Description column and convert that into a row in our DataFrame. In this we use 
# MAGIC * **split** function and specify the delimiter
# MAGIC * **Array Length** to query its size
# MAGIC * **array_contains** to check whether the given array contains the specified value
# MAGIC * **explode function** takes a column that consists of arrays and creates one row (with the rest of the values duplicated) per value in the array.

# COMMAND ----------

from pyspark.sql.functions import split

df.select(split(col("Description"), " ")).show(1)

df.select(split(col("Description"), " ").alias("array_col")).selectExpr("array_col[0]").show(1)

# COMMAND ----------

from pyspark.sql.functions import size

df.select(size(split(col("Description"), " "))).show(2)

# COMMAND ----------

from pyspark.sql.functions import array_contains

df.select(array_contains(split(col("Description")," "),'WHITE')).show(2)

# COMMAND ----------

# To convert a complex type into a set of rows (one per value in our array), we need to use the explode function.
from pyspark.sql.functions import explode

df.withColumn("splitted",split(col("Description")," "))\
  .withColumn("exploded",explode("splitted"))\
  .select("Description","InvoiceNo","exploded").show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Maps:
# MAGIC Maps are created by using the map function and key-value pairs of columns. You then can select them just like you might select from an array

# COMMAND ----------

from pyspark.sql.functions import create_map

df.select(create_map(col("Description"),col("InvoiceNo")).alias("complex_map")).show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC You can query them by using the proper key. A missing key returns **null**

# COMMAND ----------

df.select(create_map(col("Description"), col("InvoiceNo")).alias("complex_map"))\
.selectExpr("complex_map['WHITE METAL LANTERN']").show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC We can use **explode** function on map which will turn them in to columns

# COMMAND ----------

df.select(create_map(col("Description"), col("InvoiceNo")).alias("complex_map"))\
.selectExpr("explode(complex_map)").show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Working with JSON
# MAGIC We can operate directly on strings of JSON in Spark and parse from JSON or extract JSON objects. We can use
# MAGIC * **get_json_object** to inline query a JSON object, be it a dictionary or array.
# MAGIC * **json_tuple** if this object has only one level of nesting.

# COMMAND ----------

jsonDF = spark.range(1).selectExpr(""" '{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}' as jsonString """)
jsonDF.show()

# COMMAND ----------

from pyspark.sql.functions import get_json_object, json_tuple,col

jsonDF.select(\
              get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[1]"),
              json_tuple(col("jsonString"), "myJSONKey").alias("ex_jsonString")
             ).show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC * we can also turn a StructType into a JSON string by using the **to_json** function. This function also accepts a dictionary (map) of parameters that are the same as the JSON data source.
# MAGIC 
# MAGIC * use **from_json** function to parse this (or other JSON data) back in. This naturally requires you to specify a schema, and optionally you can specify a map of options

# COMMAND ----------

from pyspark.sql.functions import to_json, from_json

df.selectExpr("(InvoiceNo, Description) as myStruct"
             ).select(to_json(col("myStruct"))).show()

# COMMAND ----------

from pyspark.sql.functions import to_json, from_json
from pyspark.sql.types import *

Schema = StructType((
                    StructField("InvoiceNo",StringType(),True),
                    StructField("Description",StringType(),True)))
df.selectExpr("(InvoiceNo, Description) as myStruct")\
  .select(to_json(col("myStruct")).alias("newJSON"))\
  .select(from_json(col("newJSON"),Schema),col("newJSON")).show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###### User-Defined Functions - define your own functions
# MAGIC 
# MAGIC * UDFs can take and return one or more columns as input.
# MAGIC * you can write them in several different programming languages; you do not need to create them in an esoteric format or domain-specific language.
# MAGIC * By default, these functions are registered as temporary functions to be used in that specific SparkSession or Context
# MAGIC 
# MAGIC **performance considerations:**
# MAGIC * If the function is written in Scala or Java, you can use it within the Java Virtual Machine (JVM). This means that there will be little performance penalty aside from the fact that you can’t take advantage of code generation capabilities that Spark has for built-in functions.
# MAGIC 
# MAGIC * If the function is written in Python, SPARK 
# MAGIC   * strats a Python process on the worker
# MAGIC   * serializes all of the data to a format that Python can understand
# MAGIC   * executes the function row by row on that data in the Python process, and then finally 
# MAGIC   * returns the results of the row operations to the JVM and Spark
# MAGIC   
# MAGIC Starting this Python process is expensive, but the real cost is in serializing the data to Python.it is an expensive computation, but also, after the data enters Python, Spark cannot manage the memory of the worker. This means that it could potentially cause a worker to fail
# MAGIC if it becomes resource constrained (because both the JVM and Python are competing for memory on the same machine).
# MAGIC 
# MAGIC It is recommended to write your UDFs in Scala or Java—the small amount of time it should take you to write the function in Scala will always yield significant speed ups, and we can still use the function from Python!

# COMMAND ----------

#create UDF function to calculate power3 

udfExampleDF = spark.range(5).toDF("num")
def power3(double_value):
  return double_value **3

power3(2.0)

# COMMAND ----------

# register them with Spark so that we can use them on all of our worker machines. Spark will serialize the function on the driver and transfer it over the network to all executor processes. This happens regardless of language

from pyspark.sql.functions import udf

power3udf = udf(power3)
udfExampleDF.select(power3udf(col("num"))).show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC we can use this only as a DataFrame function. We can’t use it within a **string expression, only on an expression.**
# MAGIC 
# MAGIC * Spark SQL function or expression is valid to use as an expression when working with DataFrames.
# MAGIC 
# MAGIC 
# MAGIC Let’s register the function in Scala and use it in python

# COMMAND ----------

# MAGIC %scala
# MAGIC val udfExampleDF = spark.range(5).toDF("num")
# MAGIC def power3scala(number:Double):Double = number * number * number
# MAGIC 
# MAGIC spark.udf.register("power3Scala", power3scala(_:Double):Double)
# MAGIC udfExampleDF.selectExpr("power3Scala(num)").show(2)

# COMMAND ----------

#using the power3Scala function in python
udfExampleDF.selectExpr("power3Scala(num)").show(2)

# COMMAND ----------

# MAGIC %md 
# MAGIC * It’s a best practice to define the return type for your function when you define it. FUnction works fine if it doesn't have a return type.
# MAGIC 
# MAGIC * If you specify the type that doesn’t align with the actual type returned by the function, Spark will not throw an error but will just return **null** to designate a failure

# COMMAND ----------

from pyspark.sql.types import IntegerType, DoubleType

spark.udf.register("power3py",power3,DoubleType())
udfExampleDF.selectExpr("power3py(num)").show(2)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC *** End of the chapter ***