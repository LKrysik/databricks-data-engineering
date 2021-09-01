# Databricks notebook source
# MAGIC %md
# MAGIC ###### SPARK SQL:
# MAGIC * with Spark SQL you can run SQL queries against views or tables organized into databases.
# MAGIC * Spark SQL is intended to operate as an online analytic processing (OLAP) database, not an online transaction processing (OLTP) database. This means that it is not intended to perform extremely lowlatency.

# COMMAND ----------

spark.sql("SELECT 1 + 1").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### How to Run Spark SQL Queries:
# MAGIC Spark provides several interfaces to execute SQL queries.
# MAGIC 
# MAGIC **Spark SQL CLI:**
# MAGIC * The Spark SQL CLI is a convenient tool with which you can make basic Spark SQL queries in local mode from the command line.
# MAGIC * this mode cannot communicate with the Thrift JDBC server.
# MAGIC 
# MAGIC **Spark’s Programmatic SQL Interface:**
# MAGIC * we can also execute SQL in an ad hoc manner via any of Spark’s language APIs.
# MAGIC * We can do this via the method sql on the SparkSession object. 
# MAGIC * This returns a DataFrame.
# MAGIC * Just like other transformations, this will not be executed eagerly but lazily.

# COMMAND ----------

spark.sql("SELECT 1+1").show()

# COMMAND ----------

spark.read.json("/FileStore/tables/2010_summary-506d8.json")\
.createOrReplaceTempView("some_sql_view") # DF => SQL

spark.sql("SELECT DEST_COUNTRY_NAME, sum(count)\
FROM some_sql_view GROUP BY DEST_COUNTRY_NAME")\
.where("DEST_COUNTRY_NAME like 'S%'").where("`sum(count)` > 10")\
.count() # SQL => DF

# COMMAND ----------

# MAGIC %md
# MAGIC **SparkSQL Thrift JDBC/ODBC Server:**
# MAGIC 
# MAGIC * SPARK provides a Java DataBase Connection(JDBC) interface by which either you or a remote program connects to the Spark driver in order to execute Spark SQL queries.
# MAGIC   
# MAGIC   Eg: a business analyst to connect business intelligence software like Tableau to Spark.
# MAGIC   For More details check this link to connect using DataBricks "https://docs.databricks.com/bi/jdbc-odbc-bi.html"

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Catalog:
# MAGIC * Highest level of abstraction in SPARK SQL is Catalog.
# MAGIC * The Catalog is an abstraction for the storage of 
# MAGIC     * metadata about the data stored in your tables as well as 
# MAGIC     * other helpful things like databases, tables, functions, and views.
# MAGIC * The catlog is availble in "https://spark.apache.org/docs/2.3.0/api/python/_modules/pyspark/sql/catalog.html". It contains different functions like listing tables,databases, and functions.

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Tables:
# MAGIC * Tables are logically equivalent to a DataFrame in that they are a structure of data against which you run commands.
# MAGIC * We can join tables, filter them, aggregate them, and perform different manipulations.
# MAGIC * core difference between tables and DataFrames is
# MAGIC   * we define DataFrames in the scope of a programming language, whereas you define tables within a database.
# MAGIC   * It means that when you create a table (assuming you never changed the database), it will belong to the default database.
# MAGIC * Spark 2.X, tables always contain data. There is no notion of a temporary table, only a view, which does not contain data.
# MAGIC * If you drop a table, you can risk losing the data when doing so.

# COMMAND ----------

# MAGIC %md
# MAGIC **SPARK-Managed Tables:**
# MAGIC 
# MAGIC * Two types of information, The data within the tables as well as the data about the tables(MetaData).
# MAGIC * When we define a table from files on disk, we are defining a manage table.
# MAGIC * When we use **saveAsTable** on a Dataframe, We are creating a managed table for which SPARK will track all the relevant information.
# MAGIC * this will read the table and write to new location in SPARK format. 
# MAGIC 
# MAGIC **Creating Tables:**
# MAGIC * SPARK lets you create table on the fly. We dont need to define a table and then load data into it.
# MAGIC * We can define all sophisticated options when you read in a file.
# MAGIC * We can specify all sorts of sophisticated options when you read in a file.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE flights (DEST_COUNTRY_NAME STRING, 
# MAGIC                       ORIGIN_COUNTRY_NAME STRING, 
# MAGIC                       count LONG)
# MAGIC USING JSON OPTIONS (path '/FileStore/tables/2010_summary-506d8.json')

# COMMAND ----------

# MAGIC %md
# MAGIC **USING, COMMENT AND STORED AS:**
# MAGIC * USING  format is used to specify that to use SPARK's native serialization. If we don't specify the format, Spark will default to a Hive SerDe(Serialize and Deserialize) configuration.
# MAGIC * Hive users can also use the STORED AS syntax to specify that this should be a Hive table.
# MAGIC * we can add comments to certain columns in a table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE flights_csv (
# MAGIC   DEST_COUNTRY_NAME STRING,
# MAGIC   ORIGIN_COUNTRY_NAME STRING COMMENT "remember, the US will be most prevalent",
# MAGIC   count LONG)
# MAGIC USING csv OPTIONS (header true, path '/data/flight-data/csv/2015-summary.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC It is possible to create a table from a query as well

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE flights_from_select USING parquet AS SELECT * FROM flights_csv

# COMMAND ----------

# MAGIC %md
# MAGIC we can specify to create a table only if it does not currently exist. Here we are creating a Hive-compatible table because we did not explicitly specify the format via USING.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS flights_from_select AS SELECT * FROM flights_csv

# COMMAND ----------

# MAGIC %md
# MAGIC we can control the layout of the data by writing out a partitioned dataset.
# MAGIC 
# MAGIC These tables will be available in Spark even through sessions; temporary tables do not currently exist in Spark. You must create a **temporary view**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE partitioned_flights 
# MAGIC   USING parquet PARTITIONED BY (DEST_COUNTRY_NAME)
# MAGIC   AS SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count FROM flights LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating External Tables:**
# MAGIC * One of the usecase to create External table is to port your legacy Hive statements to Spark SQL.
# MAGIC * In this case Spark will manage the table’s metadata; however, the files are not managed by Spark at all.
# MAGIC * create this table by using the CREATE EXTERNAL TABLE statement.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE hive_flights (
# MAGIC                 DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)
# MAGIC                 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/user/hive/warehouse/flights_from_select/'

# COMMAND ----------

# MAGIC %md
# MAGIC We can also create an external table from a select clause

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE hive_flights_2
# MAGIC ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
# MAGIC LOCATION '/user/hive/warehouse/flights_from_select/' AS SELECT * FROM flights

# COMMAND ----------

# MAGIC %md
# MAGIC **Inserting into Tables:**

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO flights_from_select
# MAGIC SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count FROM flights LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC * we can optionally provide a partition specification if you want to write only into a certain partition. 
# MAGIC * Write will respect a partitioning scheme, as well (which may cause the query to run quite slowly); however, it will add additional files only into the end partitions

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO partitioned_flights
# MAGIC PARTITION (DEST_COUNTRY_NAME="UNITED STATES")
# MAGIC SELECT count, ORIGIN_COUNTRY_NAME FROM flights
# MAGIC WHERE DEST_COUNTRY_NAME='UNITED STATES' LIMIT 12

# COMMAND ----------

# MAGIC %md
# MAGIC **Describing Table Metadata:**
# MAGIC 
# MAGIC * We can view all the comments by describing the metadata 
# MAGIC * partitioning scheme for the data by using **SHOW PARTITIONS**

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE flights_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS partitioned_flights

# COMMAND ----------

# MAGIC %md
# MAGIC **Refreshing Table Metadata:**
# MAGIC There are two commands to refresh table metadata.
# MAGIC * REFRESH TABLE refreshes all cached entries (essentially, files) associated with the table. If the table were previously cached, it would be cached lazily the next time it is scanned.

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH table partitioned_flights

# COMMAND ----------

# MAGIC %md
# MAGIC * REPAIR TABLE, which refreshes the partitions maintained in the catalog for that given table.
# MAGIC  
# MAGIC This command’s focus is on collecting new partition information— an example might be writing out a new partition manually and the need to repair the table accordingly.

# COMMAND ----------

# MAGIC %sql
# MAGIC MSCK REPAIR TABLE partitioned_flights

# COMMAND ----------

# MAGIC %md
# MAGIC **Dropping Tables:**
# MAGIC * You cannot delete tables: you can only “drop” them.
# MAGIC * If you drop a managed table (e.g., flights_csv), both the data and the table definition will be removed

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE flights_csv

# COMMAND ----------

# MAGIC %md
# MAGIC To only delete a table if it already exists, use DROP TABLE IF EXISTS.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS flights_csv;

# COMMAND ----------

# MAGIC %md
# MAGIC **Dropping unmanaged tables:**
# MAGIC If you are dropping an unmanaged table (e.g., hive_flights), no data will be removed but you will no longer be able to refer to this data by the table name.

# COMMAND ----------

# MAGIC %md
# MAGIC **Caching Tables:**
# MAGIC Just like DataFrames, you can cache and uncache tables. You simply specify which table you would like using the following syntax:

# COMMAND ----------

# MAGIC %sql
# MAGIC CACHE TABLE flights

# COMMAND ----------

# MAGIC %md
# MAGIC Here’s how you uncache them:

# COMMAND ----------

# MAGIC %sql
# MAGIC UNCACHE TABLE FLIGHTS

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Views:
# MAGIC * A view specifies a set of transformations on top of the existing table.
# MAGIC * Basically just saved query plans, which can be convenient for organising or reusing your query logic.
# MAGIC * Views can be
# MAGIC   * global
# MAGIC   * set to a database or
# MAGIC   * per session
# MAGIC   
# MAGIC **Creating Views:**
# MAGIC * To an end user, views are displayed as tables, except rather than rewriting all of the data to a new location, they simply perform a transformation on the source data at query time.
# MAGIC * we create a view in which the destination is United States in order to see only those flights.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW just_usa_view AS
# MAGIC SELECT * FROM flights WHERE dest_country_name = 'United States'

# COMMAND ----------

# MAGIC %md
# MAGIC Like tables, you can create temporary views that are available only during the current session and are not registered to a database

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMP VIEW just_usa_view_temp AS
# MAGIC SELECT * FROM flights WHERE dest_country_name = 'United States'

# COMMAND ----------

# MAGIC %md
# MAGIC Global temp views are resolved regardless of database and are viewable across the entire Spark application, but they are removed at the end of the session.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE GLOBAL TEMP VIEW just_usa_global_view_temp AS
# MAGIC SELECT * FROM flights WHERE dest_country_name = 'United States'

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC To overwite a view if one already exists by using the keywords. We can overwrite both temp views and regular views

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW just_usa_view_temp AS
# MAGIC SELECT * FROM flights WHERE dest_country_name = 'United States'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM just_usa_view_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN SELECT * FROM flights WHERE dest_country_name = 'United States'

# COMMAND ----------

# MAGIC %md
# MAGIC **Dropping Views:**
# MAGIC * We can drop views in the same way that you drop tables.
# MAGIC * difference between dropping a view and dropping a table is
# MAGIC   * with a view, no underlying data is removed, only the view definition itself
# MAGIC   * with table both data and definition are deleted.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS just_usa_view

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Databases:
# MAGIC * Databases are a tool for organizing tables.
# MAGIC * If No databases are defined, SPARK uses default database.
# MAGIC * Any SQL statements that you run from within Spark (including DataFrame commands) execute within the context of a database.
# MAGIC * This means that if you change the database, any user-defined tables will remain in the previous database and will need to be queried differently.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating Databases:**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE some_db

# COMMAND ----------

# MAGIC %md
# MAGIC **Setting the Database:** You might want to set a database to perform a certain query

# COMMAND ----------

# MAGIC %sql
# MAGIC USE some_db

# COMMAND ----------

# MAGIC %md
# MAGIC After you set this database, all queries will try to resolve table names to this database. Queries that were working just fine might now fail or yield different results because you are in a different database

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM flights

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.flights

# COMMAND ----------

# MAGIC %md
# MAGIC We can see what database you’re currently using by running the following command:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_database()

# COMMAND ----------

# MAGIC %md
# MAGIC we can switch back to the default database by

# COMMAND ----------

# MAGIC %sql
# MAGIC USE default

# COMMAND ----------

# MAGIC %md
# MAGIC **Dropping Databases:**
# MAGIC We simply use the DROP DATABASE keyword

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS some_db

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Select Statements:
# MAGIC Queries in Spark support the following ANSI SQL requirements
# MAGIC 
# MAGIC     SELECT [ALL|DISTINCT] named_expression[, named_expression, ...]
# MAGIC         FROM relation[, relation, ...]
# MAGIC         [lateral_view[, lateral_view, ...]]
# MAGIC         [WHERE boolean_expression]
# MAGIC         [aggregation [HAVING boolean_expression]]
# MAGIC         [ORDER BY sort_expressions]
# MAGIC         [CLUSTER BY expressions]
# MAGIC         [DISTRIBUTE BY expressions]
# MAGIC         [SORT BY sort_expressions]
# MAGIC         [WINDOW named_window[, WINDOW named_window, ...]]
# MAGIC         [LIMIT num_rows]
# MAGIC 
# MAGIC named_expression:
# MAGIC 
# MAGIC         : expression [AS alias]
# MAGIC       
# MAGIC relation:
# MAGIC 
# MAGIC       | join_relation
# MAGIC       | (table_name|query|relation) [sample] [AS alias]
# MAGIC       : VALUES (expressions)[, (expressions), ...]
# MAGIC       [AS (column_name[, column_name, ...])]
# MAGIC       
# MAGIC expressions:
# MAGIC 
# MAGIC       : expression[, expression, ...]
# MAGIC       
# MAGIC sort_expressions:
# MAGIC 
# MAGIC       : expression [ASC|DESC][, expression [ASC|DESC], ...]

# COMMAND ----------

# MAGIC %md
# MAGIC **case…when…then Statements:**
# MAGIC To conditionally replace values in your SQL queries, we can use case...when...then...end style statement.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC CASE WHEN DEST_COUNTRY_NAME = 'UNITED STATES' THEN 1
# MAGIC WHEN DEST_COUNTRY_NAME = 'Egypt' THEN 0
# MAGIC ELSE -1 END
# MAGIC FROM partitioned_flights

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Querying Complex Types:
# MAGIC There are three core complex types in Spark SQL: structs, lists, and maps.
# MAGIC 
# MAGIC **Structs:**
# MAGIC Structs provide a way of creating or querying nested data in Spark. To create one, you simply need to wrap a set of columns (or expressions) in parentheses

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW IF NOT EXISTS nested_data AS
# MAGIC SELECT (DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME) as country, count FROM flights

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from nested_data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT country.DEST_COUNTRY_NAME, count FROM nested_data

# COMMAND ----------

# MAGIC %md
# MAGIC **LIST:**
# MAGIC * We can use the **collect_list** function, which creates a list of values.
# MAGIC * We can also use the function **collect_set,** which creates an array without duplicate values.
# MAGIC * These are both aggregation functions and therefore can be specified only in aggregations

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DEST_COUNTRY_NAME as new_name, 
# MAGIC        collect_list(count) as flight_counts,
# MAGIC        collect_set(ORIGIN_COUNTRY_NAME) as origin_set
# MAGIC FROM flights GROUP BY DEST_COUNTRY_NAME

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DEST_COUNTRY_NAME, ARRAY(1, 2, 3) FROM flights

# COMMAND ----------

# MAGIC %md
# MAGIC We can also query lists by position by using a Python-like array query syntax

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DEST_COUNTRY_NAME as new_name, collect_list(count)[0]
# MAGIC FROM flights GROUP BY DEST_COUNTRY_NAME

# COMMAND ----------

# MAGIC %md
# MAGIC * We can also do things like convert an array back into rows. 
# MAGIC * we do this by using the explode function. To demonstrate, let’s create a new view as our aggregation

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW flights_agg AS
# MAGIC SELECT DEST_COUNTRY_NAME, collect_list(count) as collected_counts
# MAGIC FROM flights GROUP BY DEST_COUNTRY_NAME

# COMMAND ----------

# MAGIC %md
# MAGIC Now let’s explode the complex type to one row in our result for every value in the array. The DEST_COUNTRY_NAME will duplicate for every value in the array, performing the exact opposite of the original collect and returning us to the original DataFrame

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT explode(collected_counts), DEST_COUNTRY_NAME FROM flights_agg

# COMMAND ----------

# MAGIC %md
# MAGIC **Functions:**
# MAGIC To see a list of functions in Spark SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW FUNCTIONS

# COMMAND ----------

# MAGIC %md
# MAGIC * We can also more specifically indicate whether we would like to see the system functions (i.e., those built into Spark) as well as user functions.
# MAGIC * User functions are those defined by you or someone else sharing your Spark environment.
# MAGIC * We can filter all SHOW commands by passing a string with wildcard (*) characters.
# MAGIC * We can include the LIKE keyword to search the functions

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SYSTEM FUNCTIONS

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW USER FUNCTIONS

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW FUNCTIONS "s*"

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW FUNCTIONS LIKE "collect*"

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FUNCTION collect_list

# COMMAND ----------

# MAGIC %md
# MAGIC **User-defined functions:**
# MAGIC * Spark gives you the ability to define your own functions and use them in a distributed manner.
# MAGIC * We can define functions, writing the function in the language of your choice and then registering it appropriately.
# MAGIC * You can also register functions through the **Hive CREATE TEMPORARY FUNCTION** syntax.

# COMMAND ----------

# MAGIC %scala
# MAGIC def power3(number:Double):
# MAGIC   Double = number * number * number
# MAGIC   
# MAGIC spark.udf.register("power3", power3(_:Double):Double)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count, power3(count) FROM flights

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT dest_country_name FROM flights
# MAGIC GROUP BY dest_country_name ORDER BY sum(count) DESC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC **Setting Configuration Values in SQL:**

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.sql.shuffle.partitions = 20