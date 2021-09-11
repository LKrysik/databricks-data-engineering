# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC # Exercise #6 - Business Questions
# MAGIC 
# MAGIC In our last exercise, we are going to execute various joins across our four tables (**`orders`**, **`line_items`**, **`sales_reps`** and **`products`**) to answer basic business questions
# MAGIC 
# MAGIC This exercise is broken up into 3 steps:
# MAGIC * Exercise 6.A - Use Database
# MAGIC * Exercise 6.B - Question #1
# MAGIC * Exercise 6.C - Question #2
# MAGIC * Exercise 6.D - Question #3

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Setup Exercise #6</h2>
# MAGIC 
# MAGIC To get started, we first need to configure your Registration ID and then run the setup notebook.

# COMMAND ----------

# MAGIC %md ### Setup - Registration ID
# MAGIC 
# MAGIC In the next commmand, please update the variable **`registration_id`** with the Registration ID you received when you signed up for this project.
# MAGIC 
# MAGIC For more information, see [Registration ID]($./Registration ID)

# COMMAND ----------

registration_id = "1898866"

# COMMAND ----------

# MAGIC %md ### Setup - Run the exercise setup
# MAGIC 
# MAGIC Run the following cell to setup this exercise, declaring exercise-specific variables and functions.

# COMMAND ----------

# MAGIC %run ./_includes/Setup-Exercise-06

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #6.A - Use Database</h2>
# MAGIC 
# MAGIC Each notebook uses a different Spark session and will initially use the **`default`** database.
# MAGIC 
# MAGIC As in the previous exercise, we can avoid contention to commonly named tables by using our user-specific database.
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC * Use the database identified by the variable **`user_db`** so that any tables created in this notebook are **NOT** added to the **`default`** database

# COMMAND ----------

# MAGIC %md ### Implement Exercise #6.A
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS dbacademy_txu_guidehouse_com_db; 
# MAGIC USE dbacademy_txu_guidehouse_com_db;

# COMMAND ----------

# MAGIC %sql
# MAGIC SET user_db = dbacademy_txu_guidehouse_com_db

# COMMAND ----------

# MAGIC %md ### Reality Check #6.A
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_06_a()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #6.B - Question #1</h2>
# MAGIC ## How many orders were shipped to each state?
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC * Aggregate the orders by **`shipping_address_state`**
# MAGIC * Count the number of records for each state
# MAGIC * Sort the results by **`count`**, in descending order
# MAGIC * Save the results to the temporary view **`question_1_results`**, identified by the variable **`question_1_results_table`**

# COMMAND ----------

# MAGIC %md ### Implement Exercise #6.B
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# TODO
from pyspark.sql.functions import count, col, desc
# orders_df = sqlContext.sql('select * from orders')
orders_df = spark.read.table(orders_table)
question_1_results_table = "question_1_results"
orders_df.groupBy("shipping_address_state").count().sort(desc("count")).createTempView(question_1_results_table)

# COMMAND ----------

# MAGIC %md ### Reality Check #6.B
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_06_b()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #6.C - Question #2</h2>
# MAGIC ## What is the average, minimum and maximum sale price for green products sold to North Carolina where the Sales Rep submitted an invalid Social Security Number (SSN)?
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC * Execute a join across all four tables:
# MAGIC   * **`orders`**, identified by the variable **`orders_table`**
# MAGIC   * **`line_items`**, identified by the variable **`line_items_table`**
# MAGIC   * **`products`**, identified by the variable **`products_table`**
# MAGIC   * **`sales_reps`**, identified by the variable **`sales_reps_table`**
# MAGIC * Limit the result to only green products (**`color`**).
# MAGIC * Limit the result to orders shipped to North Carolina (**`shipping_address_state`**)
# MAGIC * Limit the result to sales reps that initially submitted an improperly formatted SSN (**`_error_ssn_format`**)
# MAGIC * Calculate the average, minimum and maximum of **`product_sold_price`** - do not rename these columns after computing.
# MAGIC * Save the results to the temporary view **`question_2_results`**, identified by the variable **`question_2_results_table`**
# MAGIC * The temporary view should have the following three columns: **`avg(product_sold_price)`**, **`min(product_sold_price)`**, **`max(product_sold_price)`**
# MAGIC * Collect the results to the driver
# MAGIC * Assign to the following local variables, the average, minimum, and maximum values - these variables will be passed to the reality check for validation.
# MAGIC  * **`ex_avg`** - the local variable holding the average value
# MAGIC  * **`ex_min`** - the local variable holding the minimum value
# MAGIC  * **`ex_max`** - the local variable holding the maximum value

# COMMAND ----------

# MAGIC %md ### Implement Exercise #6.C
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# TODO
#line_items_df = sqlContext.sql('select * from line_items')
line_items_df = spark.read.table(line_items_table)
products_df = sqlContext.sql('select * from products')
sales_reps_df = sqlContext.sql('select * from sales_reps')
join_df = orders_df.join(line_items_df, line_items_df["order_id"]==orders_df["order_id"]).filter(orders_df["shipping_address_state"]=="NC").join(products_df, products_df["product_id"]==line_items_df["product_id"]).filter(products_df["color"]=="green").join(sales_reps_df, sales_reps_df["sales_rep_id"]==orders_df["sales_rep_id"]).filter(sales_reps_df["_error_ssn_format"]==True)

from pyspark.sql.functions import avg, min, max
join_df1 = join_df.select(avg('product_sold_price'), min('product_sold_price'), max('product_sold_price'))
display(join_df1)
join_df1.createOrReplaceTempView("question_2_results")
question_2_results_table = "question_2_results"
spark.sql('CREATE TEMP VIEW question_2_results_table AS SELECT * FROM question_2_results')


# COMMAND ----------

ex_avg = join_df1.first()[0] # FILL_IN
ex_min = join_df1.first()[1] # FILL_IN
ex_max = join_df1.first()[2] # FILL_IN

# COMMAND ----------

# MAGIC %md ### Reality Check #6.C
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_06_c(ex_avg, ex_min, ex_max)

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #6.D - Question #3</h2>
# MAGIC ## What is the first and last name of the top earning sales rep based on net sales?
# MAGIC 
# MAGIC For this scenario...
# MAGIC * The top earning sales rep will be identified as the individual producing the largest profit.
# MAGIC * Profit is defined as the difference between **`product_sold_price`** and **`price`** which is then<br/>
# MAGIC   multiplied by **`product_quantity`** as seen in **(product_sold_price - price) * product_quantity**
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC * Execute a join across all four tables:
# MAGIC   * **`orders`**, identified by the variable **`orders_table`**
# MAGIC   * **`line_items`**, identified by the variable **`line_items_table`**
# MAGIC   * **`products`**, identified by the variable **`products_table`**
# MAGIC   * **`sales_reps`**, identified by the variable **`sales_reps_table`**
# MAGIC * Calculate the profit for each line item of an order as described above.
# MAGIC * Aggregate the results by the sales reps' first &amp; last name and then sum each reps' total profit.
# MAGIC * Reduce the dataset to a single row for the sales rep with the largest profit.
# MAGIC * Save the results to the temporary view **`question_3_results`**, identified by the variable **`question_3_results_table`**
# MAGIC * The temporary view should have the following three columns: 
# MAGIC   * **`sales_rep_first_name`** - the first column by which to aggregate by
# MAGIC   * **`sales_rep_last_name`** - the second column by which to aggregate by
# MAGIC   * **`sum(total_profit)`** - the summation of the column **`total_profit`**

# COMMAND ----------

# MAGIC %md ### Implement Exercise #6.D
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# TODO
best_sales_rep_df = orders_df.join(line_items_df, line_items_df["order_id"]==orders_df["order_id"]).join(products_df, products_df["product_id"]==line_items_df["product_id"]).join(sales_reps_df, sales_reps_df["sales_rep_id"]==orders_df["sales_rep_id"])
best_sales_rep_df = best_sales_rep_df.withColumn("total_profit", (col("product_sold_price")-col("price"))*col("product_quantity"))

# COMMAND ----------

total_profit = best_sales_rep_df.groupBy("sales_rep_first_name", "sales_rep_last_name").sum("total_profit")
display(total_profit)

# COMMAND ----------

one_row = total_profit.agg({"sum(total_profit)": "max"}).take(1)
display(one_row)

# COMMAND ----------

final_df = total_profit.where(col('sum(total_profit)') == total_profit.agg({"sum(total_profit)": "max"}).collect()[0][0])

# COMMAND ----------

final_df.createOrReplaceTempView("question_3_results")
question_3_results_table = "question_3_results"
spark.sql('CREATE TEMP VIEW question_3_results_table AS SELECT * FROM question_3_results')

# COMMAND ----------

# MAGIC %md ### Reality Check #6.D
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_06_d()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #6 - Final Check</h2>
# MAGIC 
# MAGIC Run the following command to make sure this exercise is complete:

# COMMAND ----------

reality_check_06_final()