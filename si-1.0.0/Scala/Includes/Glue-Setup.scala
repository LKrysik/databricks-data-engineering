// Databricks notebook source
// MAGIC 
// MAGIC %run "./Course-Name"

// COMMAND ----------

// MAGIC %run "./Dataset-Mounts"

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC dbDF = spark.sql("DROP DATABASE IF EXISTS glue_db CASCADE")
// MAGIC   
// MAGIC displayHTML("""<b style="color:green">Resetting Glue databases</b>.""")

// COMMAND ----------

// MAGIC %python
// MAGIC displayHTML("All done!")