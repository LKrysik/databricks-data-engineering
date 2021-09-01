# Databricks notebook source
spark.read\
.option("header", "true")\
.csv("/FileStore/tables/online_retail_dataset-92e8e.csv")\
.repartition(2)\
.selectExpr("instr(Description, 'GLASS') >= 1 as is_glass")\
.groupBy("is_glass")\
.count()\
.collect()

# COMMAND ----------



# COMMAND ----------

# gs://e451_training_materials_3c8c/sales_by_div_example_p249063/online-retail-dataset.csv

# COMMAND ----------

myrdd = spark.read\
.option("header", "true")\
.csv("/FileStore/tables/online_retail_dataset-92e8e.csv")\
.repartition(2)

# COMMAND ----------

myrdd.rdd.getNumPartitions()