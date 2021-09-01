# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Indirect Performance Enhancements

# COMMAND ----------

# MAGIC %md
# MAGIC ### Design Choices
# MAGIC 
# MAGIC #### Design Choices
# MAGIC 				
# MAGIC 1. Minimize using of UDFs. Of course, we cant avoid UDFs as they become handy when we want to do custom transformations.  When writing code in Python, have the UDF developed in Scala and port that to application which will increase the usability, maintainability and performance.
# MAGIC 
# MAGIC 2. All the structured APIs(DF,DS and SQL) will give the same type of performance. Although structured apis compiles down to RDD, Sparks catalyst optimizer will writter better RDD code than we manually writing it. Convert most of the RDD transformation to Structured transformations. Mainly while using python it not advisible to use RDDs because python serializes lot of data to and from for python process. This can be expensive to run over very big data and can also decrease stability.
# MAGIC 
# MAGIC #### Object Serialization in RDDs
# MAGIC 1. When you’re working with custom data types, you’re going to want to serialize them using Kryo because it’s both more compact and much more efficient than Java serialization. 
# MAGIC 
# MAGIC 2. However, this does come at the inconvenience of registering the classes that you will be using in your application. You can use Kryo serialization by setting spark.serializer to org.apache.spark.serializer.KryoSerializer. You will also need to explicitly register the classes that you would like to register with the Kryo serializer via the "spark.kryo.classesToRegister configuration."
# MAGIC 
# MAGIC 3. To register your classes, use the SparkConf that you just created and pass in the names of your classes: conf.registerKryoClasses(Array(classOf[MyClass1], classOf[MyClass2]))
# MAGIC 
# MAGIC 
# MAGIC #### Cluster Configurations

# COMMAND ----------



# COMMAND ----------

