# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # MLflow model

# COMMAND ----------

# MAGIC %md
# MAGIC This notebook trains an MLflow model to demo deployment to Sagemaker.
# MAGIC 
# MAGIC #### Training a model
# MAGIC * Train an sklearn ElasticNet regressor for rating wine qualities

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Tracking Experiments with MLflow
# MAGIC 
# MAGIC Over the course of the machine learning lifecycle, data scientists test many different models from various libraries with different hyperparemeters.  Tracking these various results poses an organizational challenge.  In brief, storing experiements, results, models, supplementary artifacts, and code creates significant challenges in the machine learning lifecycle.
# MAGIC 
# MAGIC MLflow Tracking is a logging API specific for machine learning and agnostic to libraries and environments that do the training.  It is organized around the concept of **runs**, which are executions of data science code.  Runs are aggregated into **experiments** where many runs can be a part of a given experiment and an MLflow server can host many experiments.
# MAGIC 
# MAGIC Each run can record the following information:<br><br>
# MAGIC 
# MAGIC - **Parameters:** Key-value pairs of input parameters such as the number of trees in a random forest model
# MAGIC - **Metrics:** Evaluation metrics such as RMSE or Area Under the ROC Curve
# MAGIC - **Artifacts:** Arbitrary output files in any format.  This can include images, pickled models, and data files
# MAGIC - **Source:** The code that originally ran the experiement
# MAGIC 
# MAGIC MLflow tracking also serves as a **model registry** so tracked models can easily be stored and, as necessary, deployed into production.
# MAGIC 
# MAGIC Experiments can be tracked using libraries in Python, R, and Java as well as by using the CLI and REST calls.  This course will use Python, though the majority of MLflow funcionality is also exposed in these other APIs.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/mlflow-tracking.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md
# MAGIC The following widgets allow us to pass variables when running this notebook.

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.widgets.text("alpha", "1.0")
# MAGIC dbutils.widgets.text("l1_ratio", "0.5")
# MAGIC dbutils.widgets.text("model_path", "mlflow-wine")
# MAGIC alpha = float(dbutils.widgets.get("alpha"))
# MAGIC l1_ratio = float(dbutils.widgets.get("l1_ratio"))
# MAGIC model_path = dbutils.widgets.get("model_path")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Here we import necessary modules and define our data source.

# COMMAND ----------

# MAGIC %python
# MAGIC import warnings
# MAGIC 
# MAGIC import pandas as pd
# MAGIC import numpy as np
# MAGIC from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
# MAGIC from sklearn.model_selection import train_test_split
# MAGIC from sklearn.linear_model import ElasticNet
# MAGIC 
# MAGIC import mlflow
# MAGIC import mlflow.sklearn
# MAGIC 
# MAGIC #print the MLflow version
# MAGIC print("Using MLflow version: " + mlflow.__version__)
# MAGIC 
# MAGIC #set the path of the file name for the expirement
# MAGIC wine_data_path = "/mnt/training/wine.parquet"

# COMMAND ----------

# MAGIC %md
# MAGIC Here we define the functions we'll use to train our model.

# COMMAND ----------

# MAGIC %python
# MAGIC def eval_metrics(actual, pred):
# MAGIC     rmse = np.sqrt(mean_squared_error(actual, pred))
# MAGIC     mae = mean_absolute_error(actual, pred)
# MAGIC     r2 = r2_score(actual, pred)
# MAGIC     return rmse, mae, r2
# MAGIC 
# MAGIC 
# MAGIC def train_model(wine_data_path, model_path, alpha, l1_ratio):
# MAGIC     warnings.filterwarnings("ignore")
# MAGIC     np.random.seed(40)
# MAGIC 
# MAGIC     # Read the wine-quality parquet file (make sure you're running this from the root of MLflow!)
# MAGIC     data = (spark.read
# MAGIC       .format("parquet")
# MAGIC       .load(wine_data_path).toPandas())
# MAGIC 
# MAGIC     # Split the data into training and test sets. (0.75, 0.25) split.
# MAGIC     train, test = train_test_split(data)
# MAGIC 
# MAGIC     # The predicted column is "quality" which is a scalar from [3, 9]
# MAGIC     train_x = train.drop(["quality"], axis=1)
# MAGIC     test_x = test.drop(["quality"], axis=1)
# MAGIC     train_y = train[["quality"]]
# MAGIC     test_y = test[["quality"]]
# MAGIC 
# MAGIC     with mlflow.start_run():
# MAGIC         lr = ElasticNet(alpha=alpha, l1_ratio=l1_ratio, random_state=42)
# MAGIC         lr.fit(train_x, train_y)
# MAGIC 
# MAGIC         predicted_qualities = lr.predict(test_x)
# MAGIC 
# MAGIC         (rmse, mae, r2) = eval_metrics(test_y, predicted_qualities)
# MAGIC 
# MAGIC         print("Elasticnet model (alpha=%f, l1_ratio=%f):" % (alpha, l1_ratio))
# MAGIC         print("  RMSE: %s" % rmse)
# MAGIC         print("  MAE: %s" % mae)
# MAGIC         print("  R2: %s" % r2)
# MAGIC 
# MAGIC         mlflow.log_param("alpha", alpha)
# MAGIC         mlflow.log_param("l1_ratio", l1_ratio)
# MAGIC         mlflow.log_metric("rmse", rmse)
# MAGIC         mlflow.log_metric("r2", r2)
# MAGIC         mlflow.log_metric("mae", mae)
# MAGIC 
# MAGIC         mlflow.sklearn.log_model(lr, model_path)
# MAGIC         
# MAGIC         return mlflow.active_run().info.run_uuid

# COMMAND ----------

# MAGIC %md
# MAGIC Now each model will be registered alongside its params and metrics.

# COMMAND ----------

# MAGIC %python
# MAGIC run_id = train_model(wine_data_path=wine_data_path, model_path=model_path, alpha=alpha, l1_ratio=l1_ratio)

# COMMAND ----------

# MAGIC %md
# MAGIC We return the run ID to the calling notebook so we can use it programmatically to interact with our trained model.

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.notebook.exit(run_id)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>