# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Serving MLflow Models via Amazon SageMaker
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC 
# MAGIC By the end of the lesson, you should be able to:
# MAGIC * Deploy a model registered in MLflow to Sagemaker
# MAGIC * Query the deployed model
# MAGIC * Demonstrate behavior during model updates
# MAGIC 
# MAGIC Databricks DBR 5.5+ comes pre-loaded with MLflow for tracking data science experiments and models. The ML Runtime on Databricks includes additional libraries that make it easy for data scientists to produce scalable models ready for deployment.
# MAGIC 
# MAGIC We won't be focusing on model training today, but rather the ability to easily deploy MLflow models to SageMaker for real-time analytics.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Configuration Notes
# MAGIC 
# MAGIC In addition to configurations in Databricks and the AWS console, this demo requires that a Docker container be deployed by MLflow to ECR. The following links provide instructions for how to complete the requisite steps.
# MAGIC 
# MAGIC - [Set up AWS Authentication for SageMaker Deployment](https://docs.databricks.com/administration-guide/cloud-configurations/aws/sagemaker.html)
# MAGIC - [Install MLflow on your computer](https://mlflow.org/docs/latest/quickstart.html#installing-mlflow)
# MAGIC - [Install the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html)
# MAGIC - [Configure the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html)
# MAGIC - [Grant User Access to ECR](https://docs.aws.amazon.com/AmazonECR/latest/userguide/RepositoryPolicyExamples.html)
# MAGIC - [Build and Push pyfunc Container](https://mlflow.org/docs/latest/cli.html#mlflow-sagemaker-build-and-push-container)

# COMMAND ----------

# MAGIC %run "./Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports
# MAGIC 
# MAGIC This notebook requires a number of imported python libraries, and reuses a number of variables. We import and define these in the cell below.

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC import os
# MAGIC import pandas as pd
# MAGIC import numpy as np
# MAGIC import boto3
# MAGIC import json
# MAGIC 
# MAGIC app_name = "model-wine"
# MAGIC model_path = "mlflow-wine"
# MAGIC wine_data_path = "/mnt/training/wine.parquet"

# COMMAND ----------

# MAGIC %md
# MAGIC ## SageMaker Hosting Services
# MAGIC 
# MAGIC Amazon SageMaker provides hosting services for model deployment, which allow real-time querying for inference using simple HTTPS endpoints.
# MAGIC 
# MAGIC More information is available [here](https://docs.aws.amazon.com/sagemaker/latest/dg/how-it-works-hosting.html).
# MAGIC 
# MAGIC We've already configured an endpoint that's currently in service.

# COMMAND ----------

# MAGIC %md
# MAGIC Check the status of the SageMaker endpoint by running the following cell.
# MAGIC 
# MAGIC **Note**: The application status should be **InService**. 

# COMMAND ----------

# MAGIC %python
# MAGIC def check_status(app_name):
# MAGIC   sage_client = boto3.client('sagemaker', region_name="us-west-2")
# MAGIC   endpoint_description = sage_client.describe_endpoint(EndpointName=app_name)
# MAGIC   endpoint_status = endpoint_description["EndpointStatus"]
# MAGIC   return endpoint_status
# MAGIC 
# MAGIC print("Application status is: {}".format(check_status(app_name)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Querying the deployed model
# MAGIC 
# MAGIC Once a model is deployed, it's simple to get a result from new data.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load a Sample Input Vector from the Wine Dataset

# COMMAND ----------

# MAGIC %python
# MAGIC data = (spark.read
# MAGIC       .format("parquet")
# MAGIC       .load(wine_data_path).toPandas())
# MAGIC sample = data.drop(["quality"], axis=1).iloc[[0]]
# MAGIC query_input = sample.values.tolist()[0]
# MAGIC 
# MAGIC print("Using input vector: {}".format(query_input))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> We'll be passing our data as a JSON using the `pandas-split` format.

# COMMAND ----------

# MAGIC %python
# MAGIC input_json = pd.DataFrame([query_input]).to_json(orient='split')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Evaluate the Input Vector by Sending an HTTP Request
# MAGIC Query the SageMaker endpoint's REST API using the ``sagemaker-runtime`` API provided in boto3. 

# COMMAND ----------

# MAGIC %python
# MAGIC def query_endpoint_example(app_name, input_json):
# MAGIC   print("Sending batch prediction request with inputs: {}".format(input_json))
# MAGIC   client = boto3.session.Session().client("sagemaker-runtime", "us-west-2")
# MAGIC   
# MAGIC   response = client.invoke_endpoint(
# MAGIC       EndpointName=app_name,
# MAGIC       Body=input_json,
# MAGIC       ContentType='application/json; format=pandas-split',
# MAGIC   )
# MAGIC   preds = response['Body'].read().decode("ascii")
# MAGIC   preds = json.loads(preds)
# MAGIC   print("Received response: {}".format(preds))
# MAGIC   return preds
# MAGIC 
# MAGIC prediction1 = query_endpoint_example(app_name=app_name, input_json=input_json)

# COMMAND ----------

# MAGIC %md
# MAGIC Now we'll train a new model, deploy it, and get another prediction.

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
# MAGIC ### Training a model

# COMMAND ----------

# MAGIC %python
# MAGIC import random
# MAGIC 
# MAGIC alpha = random.random()
# MAGIC l1_ratio = random.random()
# MAGIC 
# MAGIC run_id = dbutils.notebook.run("./Runnable/MLflow-Model", 0, {"alpha": alpha, "l1_ratio": l1_ratio, "model_path": model_path})
# MAGIC 
# MAGIC print(run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC #### The mlflow-pyfunc Docker image
# MAGIC 
# MAGIC During deployment, MLflow will use a specialized Docker container with resources required to load and serve the model. This container is named `mlflow-pyfunc`.
# MAGIC 
# MAGIC By default, MLflow will search for this container within your AWS Elastic Container Registry (ECR). You can build and upload this container to ECR using the
# MAGIC `mlflow.sagemaker.build_image()` function in MLflow.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deploy a Model
# MAGIC Use MLflow's SageMaker API to deploy your trained model to SageMaker. The `mlflow.sagemaker.deploy()` function creates a SageMaker endpoint as well as all intermediate SageMaker objects required for the endpoint.
# MAGIC 
# MAGIC **Note**: You must create a new SageMaker endpoint for each new region.

# COMMAND ----------

# MAGIC %python
# MAGIC import mlflow.sagemaker as mfs
# MAGIC 
# MAGIC model_uri = "runs:/{}/{}".format(run_id, model_path)
# MAGIC 
# MAGIC mfs.deploy(app_name=app_name, model_uri=model_uri, region_name="us-west-2", mode="replace")

# COMMAND ----------

# MAGIC %md
# MAGIC The above cell will take around 8 minutes to complete. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query the updated model
# MAGIC You should get a different prediction.

# COMMAND ----------

# MAGIC %python
# MAGIC prediction2 = query_endpoint_example(app_name=app_name, input_json=input_json) 

# COMMAND ----------

# MAGIC %md
# MAGIC Compare the predictions.

# COMMAND ----------

# MAGIC %python
# MAGIC print("Previous model prediction: {}".format(prediction1)) 
# MAGIC print("New model prediction: {}".format(prediction2))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Cleaning up the deployment
# MAGIC 
# MAGIC When your model deployment is no longer needed, run the `mlflow.sagemaker.delete()` function to delete it.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> This is commented out, as we currently leave the endpoint up so we can query it during future deliveries of this course.

# COMMAND ----------

# MAGIC %python
# MAGIC # Specify the archive=False option to delete any SageMaker models and configurations
# MAGIC # associated with the specified application
# MAGIC 
# MAGIC # mfs.delete(app_name=app_name, region_name="us-west-2", archive=False)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The following cell can be run to confirm which endpoints are still up and running for a given application.

# COMMAND ----------

# MAGIC %python
# MAGIC def get_active_endpoints(app_name):
# MAGIC   sage_client = boto3.client('sagemaker', region_name="us-west-2")
# MAGIC   app_endpoints = sage_client.list_endpoints(NameContains=app_name)["Endpoints"]
# MAGIC   return list(filter(lambda en : en == app_name, [str(endpoint["EndpointName"]) for endpoint in app_endpoints]))
# MAGIC   
# MAGIC print("The following endpoints exist for the `{an}` application: {eps}".format(an=app_name, eps=get_active_endpoints(app_name)))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>