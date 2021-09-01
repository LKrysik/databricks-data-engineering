// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC # Wikipedia Edits Stream Producer
// MAGIC 
// MAGIC This notebook grabs data from a Wikipedia web server and feeds it into Kinesis Stream called `Wikipedia-kinesis-stream`.
// MAGIC 
// MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> This is intended to be run by the instructor only as **demo**.
// MAGIC 
// MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Credentials are setup using cluster-mounted IAM roles. This will only work in a correctly configured instructor workspace.

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.widgets.text("batches", "4")

// COMMAND ----------

// MAGIC %python
// MAGIC batches = int(dbutils.widgets.get("batches"))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Make a kinesis client

// COMMAND ----------

// MAGIC %python
// MAGIC import boto3
// MAGIC 
// MAGIC client = boto3.client('kinesis', region_name='us-west-2')

// COMMAND ----------

// MAGIC %md
// MAGIC ### Make sure you can connect to the stream

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC stream = "Wikipedia-kinesis-stream"
// MAGIC 
// MAGIC response = client.describe_stream(
// MAGIC     StreamName=stream,
// MAGIC )
// MAGIC response

// COMMAND ----------

// MAGIC %md
// MAGIC ### Setup helper function to randomly assign geolocation (absent from real data)

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC import json, random
// MAGIC 
// MAGIC geo_data = [{"city" : "Sydney", "country" : "Australia", "countrycode3" : "AUS", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Sofia", "country" : "Bulgaria", "countrycode3" : "BGR", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Calgary", "country" : "Canada", "countrycode3" : "CAN", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Shantou", "country" : "China", "countrycode3" : "CHN", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Giza", "country" : "Egypt", "countrycode3" : "EGY", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Munich", "country" : "Germany", "countrycode3" : "DEU", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Chennai", "country" : "India", "countrycode3" : "IND", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Jaipur", "country" : "India", "countrycode3" : "IND", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Nagpur", "country" : "India", "countrycode3" : "IND", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Tehran", "country" : "Iran", "countrycode3" : "IRN", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Hiroshima", "country" : "Japan", "countrycode3" : "JPN", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Kuala Lumpur", "country" : "Malaysia", "countrycode3" : "MYS", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Fez", "country" : "Morocco", "countrycode3" : "MAR", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Maputo", "country" : "Mozambique", "countrycode3" : "MOZ", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Mandalay", "country" : "Myanmar", "countrycode3" : "MMR", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Gujranwala", "country" : "Pakistan", "countrycode3" : "PAK", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Manila", "country" : "Philippines", "countrycode3" : "PHL", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Riyadh", "country" : "Saudi Arabia", "countrycode3" : "SAU", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Dakar", "country" : "Senegal", "countrycode3" : "SEN", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Dubai", "country" : "United Arab Emirates", "countrycode3" : "ARE", "StateProvince" : "None", "PostalCode" : "None"},
// MAGIC {"city" : "Fresno", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "California", "PostalCode" : "93650"},
// MAGIC {"city" : "Cincinnati", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Ohio", "PostalCode" : "41073"},
// MAGIC {"city" : "San Diego", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "California", "PostalCode" : "91945"},
// MAGIC {"city" : "Portland", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Oregon", "PostalCode" : "97035"},
// MAGIC {"city" : "Long Beach", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "California", "PostalCode" : "90712"},
// MAGIC {"city" : "San Antonio", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Texas", "PostalCode" : "78006"},
// MAGIC {"city" : "Kansas City", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Missouri", "PostalCode" : "64030"},
// MAGIC {"city" : "Los Angeles", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "California", "PostalCode" : "90001"},
// MAGIC {"city" : "Memphis", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Tennessee", "PostalCode" : "37501"},
// MAGIC {"city" : "Tucson", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Arizona", "PostalCode" : "85641"},
// MAGIC {"city" : "Rochester", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "New York", "PostalCode" : "14602"},
// MAGIC {"city" : "Denver", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Colorado", "PostalCode" : "80014"},
// MAGIC {"city" : "Virginia Beach", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Virginia", "PostalCode" : "23450 "},
// MAGIC {"city" : "Montgomery", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Alabama", "PostalCode" : "36043"},
// MAGIC {"city" : "Plano", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Texas", "PostalCode" : "75023"},
// MAGIC {"city" : "Huntington", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "New York", "PostalCode" : "11721"},
// MAGIC {"city" : "Henderson", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Nevada", "PostalCode" : "89002"},
// MAGIC {"city" : "St. Paul", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Minnesota", "PostalCode" : "55101"},
// MAGIC {"city" : "Birmingham", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Alabama", "PostalCode" : "35005"},
// MAGIC {"city" : "St. Louis", "country" : "United States", "countrycode3" : "USA", "StateProvince" : "Missouri", "PostalCode" : "63101"}];
// MAGIC 
// MAGIC def add_geo_data(event):
// MAGIC     event_data = json.loads(event)
// MAGIC     event_data["geolocation"] = random.choice(geo_data)
// MAGIC     return json.dumps(event_data)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Get recent changes and load batches to Kinesis

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC import requests
// MAGIC 
// MAGIC batch = 0
// MAGIC records = []
// MAGIC i = 0
// MAGIC 
// MAGIC with requests.get('https://stream.wikimedia.org/v2/stream/recentchange', stream=True) as r:
// MAGIC   for line in r.iter_lines():
// MAGIC     temp = line.decode("utf-8")
// MAGIC     if temp[:4] == "data":
// MAGIC       try:
// MAGIC         records.append({
// MAGIC           'Data': add_geo_data(temp[6:]),
// MAGIC           'PartitionKey': 'string'})
// MAGIC       except:
// MAGIC         pass
// MAGIC       i += 1
// MAGIC       if not (i+1) % 500:
// MAGIC         batch += 1
// MAGIC         print("Done fetching {} records.".format(batch * 500))
// MAGIC         client.put_records(
// MAGIC           Records=records,
// MAGIC           StreamName=stream)
// MAGIC         records = []
// MAGIC       if batch == batches:
// MAGIC         print("Done fetching {} batches.".format(batches))
// MAGIC         break

// COMMAND ----------

// MAGIC %python
// MAGIC displayHTML("All done!")