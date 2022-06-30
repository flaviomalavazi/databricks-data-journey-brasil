# Databricks notebook source
# DBTITLE 1,Let's install mlflow to load our model
# MAGIC %pip install mlflow

# COMMAND ----------

# MAGIC %md #Registering python UDF to a SQL function
# MAGIC This is a companion notebook to load the customer_segmentation model as a spark udf and save it as a SQL function
# MAGIC  
# MAGIC Make sure you add this notebook in your DLT job to have access to the `get_customer_segmentation_cluster` function. (Currently mixing python in a SQL DLT notebook won't run the python)
# MAGIC 
# MAGIC <!-- do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Fdlt%2Fnotebook_ml_model&dt=DATA_PIPELINE">
# MAGIC <!-- [metadata={"description":"Load ML model from registry in python to inject it in a DLT SQL pipeline.<br/><i>Usage: not necessary, showcase a mix Python/sql DLT, it's mostly a tech requirement to load ML model in SQL</i>",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{"DLT": ["DLT customer SQL"]},
# MAGIC  "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["autoloader", "copy into"]},
# MAGIC  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

import mlflow
get_cluster_udf = mlflow.pyfunc.spark_udf(spark, "models:/field_demos_customer_segmentation/Production", "string")
spark.udf.register("get_customer_segmentation_cluster", get_cluster_udf)

# COMMAND ----------

# MAGIC %md ### Setting up the DLT 
# MAGIC 
# MAGIC Here is a setting example including the python UDF and the SQL function. Note the multiple entries (1 per notebook) in the "libraries" option:
# MAGIC 
# MAGIC ```
# MAGIC {
# MAGIC     "id": "95f28631-1884-425e-af69-05c3f397dd90",
# MAGIC     "name": "field_demos_retail",
# MAGIC     "storage": "/Users/quentin.ambard@databricks.com/field_demos_retail/dlt",
# MAGIC     "configuration": {
# MAGIC         "pipelines.useV2DetailsPage": "true"
# MAGIC     },
# MAGIC     "clusters": [
# MAGIC         {
# MAGIC             "label": "default",
# MAGIC             "autoscale": {
# MAGIC                 "min_workers": 1,
# MAGIC                 "max_workers": 5
# MAGIC             }
# MAGIC         }
# MAGIC     ],
# MAGIC     "libraries": [
# MAGIC         {
# MAGIC             "notebook": {
# MAGIC                 "path": "/Repos/quentin.ambard@databricks.com/field-demo/demo-retail/02-Delta-Live-Table/02.1bis-SQL-Delta-Live-Table-Python-UDF"
# MAGIC             }
# MAGIC         },
# MAGIC         {
# MAGIC             "notebook": {
# MAGIC                 "path": "/Repos/quentin.ambard@databricks.com/field-demo/demo-retail/02-Delta-Live-Table/02.1-SQL-Delta-Live-Table-Ingestion"
# MAGIC             }
# MAGIC         }
# MAGIC     ],
# MAGIC     "target": "retail_quentin_ambard",
# MAGIC     "continuous": false,
# MAGIC     "development": false
# MAGIC }
# MAGIC ```
