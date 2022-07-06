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
# #                                                                                         Stage/version    output
# #                                                                 Model name                     |            |
# #                                                                     |                          |            |
get_cluster_udf = mlflow.pyfunc.spark_udf(spark, "models:/demos_retail_customer_segmentation/Production", "string")
spark.udf.register("get_customer_segmentation_cluster", get_cluster_udf)
