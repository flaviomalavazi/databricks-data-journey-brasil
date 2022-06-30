# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC # Technical Setup notebook. Hide this cell results
# MAGIC Initialize dataset to the current user and cleanup data when reset_all_data is set to true
# MAGIC 
# MAGIC Do not edit

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
dbutils.widgets.text("db_prefix", "retail", "Database prefix")
dbutils.widgets.text("min_dbr_version", "9.1", "Min required DBR version")

# COMMAND ----------

from delta.tables import *
import pandas as pd
import logging
from pyspark.sql.functions import to_date, col, regexp_extract, rand, to_timestamp, initcap, sha1
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf, PandasUDFType, input_file_name
import re


# VERIFY DATABRICKS VERSION COMPATIBILITY ----------

try:
  min_required_version = dbutils.widgets.get("min_dbr_version")
except:
  min_required_version = "9.1"

version_tag = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
version_search = re.search('^([0-9]*\.[0-9]*)', version_tag)
assert version_search, f"The Databricks version can't be extracted from {version_tag}, shouldn't happen, please correct the regex"
current_version = float(version_search.group(1))
assert float(current_version) >= float(min_required_version), f'The Databricks version of the cluster must be >= {min_required_version}. Current version detected: {current_version}'
assert "ml" in version_tag.lower(), f"The Databricks ML runtime must be used. Current version detected doesn't contain 'ml': {version_tag} "


#python Imports for ML...
from sklearn.compose import ColumnTransformer
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestClassifier
from sklearn import metrics
from sklearn.model_selection import GridSearchCV
import mlflow
import mlflow.sklearn
from mlflow.tracking.client import MlflowClient
from hyperopt import fmin, hp, tpe, STATUS_OK, Trials
from hyperopt.pyll.base import scope
from hyperopt import SparkTrials
from sklearn.model_selection import GroupKFold
from pyspark.sql.functions import pandas_udf, PandasUDFType
import os
import pandas as pd
from hyperopt import space_eval
import numpy as np
from time import sleep


from sklearn.preprocessing import LabelBinarizer, LabelEncoder
from sklearn.metrics import confusion_matrix

#force the experiment to the field demos one. Required to launch as a batch
def init_experiment_for_batch(demo_name, experiment_name):
  #You can programatically get a PAT token with the following
  pat_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
  url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
  #current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
  import requests
  xp_root_path = f"/field-demos/experiments/{demo_name}"
  requests.post(f"{url}/api/2.0/workspace/mkdirs", headers = {"Accept": "application/json", "Authorization": f"Bearer {pat_token}"}, json={ "path": xp_root_path})
  xp = f"{xp_root_path}/{experiment_name}"
  print(f"Using common experiment under {xp}")
  mlflow.set_experiment(xp)
  return mlflow.get_experiment_by_name(xp)

# COMMAND ----------

def get_cloud_name():
  return spark.conf.get("spark.databricks.clusterUsageTags.cloudProvider").lower()

# COMMAND ----------

mount_name = "field-demos"

try:
  dbutils.fs.ls("/mnt/%s" % mount_name)
except:
  workspace_id = dbutils.entry_point.getDbutils().notebook().getContext().workspaceId().get()
  url = dbutils.entry_point.getDbutils().notebook().getContext().apiUrl().get()
  if workspace_id == '8194341531897276':
    print("CSE2 bucket isn't mounted, mount the demo data under %s" % mount_name)
    dbutils.fs.mount(f"s3a://databricks-field-demos/" , f"/mnt/{mount_name}")
  elif "azure" in url:
    print("ADLS2 isn't mounted, mount the demo data under %s" % mount_name)
    configs = {"fs.azure.account.auth.type": "OAuth",
              "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
              "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope = "common-sp", key = "common-sa-sp-client-id"),
              "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope = "common-sp", key = "common-sa-sp-client-secret"),
              "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/9f37a392-f0ae-4280-9796-f1864a10effc/oauth2/token"}

    dbutils.fs.mount(
      source = "abfss://field-demos@fielddemosdatasets.dfs.core.windows.net/field-demos",
      mount_point = "/mnt/"+mount_name,
      extra_configs = configs)
  else:
    aws_bucket_name = ""
    print("bucket isn't mounted, mount the demo bucket under %s" % mount_name)
    dbutils.fs.mount(f"s3a://databricks-datasets-private/field-demos" , f"/mnt/{mount_name}")

# COMMAND ----------

spark.conf.set("spark.databricks.cloudFiles.schemaInference.sampleSize.numFiles", "10")
#spark.conf.set("spark.databricks.cloudFiles.schemaInference.enabled", "true")

current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
if current_user.rfind('@') > 0:
  current_user_no_at = current_user[:current_user.rfind('@')]
else:
  current_user_no_at = current_user
current_user_no_at = re.sub(r'\W+', '_', current_user_no_at)

db_prefix = dbutils.widgets.get("db_prefix")

dbName = db_prefix+"_"+current_user_no_at
cloud_storage_path = f"/Users/{current_user}/field_demos/{db_prefix}"
reset_all = dbutils.widgets.get("reset_all_data") == "true"

if reset_all:
  spark.sql(f"DROP DATABASE IF EXISTS {dbName} CASCADE")
  dbutils.fs.rm(cloud_storage_path, True)

spark.sql(f"""create database if not exists {dbName} LOCATION '{cloud_storage_path}/tables' """)
spark.sql(f"""USE {dbName}""")

print("using cloud_storage_path {}".format(cloud_storage_path))

# COMMAND ----------

def display_slide(slide_id, slide_number):
  displayHTML(f'''
  <div style="width:1150px; margin:auto">
  <iframe
    src="https://docs.google.com/presentation/d/{slide_id}/embed?slide={slide_number}"
    frameborder="0"
    width="1150"
    height="683"
  ></iframe></div>
  ''')

# COMMAND ----------

# Function to stop all streaming queries 
def stop_all_streams():
  stream_count = len(spark.streams.active)
  if stream_count > 0:
    print(f"Stopping {stream_count} streams")
    for s in spark.streams.active:
        try:
            s.stop()
        except:
            pass
    print("All stream stopped.")
    
def wait_for_all_stream():
  import time
  if len(spark.streams.active) > 0:
    print(f"{len(spark.streams.active)} streams still active, waiting...")
  while len(spark.streams.active) > 0:
    spark.streams.awaitAnyTermination()
    time.sleep(1)
  print("All streams completed.")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Helpers for autoML runs

# COMMAND ----------

from pyspark.sql.functions import col
from databricks.feature_store import FeatureStoreClient
import mlflow

import databricks
from databricks import automl
from datetime import datetime

def get_automl_run(name):
  #get the most recent automl run
  df = spark.table("field_demos_metadata.automl_experiment").filter(col("name") == name).orderBy(col("date").desc()).limit(1)
  return df.collect()

#Get the automl run information from the field_demos_metadata.automl_experiment table. 
#If it's not available in the metadata table, start a new run with the given parameters
def get_automl_run_or_start(name, model_name, dataset, target_col, timeout_minutes, move_to_production = False):
  spark.sql("create database if not exists field_demos_metadata")
  spark.sql("create table if not exists field_demos_metadata.automl_experiment (name string, date string)")
  result = get_automl_run(name)
  if len(result) == 0:
    print("No run available, start a new Auto ML run, this will take a few minutes...")
    start_automl_run(name, model_name, dataset, target_col, timeout_minutes, move_to_production)
    result = get_automl_run(name)
  return result[0]


#Start a new auto ml classification task and save it as metadata.
def start_automl_run(name, model_name, dataset, target_col, timeout_minutes = 5, move_to_production = False):
  automl_run = databricks.automl.classify(
    dataset = dataset,
    target_col = target_col,
    timeout_minutes = timeout_minutes
  )
  experiment_id = automl_run.experiment.experiment_id
  path = automl_run.experiment.name
  data_run_id = mlflow.search_runs(experiment_ids=[automl_run.experiment.experiment_id], filter_string = "tags.mlflow.source.name='Notebook: DataExploration'").iloc[0].run_id
  exploration_notebook_id = automl_run.experiment.tags["_databricks_automl.exploration_notebook_id"]
  best_trial_notebook_id = automl_run.experiment.tags["_databricks_automl.best_trial_notebook_id"]

  cols = ["name", "date", "experiment_id", "experiment_path", "data_run_id", "best_trial_run_id", "exploration_notebook_id", "best_trial_notebook_id"]
  spark.createDataFrame(data=[(name, datetime.today().isoformat(), experiment_id, path, data_run_id, automl_run.best_trial.mlflow_run_id, exploration_notebook_id, best_trial_notebook_id)], schema = cols).write.mode("append").option("mergeSchema", "true").saveAsTable("field_demos_metadata.automl_experiment")
  #Create & save the first model version in the MLFlow repo (required to setup hooks etc)
  model_registered = mlflow.register_model(f"runs:/{automl_run.best_trial.mlflow_run_id}/model", model_name)
  if move_to_production:
    client = mlflow.tracking.MlflowClient()
    print("registering model version "+model_registered.version+" as production model")
    client.transition_model_version_stage(name = model_name, version = model_registered.version, stage = "Production", archive_existing_versions=True)
  return get_automl_run(name)

#Generate nice link for the given auto ml run
def display_automl_link(name, model_name, dataset, target_col, timeout_minutes = 5, move_to_production = False):
  r = get_automl_run_or_start(name, model_name, dataset, target_col, timeout_minutes, move_to_production)
  html = f"""For exploratory data analysis, open the <a href="/#notebook/{r["exploration_notebook_id"]}">data exploration notebook</a><br/><br/>"""
  html += f"""To view the best performing model, open the <a href="/#notebook/{r["best_trial_notebook_id"]}">best trial notebook</a><br/><br/>"""
  html += f"""To view details about all trials, navigate to the <a href="/#mlflow/experiments/{r["experiment_id"]}/s?orderByKey=metrics.%60val_f1_score%60&orderByAsc=false">MLflow experiment</>"""
  displayHTML(html)

def reset_automl_run(model_name):
  spark.sql(f"delete from field_demos_metadata.automl_experiment where name='{model_name}'")

# COMMAND ----------

def test_not_empty_folder(folder):
  try:
    return len(dbutils.fs.ls(folder)) > 0
  except:
    return False
