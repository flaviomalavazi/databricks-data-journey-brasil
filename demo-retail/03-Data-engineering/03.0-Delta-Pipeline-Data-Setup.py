# Databricks notebook source
import os

# COMMAND ----------

print()
/Workspace/Repos/flavio.malavazi@databricks.com/databricks-data-journey-brasil/demo-retail/_data/users_json.tar.gz

# COMMAND ----------

from os import listdir

# COMMAND ----------

username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

# COMMAND ----------


# importing the "tarfile" module
import tarfile
  
# open file
file = tarfile.open(f"""/Workspace{dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("demo-retail")[0]}demo-retail/_data/users_json.tar.gz""")

# extracting file
file.extractall(f"""/dbfs/user/{username}/demo-retail/_data/users_json/""")

file.close()

# COMMAND ----------

listdir(f"""/dbfs/user/{username}/demo-retail/_data/users_json/""")
