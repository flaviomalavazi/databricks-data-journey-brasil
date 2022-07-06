# Databricks notebook source
# MAGIC %md
# MAGIC # Preparing our demo 
# MAGIC We need to decompressing our data files and place them in the folder that will be read using the Autoloader as well as priting the parameters needed for our DLT demo Pipeline

# COMMAND ----------

#SKIP_ON_DBC_ARCHIVE
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=$reset_all_data

# COMMAND ----------

# DBTITLE 1,Use the parameters bellow to setup your delta pipeline workflow
DLTH.print_pipeline_config()
