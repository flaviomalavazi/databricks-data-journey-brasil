# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ../../_resources/00-global-setup $reset_all_data=$reset_all_data $db_prefix=retail

# COMMAND ----------

username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

csv_directory = f"""/user/{username}/demo-retail/_data/spend_csv/"""

(spark.readStream
        .format("cloudFiles") 
        .option("cloudFiles.format", "csv") 
        .option("cloudFiles.schemaHints", "age int, annual_income int, spending_core int") #schema subset for evolution / new field
        .option("cloudFiles.maxFilesPerTrigger", "10") 
        .option("cloudFiles.schemaLocation", cloud_storage_path+"/schema_spend") #Autoloader will automatically infer all the schema & evolution
        .load(f"{csv_directory}")
      .withColumn("id", col("id").cast("int"))
      .writeStream
        .trigger(once=True)
        .option("checkpointLocation", cloud_storage_path+"/checkpoint_spend")
        .table("spend_silver"))

# COMMAND ----------

# MAGIC %md
# MAGIC Data successfully initialized
