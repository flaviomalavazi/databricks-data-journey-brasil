# Databricks notebook source
#SKIP_ON_DBC_ARCHIVE
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Image manipulation with Databricks & Delta Lake
# MAGIC Product images can greatly improve our product classifcation. But first we need to collect and prepare the data containing images
# MAGIC 
# MAGIC <!-- do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Fml%2Fproduct_classification%2Fml_product_classification_01&dt=ML">
# MAGIC <!-- [metadata={"description":"Ingest images to build a Delta table.<br/><i>Usage: demo image ingestion with autoloader.</i>",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Data Science", "components": ["autoloader", "image"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# MAGIC %run ../../_resources/00-setup $reset_all_data=$reset_all_data

# COMMAND ----------

# DBTITLE 1,Our images are stored as individual files in our blob storage
# MAGIC %fs ls /mnt/field-demos/retail/product_raw_images

# COMMAND ----------

# MAGIC %md Databricks can easily read these files, however this isn't efficient: it's a lot of small files to open (note how slow it is):

# COMMAND ----------

spark.read.format("binaryFile").option("pathGlobFilter", "*.jpg").load("/mnt/field-demos/retail/product_raw_images").display()

# COMMAND ----------

# MAGIC %md ## Delta & Auto-loader to the rescue
# MAGIC Databricks Autoloader provides native support for images and binary files.
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/autoloader-images.png" width="800" />
# MAGIC 
# MAGIC We'll also extract the image ID (from the name) to be able to join them with our list of products

# COMMAND ----------

spark.readStream \
         .format("cloudFiles")\
         .option("pathGlobFilter", "*.jpg") \
         .option("cloudFiles.format", "binaryFile")\
       .load("/mnt/field-demos/retail/product_raw_images") \
         .withColumn("id", regexp_extract('path', '\/([0-9]*)\.jpg', 1)) \
      .writeStream \
        .option("checkpointLocation", cloud_storage_path+"/checkpoint_image") \
        .trigger(once = True) \
        .table("product_images")

# COMMAND ----------

# MAGIC %sql select * from product_images
