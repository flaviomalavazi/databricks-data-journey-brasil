# Databricks notebook source
# In order to erase the tables and reset your workspace, change the widget to true and execute the cell 3
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %md
# MAGIC # Build a Lakehouse to deliver value to your business lines
# MAGIC 
# MAGIC <!-- do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Fnotebook_data_pipeline_full&dt=DATA_PIPELINE">
# MAGIC <!-- [metadata={"description":"Advanced Data Engineering pipeline to prepare the data with Delta Lake. SQL, Python, stream. BRONZE/SILVER/GOLD. Join the 2 flow. <br/><i>Use this notebook for Data engineering.</i>.",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{"Dashboards": ["Global Sales Report"],
# MAGIC                  "Queries": ["Customer Satisfaction Evolution"],
# MAGIC                  "DLT": ["DLT customer SQL"]},
# MAGIC          "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["autoloader", "copy into"]},
# MAGIC                       "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=$reset_all_data

# COMMAND ----------

# MAGIC %md-sandbox ## Preparing the data with Delta Lake
# MAGIC To deliver final outcomes such as segmentation, product classification or forecasting, we need to gather, process and clean the incoming data.
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-pipeline.png" style="height: 400px"/>
# MAGIC 
# MAGIC 
# MAGIC This can be challenging with traditional systems due to the following:
# MAGIC  * Data quality issue
# MAGIC  * Running concurrent operation
# MAGIC  * Running DELETE/UPDATE/MERGE over files
# MAGIC  * Governance & schema evolution
# MAGIC  * Performance ingesting millions of small files on cloud buckets
# MAGIC  * Processing & analysing unstructured data (image, video...)
# MAGIC  * Switching between batch or streaming depending of your requirement...
# MAGIC 
# MAGIC ## Solving these challenges with Delta Lake
# MAGIC 
# MAGIC <div style="float:left">
# MAGIC 
# MAGIC **What's Delta Lake? It's a new OSS standard to bring SQL Transactional database capabilities on top of parquet files!**
# MAGIC 
# MAGIC Used as a new Spark format, built on top of Spark API / SQL
# MAGIC 
# MAGIC * **ACID transactions** (Multiple writers can simultaneously modify a data set)
# MAGIC * **Full DML support** (UPDATE/DELETE/MERGE)
# MAGIC * **BATCH and STREAMING** support
# MAGIC * **Data quality** (expectatiosn, Schema Enforcement, Inference and Evolution)
# MAGIC * **TIME TRAVEL** (Look back on how data looked like in the past)
# MAGIC * **Performance boost** with ZOrder, data skipping and Caching, solves small files issue 
# MAGIC </div>
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo.png" style="height: 200px"/>
# MAGIC 
# MAGIC <br style="clear: both"/>
# MAGIC 
# MAGIC **For more information:** https://delta.io

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 1/ Explore the dataset
# MAGIC 
# MAGIC We'll be using 2 flow of data for this example:
# MAGIC  * Customer stream (`customer id`, `email`, `firstname`, `lastname`...), delivered as JSON file in a blob storage
# MAGIC  * Customer spend stream (`customer id`, `spend`, `age`), delivered as CSV files

# COMMAND ----------

# DBTITLE 1,This is the data being delivered in our cloud storage. Let's explore the raw json files
dbutils.fs.ls(f"""{json_directory}/""")

# COMMAND ----------

# -- As you can see, we have lot of small json files. Let's run a SQL query to explore the data0
spark.sql(f"""select * from json.`{json_directory}`""").limit(100).display()

# COMMAND ----------

# DBTITLE 1,Databricks can analyse your data in a single line of code
dbutils.data.summarize(spark.read.format("json").load(f"{json_directory}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 2/ Bronze: loading data from blob storage

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Simple, resilient & scalable data loading with Databricks AutoLoader
# MAGIC <div style="float:right">
# MAGIC   <img width="400px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-ingestion-step1.png"/>
# MAGIC </div>
# MAGIC Having to ingest a folders containing many small files can be slow and inefficient. Schema changes & inference can be a real challenge. 
# MAGIC 
# MAGIC Databricks solves this using the [AutoLoader](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html) (`cloudFiles` format). 
# MAGIC 
# MAGIC The autoloader will efficiently scan a cloud storage folder and subfolders and ingest new files between each run. It can be use in near real time streaming, or in a bach mode, running very X hours and ingesting the new data only.

# COMMAND ----------

# DBTITLE 1,We'll store the raw data in a USER_BRONZE DELTA table, supporting schema evolution and incorrect data
# MAGIC %sql
# MAGIC -- Note: tables are automatically created during  .writeStream.table("user_bronze") operation, but we can also use plain SQL to create them:
# MAGIC CREATE TABLE IF NOT EXISTS user_bronze (
# MAGIC      id                 STRING NOT NULL,
# MAGIC      email              STRING,
# MAGIC      firstname          STRING,
# MAGIC      lastname           STRING,
# MAGIC      address            STRING,
# MAGIC      city               STRING,
# MAGIC      creation_date      STRING,
# MAGIC      last_activity_date STRING,
# MAGIC      last_ip            STRING,
# MAGIC      postcode           STRING
# MAGIC   ) using delta tblproperties (
# MAGIC      delta.autooptimize.optimizewrite = TRUE,
# MAGIC      delta.autooptimize.autocompact   = TRUE ); 
# MAGIC -- With these 2 last options, Databricks engine will solve small files & optimize write out of the box!

# COMMAND ----------

# DBTITLE 1,Stream api with Autoloader allows incremental data loading
bronze_products = (spark.readStream
                            .format("cloudFiles")
                            .option("cloudFiles.format", "json")
                            .option("cloudFiles.maxFilesPerTrigger", "1")  #demo only, remove in real stream
                            .option("cloudFiles.schemaLocation", cloud_storage_path+"/schema_bronze") #Autoloader will automatically infer all the schema & evolution
                            .load(f"{json_directory}"))

(bronze_products.writeStream
                  .option("checkpointLocation", cloud_storage_path+"/checkpoint_bronze") #exactly once delivery on Delta tables over restart/kill
                  .option("mergeSchema", "true") #merge any new column dynamically
                  .table("user_bronze"))

# COMMAND ----------

# DBTITLE 1,Our user_bronze Delta table is now ready for efficient query
# MAGIC %sql 
# MAGIC -- Note the "_rescued_data" column. If we receive wrong data not matching existing schema, it'll be stored here
# MAGIC select * from user_bronze

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 2/ Silver data: anonimized table, date cleaned
# MAGIC 
# MAGIC <img width="400px" style="float:right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-ingestion-step2.png"/>
# MAGIC 
# MAGIC We can chain these incremental transformation between tables, consuming only new data.
# MAGIC 
# MAGIC This can be triggered in near realtime, or in batch fashion, for example as a job running every night to consume daily data.

# COMMAND ----------

(spark.readStream 
        .table("user_bronze")
          .withColumn("email", sha1(col("email")))
          .withColumn("firstname", initcap(col("firstname")))
          .withColumn("lastname", initcap(col("lastname")))
          .withColumn("creation_date", to_timestamp(col("creation_date"), "MM-dd-yyyy HH:mm:ss"))
          .withColumn("last_activity_date", to_timestamp(col("last_activity_date"), "MM-dd-yyyy HH:mm:ss"))
          .drop(col("_rescued_data"))
     .writeStream
        .option("checkpointLocation", cloud_storage_path+"/checkpoint_silver")
        .table("user_silver"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 3/ Ingest Customer Spend data
# MAGIC 
# MAGIC <img style="float:right" width="400px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-ingestion-step3.png"/>
# MAGIC 
# MAGIC Our customer spend information is delivered by the finance team on another stream in csv file. Let's ingest this data using the auto-loader as previously: 

# COMMAND ----------

# DBTITLE 1,Loading user spending score
# This could be written in SQL with a COPY INTO spend_silver FROM xxx FILEFORMAT = CSV
silver_spend = (spark.readStream
        .format("cloudFiles") 
        .option("cloudFiles.format", "csv") 
        .option("cloudFiles.schemaHints", "age int, annual_income int, spending_core int") # schema subset for evolution / new field
        .option("cloudFiles.schemaLocation", cloud_storage_path+"/schema_spend") # Autoloader will automatically infer all the schema & evolution
        .load(f"{csv_directory}")).withColumn("id", col("id").cast("int"))

(
  silver_spend
  .writeStream
  .trigger(once=True)
  .option("checkpointLocation", cloud_storage_path+"/checkpoint_spend")
  .table("spend_silver").awaitTermination()
)

spark.read.table("spend_silver").display()

# COMMAND ----------

# MAGIC %sql describe history spend_silver;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 4/ Gold table: users joined with their spend score
# MAGIC 
# MAGIC <img style="float:right" width="400px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-ingestion-step4.png"/>
# MAGIC 
# MAGIC We can now join the 2 tables based on the customer ID to create our final gold table.

# COMMAND ----------

# DBTITLE 1,Join user and spend to our gold table
spend = spark.read.table("spend_silver")
(spark.readStream.table("user_silver") 
     .join(spend, "id") 
     .drop("_rescued_data")
     .writeStream
        .trigger(once=True)
        .option("checkpointLocation", cloud_storage_path+"/checkpoint_gold")
        .table("user_gold").awaitTermination())

spark.read.table("user_gold").display()

# COMMAND ----------

# MAGIC %md ## Simplify your operations with transactional DELETE/UPDATE/MERGE operations
# MAGIC Traditional Data Lake struggle to run these simple DML operations. Using Databricks and Delta Lake, your data is stored on your blob storage with transactional capabilities. You can issue DML operation on Petabyte of data without having to worry about concurrent operations.

# COMMAND ----------

# DBTITLE 1,We just realised we have to delete data before 2015-01-01, let's fix that
# MAGIC %sql DELETE FROM user_gold where creation_date < '2015-01-01';

# COMMAND ----------

# DBTITLE 1,Delta Lake keeps history of the table operation
# MAGIC %sql describe history user_gold;

# COMMAND ----------

# DBTITLE 1,We can leverage the history to go back in time, restore or clone a table and enable CDC
# MAGIC %sql 
# MAGIC  --also works with AS OF TIMESTAMP "yyyy-MM-dd HH:mm:ss"
# MAGIC select * from user_gold version as of 1 ;
# MAGIC 
# MAGIC -- You made the DELETE by mistake ? You can easily restore the table at a given version / date:
# MAGIC -- RESTORE TABLE user_gold_clone TO VERSION AS OF 1
# MAGIC 
# MAGIC -- Or clone it (SHALLOW provides zero copy clone):
# MAGIC -- CREATE TABLE user_gold_clone SHALLOW|DEEP CLONE user_gold VERSION AS OF 1
# MAGIC 
# MAGIC -- Turn on CDC to capture insert/update/delete operation:
# MAGIC -- ALTER TABLE myDeltaTable SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Our finale tables are now ready to be used to build SQL Dashboards and ML models for customer classification!
# MAGIC <img style="float: right" width="400" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dashboard.png"/>
# MAGIC 
# MAGIC Switch to Databricks SQL to see how this data can easily be requested using [Databricks SQL Dashboard](https://e2-demo-field-eng.cloud.databricks.com/sql/dashboards/ab66e6c6-c2c5-4434-b784-ea5b02fe5eeb-sales-report?o=1444828305810485), or an external BI tool. 
# MAGIC 
# MAGIC Creating a single flow was simple.  However, handling many data pipeline at scale can become a real challenge:
# MAGIC * Hard to build and maintain table dependencies 
# MAGIC * Difficult to monitor & enforce advance data quality
# MAGIC * Impossible to trace data lineage
# MAGIC * Difficult pipeline operations (observability, error recovery)
# MAGIC 
# MAGIC <img style="float: right;" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/dlt-data-quality.png" width="400" />
# MAGIC 
# MAGIC #### To solve these challenges, Databricks introduced **Delta Live Table**
# MAGIC A simple way to build and manage data pipelines for fresh, high quality data!

# COMMAND ----------

# MAGIC %md
# MAGIC Continue with DLT: [DLT Pipeline](./?o=1444828305810485&owned-by-me=false&name-order=ascend&name=field_demos_retail#joblist/pipelines) and the [SQL notebook]($../02-Delta-Live-Table/02.1-SQL-Delta-Live-Table-Ingestion): 
# MAGIC 
# MAGIC Continue with ML: [create customers segments]($../04-DataScience-ML-use-cases/04.2-customer-segmentation/01-Retail-customer-segmentation)
