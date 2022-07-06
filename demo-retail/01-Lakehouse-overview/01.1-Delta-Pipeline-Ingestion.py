# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %md
# MAGIC # Build a Lakehouse to deliver value to your buiness lines
# MAGIC 
# MAGIC <!-- do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Fnotebook_data_pipeline&dt=DATA_PIPELINE">
# MAGIC <!-- [metadata={"description":"Basic Data Ingestion pipeline to present Notebook / Lakehouse capability. COPY INTO, SQL, Python, stream. BRONZE/SILVER/GOLD. <br/><i>Usage: basic data prep / exploration demo / Lakehouse presentation.</i>",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{"Dashboards": ["Global Sales Report"],
# MAGIC                  "Queries": ["Customer Satisfaction Evolution"],
# MAGIC                  "DLT": ["DLT customer SQL"]},
# MAGIC          "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["autoloader", "copy into"]},
# MAGIC                       "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=$reset_all_data

# COMMAND ----------

# MAGIC %md-sandbox 
# MAGIC 
# MAGIC ## Preparing the data with Delta Lake
# MAGIC To deliver final outcomes such as segmentation, product classification or forecasting, we need to gather, process and clean the incoming data.
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-pipeline.png" style="height: 400px"/>
# MAGIC 
# MAGIC 
# MAGIC <br/>
# MAGIC 
# MAGIC <div style="float:left"> 
# MAGIC   
# MAGIC This can be challenging with traditional systems due to the following:
# MAGIC  * Data quality issue
# MAGIC  * Running concurrent operation
# MAGIC  * Running DELETE/UPDATE/MERGE over files
# MAGIC  * Governance (time travel, security, sharing data change, schema evolution)
# MAGIC  * Performance (lack of index, ingesting millions of small files on cloud storage)
# MAGIC  * Processing & analysing unstructured data (image, video...)
# MAGIC  * Switching between batch or streaming depending of your requirement
# MAGIC   
# MAGIC </div>
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo.png" width="200px" style="margin: 50px 0px 0px 50px"/>
# MAGIC 
# MAGIC <br style="clear: both"/>
# MAGIC 
# MAGIC ## Solving these challenges with Delta Lake
# MAGIC 
# MAGIC 
# MAGIC **What's Delta Lake? It's a new OSS standard to bring SQL Transactional database capabilities on top of parquet files!**
# MAGIC 
# MAGIC Delta Lake simplifies data manipulation and improves team efficiency by removing Data Engineering technical pains.
# MAGIC 
# MAGIC In addition to performance boost, it also provides functionalities required in most ETL pipelines, including ACID transaction, support for DELETE / UPDATE / MERGE operation using SQL or python, Time travel, clone...
# MAGIC <!-- 
# MAGIC * **ACID transactions** (Multiple writers can simultaneously modify a data set)
# MAGIC * **Full DML support** (UPDATE/DELETE/MERGE)
# MAGIC * **BATCH and STREAMING** support
# MAGIC * **Data quality** (expectatiosn, Schema Enforcement, Inference and Evolution)
# MAGIC * **TIME TRAVEL** (Look back on how data looked like in the past)
# MAGIC * **Performance boost** with ZOrder, data skipping and Caching, solves small files issue  -->

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 1/ Explore the dataset
# MAGIC 
# MAGIC We'll be using 1 flow of data for this example:
# MAGIC  * Customer stream (`customer id`, `email`, `firstname`, `lastname`...), delivered as JSON file in a blob storage

# COMMAND ----------

# DBTITLE 1,This is the data being delivered in our cloud storage. Let's explore the raw json files
# MAGIC %fs ls /mnt/field-demos/retail/users_json

# COMMAND ----------

# MAGIC %sql
# MAGIC -- As you can see, we have lot of small json files. Let's run a SQL query to explore the data
# MAGIC select * from json.`/mnt/field-demos/retail/users_json`

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 2/ Bronze: loading data from blob storage
# MAGIC <div style="float:right">
# MAGIC   <img width="400px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-ingestion-step1.png"/>
# MAGIC </div>
# MAGIC Databricks has advanced capacity such as Autoloader to easily ingest files, and support schema inference and evolution. 
# MAGIC 
# MAGIC For this example we'll use a sql `COPY INTO` command to incrementally load the data

# COMMAND ----------

# DBTITLE 1,We'll store the raw data in a USER_BRONZE DELTA table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS user_bronze (id LONG NOT NULL, email STRING, firstname STRING, lastname STRING, address STRING,
# MAGIC                                         city STRING, creation_date STRING, last_activity_date STRING, last_ip STRING, postcode STRING );

# COMMAND ----------

# DBTITLE 1,Loading JSON data to our Delta table
# MAGIC %sql
# MAGIC COPY INTO user_bronze
# MAGIC   FROM '/mnt/field-demos/retail/users_json'
# MAGIC   FILEFORMAT = JSON;
# MAGIC 
# MAGIC -- Our user_bronze Delta table is now ready for efficient query
# MAGIC SELECT count(*) as c, postcode FROM user_bronze GROUP BY postcode ORDER BY c desc limit 10;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 3/ Silver data: anonymized table, date cleaned
# MAGIC 
# MAGIC <img style="float:right" width="400px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-ingestion-step2.png"/>
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
     .writeStream
        .option("checkpointLocation", cloud_storage_path+"/checkpoint_user_silver")
        .table("user_silver"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 4/ Gold table: users joined with their spend score
# MAGIC 
# MAGIC <img style="float:right" width="400px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-ingestion-step4.png"/>
# MAGIC 
# MAGIC We can now join the 2 tables based on the customer ID to create our final gold table.
# MAGIC 
# MAGIC This time we'll do that as an incremental batch (not the `.trigger(once=True)` setting).

# COMMAND ----------

# DBTITLE 1,Join user and spend to our gold table, in python
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

# DBTITLE 1,Grant permission to other teams
# MAGIC %sql
# MAGIC GRANT SELECT ON DATABASE field_demos_retail TO 'data.scientist@databricks.com';
# MAGIC GRANT SELECT ON DATABASE field_demos_retail TO 'data.analyst@databricks.com';

# COMMAND ----------

# MAGIC %md ## Simplify your operations with transactional DELETE/UPDATE/MERGE operations
# MAGIC Traditional Data Lake struggle to run these simple DML operations. Using Databricks and Delta Lake, your data is stored on your blob storage with transactional capabilities. You can issue DML operation on Petabyte of data without having to worry about concurrent operations.
# MAGIC 
# MAGIC Databrick Managed Delta also provides more advanced capabilities:
# MAGIC 
# MAGIC * Time travel, table restore, 
# MAGIC * clone zero copy
# MAGIC * Performance: Index (Zorder), Auto compaction, optimize write, Low shuffle Merge...
# MAGIC * CDC (to propagate data changes)
# MAGIC * ...

# COMMAND ----------

# DBTITLE 1,We just realised we have to delete data before 2015-01-01, let's fix that
# MAGIC %sql DELETE FROM user_gold where creation_date < '2015-01-01';

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
# MAGIC <br/>
# MAGIC <img style="float: right;" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/dlt-data-quality.png" width="400" />
# MAGIC 
# MAGIC #### To solve these challenges, Databricks introduced **Delta Live Table**
# MAGIC A simple way to build and manage data pipelines for fresh, high quality data!

# COMMAND ----------

# MAGIC %md
# MAGIC Continue with DLT: [DLT Pipeline](./?o=1444828305810485&owned-by-me=false&name-order=ascend&name=field_demos_retail#joblist/pipelines) and the [SQL notebook]($../02-Delta-Live-Table/02.1-SQL-Delta-Live-Table-Ingestion): 
# MAGIC 
# MAGIC Continue with ML: [create customers segments]($../04-DataScience-ML-use-cases/04.2-customer-segmentation/01-Retail-customer-segmentation)
