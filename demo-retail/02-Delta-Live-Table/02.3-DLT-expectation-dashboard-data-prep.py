# Databricks notebook source
dbutils.widgets.text("storage_path", "/Users/quentin.ambard@databricks.com/field_demos_retail/dlt", "DLT storage path")
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %md #Delta Live Table expectation analysis
# MAGIC Delta live table tracks our data quality through expectations. These expectations are stored as technical tables without the DLT log events. We can create a view to simply analyze this information
# MAGIC 
# MAGIC **Make sure you set your DLT storage path in thewidget!**
# MAGIC 
# MAGIC <!-- do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Fdlt%2Fnotebook_quality_expectations&dt=DATA_PIPELINE">
# MAGIC <!-- [metadata={"description":"Notebook extracting DLT expectations as delta tables used to build DBSQL data quality Dashboard.",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{"Dashboards": ["DLT Data Quality Stats"]},
# MAGIC  "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["autoloader", "copy into"]},
# MAGIC  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# MAGIC %run ../_resources/00-setup $reset_all_data=$reset_all_data

# COMMAND ----------

# DBTITLE 1,Adding our DLT system table to the metastore
# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS dlt_system_event_log_raw using delta LOCATION '${storage_path}/system/events';
# MAGIC select * from dlt_system_event_log_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyzing dlt_system_event_log_raw table structure
# MAGIC The `details` column contains metadata about each Event sent to the Event Log. There are different fields depending on what type of Event it is. Some examples include:
# MAGIC * `user_action` Events occur when taking actions like creating the pipeline
# MAGIC * `flow_definition` Events occur when a pipeline is deployed or updated and have lineage, schema, and execution plan information
# MAGIC   * `output_dataset` and `input_datasets` - output table/view and its upstream table(s)/view(s)
# MAGIC   * `flow_type` - whether this is a complete or append flow
# MAGIC   * `explain_text` - the Spark explain plan
# MAGIC * `flow_progress` Events occur when a data flow starts running or finishes processing a batch of data
# MAGIC   * `metrics` - currently contains `num_output_rows`
# MAGIC   * `data_quality` - contains an array of the results of the data quality rules for this particular dataset
# MAGIC     * `dropped_records`
# MAGIC     * `expectations`
# MAGIC       * `name`, `dataset`, `passed_records`, `failed_records`
# MAGIC       
# MAGIC We can leverage this information to track our table quality using SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploring the JSON data using Databricks SQL notation
# MAGIC Databricks can easily [explore semi structured data](https://docs.databricks.com/spark/latest/spark-sql/semi-structured.html)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   details:flow_definition.output_dataset,
# MAGIC   details:flow_definition.input_datasets,
# MAGIC   details:flow_definition.flow_type,
# MAGIC   details:flow_progress
# MAGIC FROM dlt_system_event_log_raw
# MAGIC WHERE details:flow_definition IS NOT NULL
# MAGIC ORDER BY timestamp

# COMMAND ----------

# MAGIC %sql 
# MAGIC create or replace temp view expectations as (
# MAGIC   SELECT 
# MAGIC     id,
# MAGIC     timestamp,
# MAGIC     details:flow_progress.metrics.num_output_rows as output_records,
# MAGIC     details:flow_progress.data_quality.dropped_records,
# MAGIC     details:flow_progress.status as status_update,
# MAGIC     explode(from_json(details:flow_progress.data_quality.expectations
# MAGIC              ,'array<struct<dataset: string, failed_records: bigint, name: string, passed_records: bigint>>')) expectations
# MAGIC   FROM dlt_system_event_log_raw
# MAGIC   where details:flow_progress.status='COMPLETED' and details:flow_progress.data_quality.expectations is not null
# MAGIC   ORDER BY timestamp);
# MAGIC select * from expectations

# COMMAND ----------

# DBTITLE 1,Let's materialize that as a Delta table to easily query it using DBSQL
#note: this could be another DLT pipeline, or using spark streaming api for incremental updates!
spark.sql("""SELECT
    id,
    expectations.dataset,
    expectations.name,
    expectations.passed_records,
    expectations.failed_records,
    status_update,
    dropped_records,
    output_records,
    timestamp 
    FROM expectations""").write.mode("overwrite").saveAsTable("dlt_expectations")

# COMMAND ----------

# MAGIC %sql select sum(failed_records) as failed_records, sum(passed_records) as passed_records, dataset from dlt_expectations group by dataset
