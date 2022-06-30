# Databricks notebook source
# MAGIC %md ### Setup database required for DBSQL Dashboard
# MAGIC This database is shared globally, no need to run it per user, only once per workspace

# COMMAND ----------

# MAGIC %run ./00-global-setup $reset_data=false

# COMMAND ----------

# MAGIC %run "../demo-media/Usecase: Video Streaming QoE/_resources/00-prep-data-db-sql"

# COMMAND ----------

# MAGIC %run ../demo-retail/_resources/00-prep-data-db-sql

# COMMAND ----------

# MAGIC %run "../demo-retail/Usecase: POS stock and inventory/_resources/01-prep-data-db-sql"

# COMMAND ----------

# MAGIC %run "../demo-FSI/Usecase: ESG scoring/_resources/00-prep-data-db-sql"

# COMMAND ----------

# MAGIC %run "../product_demos/MLOps E2E pipeline: Churn detection/_resources/00-prep-data-db-sql"

# COMMAND ----------

# MAGIC %run "../demo-HLS/OMOP-common-data-model/_resources/00-prep-data-db-sql"

# COMMAND ----------

# MAGIC %run "../demo-media/Usecase: Real Time Bidding/_resources/00-prep-data-db-sql"

# COMMAND ----------

# MAGIC %run "../demo-media/Usecase: Product Reco Game event/_resources/00-prep-data-db-sql"

# COMMAND ----------

# MAGIC %run "../demo-manufacturing/Usecase: Digital Twins/_resources/00-prep-data-db-sql"

# COMMAND ----------

# MAGIC %run "../demo-manufacturing/Usecase: Factory optimization - OOE scoring/_resources/01-load-table-for-sql-analytics-dashboard"

# COMMAND ----------

# MAGIC %run "../demo-manufacturing/Usecase: Wind Turbine Predictive Maintenance/_resources/01-load-table-for-sql-analytics-dashboard"
