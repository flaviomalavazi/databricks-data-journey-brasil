# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists field_demos_retail;
# MAGIC use field_demos_retail;

# COMMAND ----------

for folder in dbutils.fs.ls("/mnt/field-demos/retail/olist"):
  sql = "create table if not exists field_demos_retail."+folder.name[:-1]+" location '"+folder.path[:-1]+"'"
  print(sql)
  spark.sql(sql)
  
spark.sql(f"create table if not exists field_demos_retail.customer_satisfaction location '/mnt/field-demos/retail/customer_satisfaction'")
spark.sql(f"create table if not exists field_demos_retail.customer_segmentation location '/mnt/field-demos/retail/customer_segmentation_cluster'")
spark.sql(f"create table if not exists field_demos_retail.dlt_expectations location '/mnt/field-demos/retail/dlt_expectations'")

spark.sql(f"create table if not exists field_demos_retail.finegrain_forecasts location '/mnt/field-demos/retail/fgforecasts_predictions'")
spark.sql(f"create table if not exists field_demos_retail.finegrain_sales location '/mnt/field-demos/retail/fgforecasts_sales'")

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- GRANT ALL PRIVILEGES ON DATABASE field_demos_retail TO `quentin.ambard@databricks.com`;
# MAGIC -- REVOKE ALL PRIVILEGES ON DATABASE field_demos_retail FROM admins;
# MAGIC -- GRANT USAGE, SELECT ON DATABASE field_demos_retail TO admins;

# COMMAND ----------

# MAGIC %sql
# MAGIC use field_demos_retail;
# MAGIC show tables;
