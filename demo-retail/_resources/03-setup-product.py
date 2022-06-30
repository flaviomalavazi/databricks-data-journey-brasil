# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ./00-setup $reset_all_data=$reset_all_data

# COMMAND ----------

init_experiment_for_batch('demo-retail', 'Product Classification')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists product using DELTA location '/mnt/field-demos/retail/products';
# MAGIC create table if not exists product_image using DELTA location '/mnt/field-demos/retail/product_images';
