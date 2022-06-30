# Databricks notebook source
# MAGIC %md
# MAGIC # Data generator for DLT pipeline
# MAGIC 
# MAGIC # -- WORK IN PROGRESS, requires update on DLT pipeline + customer segmentation model --
# MAGIC This notebook will generate data in the given storage path to simulate a data flow. 
# MAGIC 
# MAGIC **Make sure the storage path matches what you defined in your DLT pipeline as input.**
# MAGIC 
# MAGIC 1. Run Cmd 2 to show widgets
# MAGIC 2. Specify Storage path in widget
# MAGIC 3. "Run All" to generate your data
# MAGIC 4. Cmd 5 output should show data being generated into storage path
# MAGIC 5. When finished generating data, "Stop Execution"
# MAGIC 6. To refresh landing zone, run Cmd 7
# MAGIC 
# MAGIC <!-- do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdlt%2Fnotebook_dlt_generator&dt=DLT">
# MAGIC <!-- [metadata={"description":"Generate data for the DLT demo",
# MAGIC  "authors":["dillon.bostwick@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["autoloader", "dlt"]}}] -->

# COMMAND ----------

# DBTITLE 1,Run First for Widgets
dbutils.widgets.text('path', '/demo/dlt_retail', 'Storage Path')
dbutils.widgets.combobox('batch_wait', '30', ['15', '30', '45', '60'], 'Speed (secs between writes)')
dbutils.widgets.combobox('num_recs', '10000', ['5000', '10000', '20000'], 'Volume (# records per writes)')
dbutils.widgets.combobox('batch_count', '100', ['100', '200', '500'], 'Write count (for how long we append data)')

# COMMAND ----------

# MAGIC %pip install iso3166 Faker

# COMMAND ----------

# MAGIC %sql 
# MAGIC select min(cast(_c0 as int)), max(cast(_c0 as int)) from csv.`/mnt/field-demos/retail/spend_csv`
# MAGIC 
# MAGIC -- select count(distinct cast(_c0 as int)) from csv.`/mnt/field-demos/retail/spend_csv`

# COMMAND ----------

# MAGIC %sql select * from json.`/mnt/field-demos/retail/users_json`

# COMMAND ----------



# COMMAND ----------

folder = "/demos/dlt_retail"
#dbutils.fs.rm(folder, True)
try:
  dbutils.fs.ls(folder)
except:
  print("folder doesn't exists, generating the data...")
  from pyspark.sql import functions as F
  from faker import Faker
  from collections import OrderedDict 
  import uuid
  fake = Faker()

  fake_firstname = F.udf(fake.first_name)
  fake_lastname = F.udf(fake.last_name)
  fake_email = F.udf(fake.ascii_company_email)
  fake_date = F.udf(lambda:fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S"))
  fake_address = F.udf(fake.address)
  fake_id = F.udf(lambda: str(uuid.uuid4()))

  df = spark.range(0, 100000)
  #TODO: need to increment ID for each write batch to avoid duplicate. Could get the max reading existing data, zero if none, and add it ti the ID to garantee almost unique ID (doesn't have to be perfect)  
  df = df.withColumn("id", F.monotonically_increasing_id())
  df = df.withColumn("creation_date", fake_date())
  df = df.withColumn("firstname", fake_firstname())
  df = df.withColumn("lastname", fake_lastname())
  df = df.withColumn("email", fake_email())
  df = df.withColumn("address", fake_address())
  df = df.withColumn("gender", F.round(F.rand()+0.2))
  df = df.withColumn("age_group", F.round(F.rand()*10))
  #TODO: add bad data (id null, email null ?)
  
  display(df)

# COMMAND ----------

import random

#let's generate 4 obvious clusters for our model. 
def get_user_transactions(gender, age_group, income_group):
  if age_group > 7 and income_group > 5:
    transaction_count_boost = 3
    transaction_amount_boost = 3
  elif age_group > 7 and income_group < 5:
    transaction_count_boost = 4
    transaction_amount_boost = 0
  elif age_group < 4 and income_group > 6:
    transaction_count_boost = 0
    transaction_amount_boost = 5
  else:
    transaction_count_boost = 0
    transaction_amount_boost = 0
    
  #gender 0 will make bigger transaction
  if gender == 0:
    transaction_amount_boost += 5

  transactions = []
  for i in range(0,random.randint(1,3)+random.randint(0,1+transaction_count_boost)):
    amount = 10 + random.randint(1,10)+random.randint(0,20*transaction_amount_boost)
    item_count = 1 + random.randint(0,2)+random.randint(0,transaction_amount_boost)
    transactions += [{"id": str(uuid.uuid4()), "amount": amount, "item_count": item_count, "transaction_date": fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S")}]
    
  return transactions

get_user_transactions(0, 3, 3)

#TODO: add a pands udf to return an array of N transaction
#def get_user_transactions_udf():
#...


# COMMAND ----------

customer_number = df.count()

df_t = spark.range(0, customer_number)
df_t = df_t.withColumn("id", fake_id())
df_t = df_t.withColumn("transaction_date", fake_date())
df_t = df_t.withColumn("amount", F.round(F.rand()*1000))
df_t = df_t.withColumn("item_count", F.round(F.rand()*10))
#Join with the customer to get the same IDs generated.
df_t = df_t.withColumn("t_id", F.monotonically_increasing_id()).join(df.select("id").withColumnRenamed("id", "customer_id").withColumn("t_id", F.monotonically_increasing_id()), "t_id").drop("t_id")
#let's generate 4 obvious clusters for our model. 

#TODO: join with the other dataset to get gender, age_group, income_group
#df_t = df_t.withColumn("transactions", get_user_transactions(col(gender), col(age_group), col(income_group)))
# call the udf
#df_t = df_t.explode("transactions")
#drop the columns from the join
#df_t = df_t.drop("transactions", gender, age_group, income_group)

display(df_t)

# COMMAND ----------

import time
#TODO: output customers & transactions
#dbutils.fs.mkdirs(f'{output_path}/landing')

#for i in range(0, int(dbutils.widgets.get('batch_count'))):
#  time.sleep(int(dbutils.widgets.get('batch_wait')))
#  write_batches_to_file(generate_loans("account", int(dbutils.widgets.get('num_recs'))), f'/dbfs{output_path}/landing/accounts{i}.json')
#  print(f'Finished writing batch: {i}')
