# Databricks notebook source
#SKIP_ON_DBC_ARCHIVE
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %md #Sales forecast
# MAGIC We now have synchronized all our datas in our silver & gold Delta table.
# MAGIC 
# MAGIC We're ready to start some ML to forecast our sales for the next quarter, and adjust our stock accordingly!
# MAGIC 
# MAGIC 
# MAGIC For more information on this topic, be sure to check out [this blog](https://databricks.com/blog/2020/01/27/time-series-forecasting-prophet-spark.html) and its related notebook. (For even more forecasting examples including those that levarage alternative libraries along with event data and weather regressors, please see [this additional post](https://databricks.com/blog/2020/03/26/new-methods-for-improving-supply-chain-demand-forecasting.html))
# MAGIC 
# MAGIC For this demo, we will make use of an increasingly popular library for demand forecasting, [FBProphet](https://facebook.github.io/prophet/)
# MAGIC 
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail-cdc-forecast/resources/images/retail_forecast.png" alt='Make all your data ready for BI and ML'/>
# MAGIC <!-- do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Fml%2Fdemand_forecast%2Fml_demand_forecast_01&dt=ML">
# MAGIC <!-- [metadata={"description":"Demand forecast analysys with Prophet.<br/><i>Usage: basic MLFlow demo, model training scalability.</i>",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Data Science", "components": ["mlflow", "mlflow", "prophet"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# DBTITLE 1,We'll use Prophet to run our prediction. Let's install it in a new conda env
# MAGIC %pip install fbprophet

# COMMAND ----------

# DBTITLE 1,Data initialisation (Make sure prophet is available and installed with conda)
# MAGIC %run ../../_resources/01-setup-prophet $reset_all_data=$reset_all_data

# COMMAND ----------

# DBTITLE 1,Read sales history data from Delta table
# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS sales_history USING delta LOCATION '/mnt/field-demos/retail/forecast';
# MAGIC select * from sales_history;

# COMMAND ----------

# MAGIC %md ## Examine the Data
# MAGIC 
# MAGIC For our training dataset, we will make use of 5-years of store-item unit sales data for 50 items across 10 different stores. 
# MAGIC 
# MAGIC With the dataset accessible within Databricks, we can now explore it in preparation for modeling:

# COMMAND ----------

# MAGIC %md When performing demand forecasting, we are often interested in general trends and seasonality.  Let's start our exploration by examing the annual trend in unit sales:

# COMMAND ----------

# DBTITLE 1,Annual Trend
# MAGIC %sql
# MAGIC SELECT year(date) as year,  sum(sales) as sales FROM sales_history GROUP BY year(date) ORDER BY year;

# COMMAND ----------

# MAGIC %md It's very clear from the data that there is a generally upward trend in total unit sales across the stores. If we had better knowledge of the markets served by these stores, we might wish to identify whether there is a maximum growth capacity we'd expect to approach over the life of our forecast.  But without that knowledge and by just quickly eyeballing this dataset, it feels safe to assume that if our goal is to make a forecast a few days, weeks or months from our last observation, we might expect continued linear growth over that time span.
# MAGIC 
# MAGIC Now let's examine seasonality.  If we aggregate the data around the individual months in each year, a distinct yearly seasonal pattern is observed which seems to grow in scale with overall growth in sales:

# COMMAND ----------

# DBTITLE 1,Annual Seasonality
# MAGIC %sql
# MAGIC SELECT TRUNC(date, 'MM') as month, SUM(sales) as sales FROM sales_history GROUP BY TRUNC(date, 'MM') ORDER BY month;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales Forecasting
# MAGIC 
# MAGIC ![](https://miro.medium.com/max/2404/1*BVIwEoE5oEmHJU8XbV_mKA.png)
# MAGIC 
# MAGIC 
# MAGIC Refer: https://facebook.github.io/prophet/

# COMMAND ----------

# DBTITLE 1,Define model
def define_prophet_model(params):
  return Prophet(interval_width=params["interval_width"],
                 growth=params["growth"],
                 daily_seasonality=params["daily_seasonality"],
                 weekly_seasonality=params["weekly_seasonality"],
                 yearly_seasonality=params["yearly_seasonality"],
                 seasonality_mode=params["seasonality_mode"])

# COMMAND ----------

# DBTITLE 1,Future forecast
def make_predictions(model, number_of_days):
  return model.make_future_dataframe(periods=number_of_days, freq='d', include_history=True)

# COMMAND ----------

# DBTITLE 1,Model evaluation
def evaluate_metrics(model):
  df_cv = cross_validation(model, initial='730 days', period='90 days', horizon='180 days')
  df_p = performance_metrics(df_cv)
  return df_p

# COMMAND ----------

# DBTITLE 1,Prepare pandas dataframe sample for training
history_sample = spark.read.table("sales_history").where(col("date") >= "2015-01-01").sample(fraction=0.01, seed=123)
history_pd = history_sample.toPandas().rename(columns={'date':'ds', 'sales':'y'})[['ds','y']]
history_pd.count()

# COMMAND ----------

# DBTITLE 1,Define model parameters
params = {
      "interval_width": 0.95,
      "growth": "linear",
      "daily_seasonality": False,
      "weekly_seasonality": True,
      "yearly_seasonality": True,
      "seasonality_mode": "multiplicative"
    }

# COMMAND ----------

with mlflow.start_run():

  model = define_prophet_model(params)

  # fit the model to historical data
  model.fit(history_pd)

  # forcast for 30 days
  future_pd = make_predictions(model, 30)

  # predict over the dataset
  forecast_pd = model.predict(future_pd)
   
  # evalute performance
  metrics = evaluate_metrics(model)
  
  # plot forecasting
  predict_fig = model.plot(forecast_pd, xlabel='date', ylabel='sales', figsize=(25, 12))

  # mlflow tracking
  for key, value in params.items():
    mlflow.log_param(key, value)  
    
  mlflow.set_tag("model", "prophet")
  mlflow.log_metric("rmse", metrics.loc[0,'rmse'])  
  mlflow.log_figure(predict_fig, "forecast.png")
  
  # persist model
  model_environment = mlflow.sklearn.get_default_conda_env()
  model_environment['dependencies'] += ['fbprophet=={}'.format(fbprophet.__version__)]
  mlflow.sklearn.log_model(model, 'model', conda_env=model_environment)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Register models to MLflow Model Registry
# MAGIC 
# MAGIC The MLflow Model Registry component is a centralized model store, set of APIs, and UI, to collaboratively manage the full lifecycle of an MLflow Model. It provides model lineage (which MLflow experiment and run produced the model), model versioning, stage transitions (for example from staging to production), and annotations

# COMMAND ----------

# DBTITLE 1,Save our new model to the registry as a new version
#get the best model from the registry
best_model = mlflow.search_runs(filter_string='attributes.status = "FINISHED" and tags.model = "prophet"', max_results=1).iloc[0]
model_registered = mlflow.register_model(best_model.artifact_uri+"/model", "sales_forecast")

# COMMAND ----------

# DBTITLE 1,Flag this version as production ready
client = mlflow.tracking.MlflowClient()
print("registering model version "+model_registered.version+" as production model")
client.transition_model_version_stage(name = "sales_forecast", version = model_registered.version, stage = "Production", archive_existing_versions=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Well done! our model is now in te registry! 

# COMMAND ----------

# DBTITLE 1,Retrieve production model from ML registry
#                            Model name <----|                 |---->  Model stage (staging, production, archived)
#                                            |                 |
model = mlflow.pyfunc.load_model("models:/sales_forecast/Production")
data_to_predict = history_pd
data_to_predict['periods'] = 30
model.predict(data_to_predict)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### What's next ?
# MAGIC We can track our data & model performance using a SQL Analytics Dashboard
# MAGIC 
# MAGIC <img src='https://brysmiwasb.blob.core.windows.net/demos/images/forecasting_dashboard.PNG'>
# MAGIC 
# MAGIC ### But what if we want to scale that to multiple models, one per shop or even per item, and forecast each item?
# MAGIC 
# MAGIC This can easily be done with Databricks, let's explore that in our next notebook!
