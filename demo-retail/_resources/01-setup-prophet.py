# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ./00-setup $reset_all_data=$reset_all_data

# COMMAND ----------

init_experiment_for_batch('demo-retail', 'Demand forecast')

# COMMAND ----------

# MAGIC %md TODO: fbprophet is now supported with DBR 10.1, could be improved with the next LTS support.

# COMMAND ----------

import fbprophet
from fbprophet import Prophet
from fbprophet.diagnostics import cross_validation
from fbprophet.diagnostics import performance_metrics
from mlflow.models.signature import infer_signature
from hyperopt import fmin, hp, tpe, STATUS_OK, Trials
from hyperopt.pyll.base import scope
from hyperopt import SparkTrials
from hyperopt import space_eval
from pyspark.sql.functions import lit
import logging
logging.getLogger("py4j").setLevel(logging.ERROR)

# COMMAND ----------

# MAGIC %md
# MAGIC ### MLFlow Model Flavors
# MAGIC 
# MAGIC Flavors offer a way of saving models in a way that's agnostic to the training development, making it significantly easier to be used in various deployment options.  Some of the most popular built-in flavors include the following:<br><br>
# MAGIC 
# MAGIC * <a href="https://mlflow.org/docs/latest/python_api/mlflow.pyfunc.html#module-mlflow.pyfunc" target="_blank">mlflow.pyfunc</a>
# MAGIC * <a href="https://mlflow.org/docs/latest/python_api/mlflow.keras.html#module-mlflow.keras" target="_blank">mlflow.keras</a>
# MAGIC * <a href="https://mlflow.org/docs/latest/python_api/mlflow.pytorch.html#module-mlflow.pytorch" target="_blank">mlflow.pytorch</a>
# MAGIC * <a href="https://mlflow.org/docs/latest/python_api/mlflow.sklearn.html#module-mlflow.sklearn" target="_blank">mlflow.sklearn</a>
# MAGIC * <a href="https://mlflow.org/docs/latest/python_api/mlflow.spark.html#module-mlflow.spark" target="_blank">mlflow.spark</a>
# MAGIC * <a href="https://mlflow.org/docs/latest/python_api/mlflow.tensorflow.html#module-mlflow.tensorflow" target="_blank">mlflow.tensorflow</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ### How do we log FBProphet model with MLflow ? 
# MAGIC 
# MAGIC **Custom model creation:** https://www.mlflow.org/docs/latest/models.html#example-creating-a-custom-add-n-model

# COMMAND ----------

#temp fix with file in repos, see https://databricks.slack.com/archives/CJFK6FVMG/p1633982875363300

# COMMAND ----------

# MAGIC %sh mkdir /tmp/temp_fix

# COMMAND ----------

# MAGIC %cd /tmp/temp_fix

# COMMAND ----------

#TODO: to be removed on next update as it's now part of the last mlflow version
import mlflow
import mlflow.pyfunc
import fbprophet
from fbprophet import Prophet
import cloudpickle

class FbProphetWrapper(mlflow.pyfunc.PythonModel):

    def __init__(self, model):
        self.model = model
        super(FbProphetWrapper, self).__init__()

    def load_context(self, context):
        from fbprophet import Prophet
        return

    def predict(self, context, model_input):
        future = self.model.make_future_dataframe(periods=model_input['periods'][0])
        return self.model.predict(future)
      
conda_env = mlflow.pyfunc.get_default_conda_env()      
conda_env["dependencies"] = ['fbprophet=={}'.format(fbprophet.__version__)] + conda_env["dependencies"]
conda_env["channels"] = conda_env["channels"] + ['conda-forge']

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

def define_prophet_model(params):
  return Prophet(interval_width=params["interval_width"],
                 growth=params["growth"],
                 daily_seasonality=params["daily_seasonality"],
                 weekly_seasonality=params["weekly_seasonality"],
                 yearly_seasonality=params["yearly_seasonality"],
                 seasonality_mode=params["seasonality_mode"])

def make_predictions(model, number_of_days):
  return model.make_future_dataframe(periods=number_of_days, freq='d', include_history=True)

def evaluate_metrics(model):
  df_cv = cross_validation(model, initial='730 days', period='90 days', horizon='180 days')
  df_p = performance_metrics(df_cv)
  return df_p

# COMMAND ----------

# structure of udf result set
result_schema =StructType([
  StructField('store',IntegerType()),
  StructField('item',IntegerType()),
  StructField('ds',DateType()),
  StructField('trend',FloatType()),
  StructField('yhat_lower', FloatType()),
  StructField('yhat_upper', FloatType()),
  StructField('trend_lower', FloatType()),
  StructField('trend_upper', FloatType()),
  StructField('multiplicative_terms', FloatType()),
  StructField('multiplicative_terms_lower', FloatType()),
  StructField('multiplicative_terms_upper', FloatType()),
  StructField('weekly', FloatType()),
  StructField('weekly_lower', FloatType()),
  StructField('weekly_upper', FloatType()),
  StructField('yearly', FloatType()),
  StructField('yearly_lower', FloatType()),
  StructField('yearly_upper', FloatType()),
  StructField('additive_terms', FloatType()),
  StructField('additive_terms_lower', FloatType()),
  StructField('additive_terms_upper', FloatType()),
  StructField('yhat', FloatType())
  ])

params = {
      "interval_width": 0.95,
      "growth": "linear",
      "daily_seasonality": False,
      "weekly_seasonality": True,
      "yearly_seasonality": True,
      "seasonality_mode": "multiplicative"
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notebook ran successfully
