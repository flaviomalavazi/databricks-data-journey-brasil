# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ./00-setup $reset_all_data=$reset_all_data

# COMMAND ----------

init_experiment_for_batch('demo-retail', 'Customer Segmentation')
import seaborn as sns 
import math
from sklearn.preprocessing import MinMaxScaler
from sklearn.cluster import KMeans, AffinityPropagation
import matplotlib.pyplot as plt
import mlflow 
import plotly.express as px
import pandas as pd
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from mlflow.models.signature import infer_signature
from mlflow.tracking import MlflowClient
import json
from typing import Iterator, Tuple
import warnings
warnings.filterwarnings("ignore")
#enable autolog with silent mode
mlflow.sklearn.autolog(log_models=False, silent=True)


reset_all = dbutils.widgets.get("reset_all_data") == "true"
if not spark._jsparkSession.catalog().tableExists('customer_gold_segmentation') or reset_all:
  spark.sql("create table if not exists customer_gold_segmentation using delta location '/mnt/field-demos/retail/customer_segmentation'")

print("segmentation table loaded")
  

# COMMAND ----------

import plotly.graph_objects as go
#plot inertia
def plot_inertia(kmeans):
  plt.plot([k.n_clusters for k in kmeans], [k.inertia_ for k in kmeans])
  plt.xlabel("Number of clusters")
  plt.ylabel("Inertia")
  return plt.gcf()
  
#plot radars
def plot_radar(selected_cols, final_data):
  aggs = pd.DataFrame()
  for col in selected_cols:
    aggs[col] = final_data.groupby("cluster")[col].mean()

  radar_fig = make_subplots(rows=2, cols=2, specs=[[{'type': 'polar'}]*2]*2)
  i = 0
  for cluster_id, row in aggs.iterrows():
    radar_fig.add_trace(go.Scatterpolar(name = "Cluster "+str(cluster_id), r = row.tolist(), theta = selected_cols,), math.floor(i/2) + 1, i % 2 + 1)
    i = i+1
  radar_fig.update_layout(height=600, width=800, title_text="4 clusters exploration")
  radar_fig.update_traces(fill='toself')
  return radar_fig


#Plot 3D graph
def plot_3d_cluster(df, cluster_labels, x='annual_income', y='age', z='spending_core', width = 800, height = 500):
  fig = go.Figure()
  for cluster in list(df['cluster'].unique()):    
      fig.add_trace(go.Scatter3d(x = df[df['cluster'] == cluster]['annual_income'],
                                  y = df[df['cluster'] == cluster]['age'],
                                  z = df[df['cluster'] == cluster]['spending_core'],
                                  mode = 'markers', marker_size = 4, marker_line_width = 1,
                                  name = cluster_labels['cluster_' + str(cluster)]))

  fig.update_layout(width = width, height = height, autosize = True, showlegend = True,
                     scene = dict(xaxis=dict(title = 'Annual Income'),
                                  yaxis=dict(title = 'Age'),
                                  zaxis=dict(title = 'Spending Score')))
  return fig

# COMMAND ----------

# DBTITLE 1,Custom model to output the result as String/labels instead of integer
# Define the model class
class ClusteringModel(mlflow.pyfunc.PythonModel):

    def __init__(self, model, cluster_classes_name):
        self.model = model
        self.cluster_classes_name = cluster_classes_name

    def predict(self, context, model_input):
      predictions = self.model.predict(model_input)
      return pd.Series(predictions).apply(lambda x: self.cluster_classes_name['cluster_'+str(x)])
