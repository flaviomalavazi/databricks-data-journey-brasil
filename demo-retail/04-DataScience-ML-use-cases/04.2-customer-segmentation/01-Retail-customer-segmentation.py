# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %md
# MAGIC # End 2 end ML Customer segmentation pipeline with Databricks
# MAGIC Customer segmentation is an step to better tailor Marketing effort to various audience subsets.
# MAGIC 
# MAGIC * Customize marketing messages
# MAGIC * Select the best communication channel for the segment
# MAGIC * Identify ways to improve products or new product or service opportunities.
# MAGIC * Establish better customer relationships.
# MAGIC * Focus on the most profitable customers.
# MAGIC * ...
# MAGIC 
# MAGIC ## ML/MLOps Life cycle: Training, tracking, deployment & inference
# MAGIC 
# MAGIC <div><img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-ml-pipeline.png"/></div>
# MAGIC 
# MAGIC Let's see how this flow can easily be deployed with Databricks
# MAGIC 
# MAGIC *Note: This is a basic clustering example, real use-case would use more advanced modelisation, please refere to industry expert for more a more complete demo*
# MAGIC 
# MAGIC <!-- do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Fml%2Fcustomer_segmentation%2Fml_customer_segmentation_01&dt=ML">
# MAGIC <!-- [metadata={"description":"Create a model to classify product.<br/><i>Usage: basic MLFlow demo, auto-ml, feature store.</i>",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Data Science", "components": ["auto-ml", "mlflow", "sklearn"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# DBTITLE 1,Let's load our resources for the demo
# MAGIC %run ../../_resources/02-setup-segmentation $reset_all_data=$reset_all_data

# COMMAND ----------

# MAGIC %sql select id, firstname, age, annual_income, spending_core, email, last_activity_date from customer_gold_segmentation order by id

# COMMAND ----------

# DBTITLE 1,Data exploration & visualization
customer_segmentation = spark.read.table("customer_gold_segmentation").toPandas()
g = sns.PairGrid(customer_segmentation[['age','annual_income','spending_core']], diag_sharey=False)

g.map_lower(sns.kdeplot)
g.map_diag(sns.kdeplot, lw=3)
g.map_upper(sns.regplot)

# COMMAND ----------

# MAGIC %md ### Customer Segmentation using KMeans
# MAGIC We'll be using KMeans with sklearn to segment our customers.
# MAGIC 
# MAGIC To pick the best number of cluster, we'll be using each classification inertia and draw an "elbow graph". The ideal number of cluster will be around the "elbow"

# COMMAND ----------

from sklearn.metrics import silhouette_samples, silhouette_score

selected_cols = ['age', 'annual_income', 'spending_core']
cluster_data = customer_segmentation.loc[:,selected_cols]
scaler = MinMaxScaler([0,10])
cluster_scaled = scaler.fit_transform(cluster_data)

kmeans = []
with mlflow.start_run(run_name="segmentation_kmeans") as run:
  #We'll be testing from 2 to 10 clusters
  for cluster_number in range(2,10):
    #Each KMeans will be started as a sub-run to be properly logged in MLFlow
    with mlflow.start_run(nested=True):
      k = KMeans(n_clusters=cluster_number, random_state=0).fit(cluster_scaled)
      mlflow.log_metric("inertia", k.inertia_)
      kmeans.append(k)

  fig = plot_inertia(kmeans)
  mlflow.log_figure(fig, "inertia.jpg")
  #Let's get back the run ID as we'll need to add other figures in our run from another cell
  run_id = run.info.run_id

# COMMAND ----------

# MAGIC %md ### Segmentation analysis
# MAGIC We now need to understand our segmentation and assign classes with some meaning to each cluster. Some visualization is required to do that, we'll display radars for each clusters

# COMMAND ----------

cluster_selected = 4
#let's get back the model having 4 clusters
selected_model = kmeans[cluster_selected-2]
#We can now run .predict() on our entire dataset to assign a cluster for each row: 
final_data = pd.DataFrame(cluster_scaled, columns=selected_cols)
final_data['cluster'] = selected_model.predict(cluster_scaled)

#Based on our prediction, let's analyze each cluster popupation using a radar for each cluster:
radar_fig = plot_radar(selected_cols, final_data)
radar_fig

# COMMAND ----------

# MAGIC %md ### Updating our MLFlow run with the definition of our class and the radar figure

# COMMAND ----------

#Getting back the main run
with mlflow.start_run(run_id):
  #match the clusters to a class. This could be done automatically with a small set of data labelled, or manually in this case
  clusters = {"cluster_0": "small_spenders", "cluster_1": "medium_spenders", "cluster_2": "large_spenders", "cluster_3": "critical_spenders"}
  #Want to see how to create a custom model like ClusteringModel? Check notebook ../resources/02-setup-segmentation
  final_model = ClusteringModel(selected_model, clusters)
  
  #Saving our model with the signature and a small example
  signature = infer_signature(customer_segmentation[selected_cols], final_model.predict(None, final_data[selected_cols]))
  input_example=customer_segmentation[selected_cols][:1].to_dict('records')[0]
  # log main model & params
  mlflow.pyfunc.log_model(artifact_path = "kmeans", python_model = final_model, signature=signature, input_example=input_example)  
  mlflow.set_tag("field_demos", "retail_segmentation")
  mlflow.log_param("n_clusters", selected_model.n_clusters)
  mlflow.log_dict(clusters, "clusters_class.json")

  #let's log our radar figure and a 3D visualization
  mlflow.log_figure(radar_fig, "radar_cluster.html")
  fig = plot_3d_cluster(final_data, clusters)
  mlflow.log_figure(fig, "cluster.html")
  display(fig)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### MLFlow now has all our model information and the model is ready to be deployed in our registry!
# MAGIC We can do that manually:
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail-cdc-forecast/resources/images/mlflow_artifact.gif" alt="MLFlow artifacts"/>
# MAGIC 
# MAGIC or using MLFlow APIs directly:

# COMMAND ----------

# DBTITLE 1,Save our new model to the registry as a new version
#get the best model from the registry
best_model = mlflow.search_runs(filter_string='attributes.status = "FINISHED" and tags.field_demos = "retail_segmentation"', max_results=1).iloc[0]
model_registered = mlflow.register_model("runs:/"+best_model.run_id+"/kmeans", "field_demos_customer_segmentation")

# COMMAND ----------

# DBTITLE 1,Flag this version as production ready
client = mlflow.tracking.MlflowClient()
print("registering model version "+model_registered.version+" as production model")
client.transition_model_version_stage(name = "field_demos_customer_segmentation", version = model_registered.version, stage = "Production", archive_existing_versions=True)

# COMMAND ----------

# MAGIC %md #Deploying & using our model in production
# MAGIC 
# MAGIC Now that our model is in our MLFlow registry, we can start to use it in a production pipeline.

# COMMAND ----------

# MAGIC %md ### Scaling inferences using Spark 
# MAGIC We'll first see how it can be loaded as a spark UDF and called directly in a SQL function:

# COMMAND ----------

#                                                                                        Stage/version
#                                                                 Model name                   |
#                                                                     |                        |
get_cluster_udf = mlflow.pyfunc.spark_udf(spark, "models:/field_demos_customer_segmentation/Production", "string")
spark.udf.register("get_customer_segmentation_cluster", get_cluster_udf)

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, get_customer_segmentation_cluster(age, annual_income, spending_core) as segment from customer_gold_segmentation

# COMMAND ----------

# MAGIC %md ### Pure pandas inference
# MAGIC If we have a small dataset, we can also compute our segment using a single node and pandas API:

# COMMAND ----------

model = mlflow.pyfunc.load_model("models:/field_demos_customer_segmentation/Production")
df = spark.sql("select age, annual_income, spending_core from customer_gold_segmentation limit 10").toPandas()
df['cluster'] = model.predict(df)
df

# COMMAND ----------

# MAGIC %md Deploying the model in a Delta Live Table flow
# MAGIC Now that our model is ready, we can deploy it in a Delta Live Table pipeline. We just have to load it in a pipeline and run the transformations

# COMMAND ----------

# MAGIC %md #Segment analysis & tracking with Databricks SQL
# MAGIC 
# MAGIC Now that our entire pipeline is ready, our customer will be automatically segmented accordingly and we can start building analysis on top of this information.
# MAGIC 
# MAGIC 
# MAGIC <img width="800" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/customer-segmentation-dashboard-satisfaction.png"/>
# MAGIC 
# MAGIC 
# MAGIC [Access Databricks SQL dashboard with customer segmentation](https://e2-demo-field-eng.cloud.databricks.com/sql/dashboards/dbb7f120-9c58-471f-9817-a7c0e1b9cbe6-customer-segmentation---satisfaction?o=1444828305810485)
