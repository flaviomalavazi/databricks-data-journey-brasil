# Databricks notebook source
#SKIP_ON_DBC_ARCHIVE
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # ML/MLOps Life cycle: Training, deployment & inference
# MAGIC ## Automate Product classification with Databricks
# MAGIC 
# MAGIC Having missclassified product can have a major impact on direct revenues. Product aren't discoverable and won't transform.
# MAGIC 
# MAGIC Automated classification can also reduce operation time & accelerate product onboarding
# MAGIC 
# MAGIC <div><img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-ml-pipeline.png"/></div>
# MAGIC 
# MAGIC ## From small to big ML with Databricks ML Runtime
# MAGIC 
# MAGIC Databricks ML Runtime accelerate ML deployments, from single node (ex: sklearn) to distributed ML (ex: SparkML / DL horovod).
# MAGIC 
# MAGIC *Note: this demo demonstrate Databricks MLOps capabilities, real-world product classification would leverage DL on text and Images*
# MAGIC 
# MAGIC <!-- do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Fml%2Fproduct_classification%2Fml_product_classification_02&dt=ML">
# MAGIC <!-- [metadata={"description":"Create a model to classify product.<br/><i>Usage: basic MLFlow demo, auto-ml, feature store.</i>",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Data Science", "components": ["auto-ml", "mlflow", "feature-store"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# MAGIC %run ../../_resources/03-setup-product $reset_all_data=$reset_all_data

# COMMAND ----------

# MAGIC %md-sandbox 
# MAGIC 
# MAGIC <div style="float:right"><img src="https://quentin-demo-resources.s3.eu-west-3.amazonaws.com/images/model-explained.png" style="height: 180px"/></div>
# MAGIC 
# MAGIC ## What we want to do: product classification for our online store
# MAGIC 
# MAGIC Our shop receive new products every-day. 
# MAGIC 
# MAGIC We want to be able to automatically classify the product in the proper category!

# COMMAND ----------

# MAGIC %md #1/ Explore our dataset

# COMMAND ----------

# DBTITLE 1,Let's explore our product dataset, joining structured data and images 
# MAGIC %sql 
# MAGIC select id, content, root_category, name, description, price from product left join product_image using(id) where content is not null limit 10;

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- let's see our top 5 categories:
# MAGIC select count(*) as count, root_category from product group by root_category order by count DESC

# COMMAND ----------

# MAGIC %md #2 Prep features and save them in our Feature Store
# MAGIC 
# MAGIC The first step is to save our data in Databricks Feature store. By doing so, we'll link our model to a specific dataset and simplify further modells deployment.
# MAGIC 
# MAGIC The Feature store is the missing piece in our model governance:
# MAGIC - Feature Discoverability and Reusability across the entire organization (build feature once, reuse multiple time)
# MAGIC - Feature versioning
# MAGIC - Upstream and downstream Lineage (where is the feature coming from, which model is using it)
# MAGIC - Ensure your model will use the same data for training and inferences:
# MAGIC   - Batch (Using the underlying Delta Lake table)
# MAGIC   - Real-time access (with real-time backend synchronizing data to Mysql, Redis...)

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()
product_features_df = spark.table("product").where("description is not null and name is not null and price is not null") \
                                            .select("id", "root_category", "name", "description", "price")
#Let's create our feature store
#Note: If this line is in error, delete your feature store using the UI
fs.create_feature_table(
  name=f'{dbName}.product_feature',
  keys='id',
  schema=product_features_df.schema,
  description='These features are derived from the product dataset. We performed some cleanup and only selected products having a category and price.'
)
#and write our data to this feature store

fs.write_table(df=product_features_df, name=f'{dbName}.product_feature', mode='overwrite')

# COMMAND ----------

# DBTITLE 1,Our dataset fit in memory, we can leverage pandas+sklearn to build or model
products = fs.read_table(f'{dbName}.product_feature').orderBy(rand()).toPandas()
#if the dataset size increase, koalas can be imported to apply transformation at scale using pandas API
products.head(4)

# COMMAND ----------

# MAGIC %md-sandbox 
# MAGIC <div style="float:right"><img src="https://quentin-demo-resources.s3.eu-west-3.amazonaws.com/images/first-model.png" style="height: 230px"/></div>
# MAGIC ## Let's build our first model. 
# MAGIC 
# MAGIC Because our dataset is small and easily fit in memory, we'll use Scikit-learn for our prediction.
# MAGIC 
# MAGIC To keep it simple, we'll be using the description field of each product to find the classfication, with Scikit-Learn pipeline (Tf-Idf).
# MAGIC 
# MAGIC MLFlow will help us to track our experiment: **the model itself** (serialized), **the dependencies**, **hyper-parameters**, **model performance**, **code revision** (the notebook version), data used, images etc

# COMMAND ----------

# DBTITLE 1,Let's prepare split the data with a training and test set:
def prepare_data(products):
  le = LabelEncoder()
  le.fit(products["root_category"])
  X_train, X_test, y_train, y_test = train_test_split(products[["name", "description", "price"]], le.transform(products["root_category"]), test_size=0.33, random_state=42)
  return X_train, X_test, y_train, y_test, le

X_train, X_test, y_train, y_test, le = prepare_data(products)

# COMMAND ----------

# DBTITLE 1,Our Scikit Learn pipeline:
def get_scikit_learn_pipeline(n_estimators = 50, max_features = None):
  tf_idf = TfidfVectorizer(stop_words='english', strip_accents="ascii", max_features=max_features)
  featurisation = ColumnTransformer([("tfidf_descrition", tf_idf, 'description'),
                                     ("tfidf_name", tf_idf, 'name')], remainder='passthrough')
  
  return Pipeline([
      ('featurisation', featurisation),
      ('randomForest', RandomForestClassifier(n_estimators=n_estimators, random_state=42))
  ])

# COMMAND ----------

from sklearn.metrics import plot_confusion_matrix
from matplotlib import pyplot as plt
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import StandardScaler
from mlflow.models.signature import infer_signature


with mlflow.start_run():
  mlflow.sklearn.autolog(silent=True)
  ##########################################
  # let's train our Scikit Learn Pipeline: #
  ##########################################  
  pipeline = get_scikit_learn_pipeline()
  pipeline.fit(X_train, y_train)
  predictions = pipeline.predict(X_test)

  f1 = metrics.f1_score(y_test, predictions, average='micro')
  plot_confusion_matrix(pipeline, X_test, y_test)
  
  ###########################################
  #      Log extra parameter to MLFlow      #
  ###########################################
  mlflow.set_tag("project", "product_classification")
  mlflow.log_figure(plt.gcf(), "test_confusion_matrix.png")
  input_example = {
    "name": "Super Hero Series Villains",
    "description": "A super hero toy all the family will enjoy",
    "price": 14
  }
  signature = infer_signature(X_test, predictions)
  mlflow.sklearn.log_model(pipeline, "model", input_example=input_example, signature = signature)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Our model is automatically tracked and logged in MLFLow.
# MAGIC 
# MAGIC All our training information are now saved in MLFLow and available in the MLFLow side-bar and the UI:
# MAGIC * The model dependencies, 
# MAGIC * All hyper-parameters
# MAGIC * metrics and artifacts (custom images, confusion matrix etc)
# MAGIC * the model itself, automatically serialized
# MAGIC 
# MAGIC This garantee reproducibility and tracking over time, improving team efficiency and model governance.
# MAGIC 
# MAGIC Because we logged the model, we'll be able to use it to deploy it in production later, and track our model performance over time!

# COMMAND ----------

# MAGIC %md-sandbox 
# MAGIC ## Let's deploy our best model in production using MLFLow Registry
# MAGIC 
# MAGIC <div style="float:right"><img src="https://quentin-demo-resources.s3.eu-west-3.amazonaws.com/images/registry.gif" style="height: 280px"/></div>
# MAGIC 
# MAGIC MLFLow registry can be used to save our model and put it in production. 
# MAGIC 
# MAGIC We'll be using the Registry UI to create a model called `product_classification`
# MAGIC 
# MAGIC 
# MAGIC *Note: this example was using a small dataset with a very basic set of feature. Databricks let you scale without pain, using distributed training with spark mllib and deep learning (horovord)*

# COMMAND ----------

# DBTITLE 1,Save our new model to the registry as a new version
#get the best model from the registry
best_model = mlflow.search_runs(filter_string='tags.project = "product_classification"', order_by = ['metrics.f1'], max_results=1).iloc[0]
model_registered = mlflow.register_model("runs:/"+best_model.run_id+"/model", "field_demo_product_classification")

# COMMAND ----------

# DBTITLE 1,Flag this version as production ready
client = mlflow.tracking.MlflowClient()
client.transition_model_version_stage("field_demo_product_classification", model_registered.version, stage = "Production", archive_existing_versions=True)
print("model version "+model_registered.version+" as been registered as production ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## We now have our best model ready to be used in production!
# MAGIC Let's explore how we can use it in production

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="float:right"><img src="https://quentin-demo-resources.s3.eu-west-3.amazonaws.com/images/streaming-mlflow.png" style="height: 300px;"/></div>
# MAGIC ### Batch or Streaming inference using spark
# MAGIC 
# MAGIC Loading the model and applying the inference in a job can be done with one line of code, using spark or pandas.

# COMMAND ----------

# DBTITLE 1,Load the model and run inferences in batch (python/SQL/Scala)
#                                                                                                  stage
#                                                                            model name              |
#                                                                                 |                  |
get_product_class_udf = mlflow.pyfunc.spark_udf(spark, "models:/field_demo_product_classification/Production")
#Save the inference as a sql function
spark.udf.register("get_product_class", get_product_class_udf)

# COMMAND ----------

# MAGIC %sql select id, get_product_class(name, description, price) as predicted_category_id, name from product

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="float:right; margin-left: 50px"><img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/serving-reatime.png" style="height: 300px;"/></div>
# MAGIC ### Real-time deployments over REST API
# MAGIC 
# MAGIC Realtime deployments are also supported within Databricks.
# MAGIC 
# MAGIC Once your model is deployed in Databricks Registry, turn on model serving to serve your model over a rest API.
# MAGIC 
# MAGIC All versions are requestable, making AB/Testing simple, and deploying a new model to the registry will automatically update the serving.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Auto-ML can help you accelerate all your ML project
# MAGIC ### A glass-box solution that empowers data teams without taking away control
# MAGIC 
# MAGIC Databricks simplify model creation and MLOps. However, bootstraping new ML projects can still be long and inefficient. 
# MAGIC 
# MAGIC Instead of creating the same boterplate for each new project, Databricks Auto-ML can automatically generate state of the art models for Classifications, regression, and forecast.
# MAGIC 
# MAGIC Simply click start a new Auto-ML experimentation and deploy new projects in hours, accelerating your time to market!
# MAGIC 
# MAGIC 
# MAGIC <img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/auto-ml-full.png"/>
# MAGIC 
# MAGIC Models can be directly deployed, or instead leverage generated notebooks to boostrap projects with best-practices, saving you weeks of efforts.

# COMMAND ----------

# MAGIC %md ## Our features are now saved as feature store in our database. 
# MAGIC 
# MAGIC That's all we need to use Databricks Auto ML to build the model for us! 
# MAGIC 
# MAGIC We can also easily query and run extra analysis on our feature table using standard SQL if required.
