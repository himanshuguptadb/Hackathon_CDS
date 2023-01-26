# Databricks notebook source
# MAGIC %md
# MAGIC # Engine Failure Prediction

# COMMAND ----------

# MAGIC %md
# MAGIC # Use Auto-ML to build baseline Models to bootstrap our ML Project
# MAGIC 
# MAGIC Using Auto ML we can experiment with Machine Learning Models on our Dataset with code base been automatically generated. 
# MAGIC 
# MAGIC As Data Scientist, This saves me a lot of time and effort enabling me to focus resources on tuning my models based on the business knowledge I have if needed.
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-2.png" width="1200">
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fmlops%2F02_auto_ml&dt=MLOPS">
# MAGIC <!-- [metadata={"description":"MLOps end2end workflow: Auto-ML notebook",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["auto-ml"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------



# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Accelerating Failure Prediction model creation using Databricks Auto-ML
# MAGIC ### A glass-box solution that empowers data teams without taking away control
# MAGIC 
# MAGIC Databricks simplify model creation and MLOps. However, bootstraping new ML projects can still be long and inefficient. 
# MAGIC 
# MAGIC Instead of creating the same boilerplate for each new project, Databricks Auto-ML can automatically generate state of the art models for Classifications, regression, and forecast.
# MAGIC 
# MAGIC 
# MAGIC <img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/auto-ml-full.png"/>
# MAGIC 
# MAGIC <img style="float: right" width="600" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn-auto-ml.png"/>
# MAGIC 
# MAGIC Models can be directly deployed, or instead leverage generated notebooks to boostrap projects with best-practices, saving you weeks of efforts.
# MAGIC 
# MAGIC ### Using Databricks Auto ML with our engine dataset
# MAGIC 
# MAGIC Auto ML is available in the "Machine Learning" space. All we have to do is start a new Auto-ML experimentation and select the feature table we just created (`churn_features`)
# MAGIC 
# MAGIC Our prediction target is the `failure` column.
# MAGIC 
# MAGIC Click on Start, and Databricks will do the rest.
# MAGIC 
# MAGIC While this is done using the UI, you can also leverage the [python API](https://docs.databricks.com/applications/machine-learning/automl.html#automl-python-api-1)

# COMMAND ----------


