# Databricks notebook source
# MAGIC %md
# MAGIC # Use Auto-ML to build baseline Models to bootstrap our ML Project
# MAGIC 
# MAGIC Using Auto ML we can experiment with Machine Learning Models on our Dataset with code base been automatically generated. 
# MAGIC 
# MAGIC <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/ML_E2E_Pipeline2.png?raw=true" width="1200">
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fmlops%2F02_auto_ml&dt=MLOPS">
# MAGIC <!-- [metadata={"description":"MLOps end2end workflow: Auto-ML notebook",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["auto-ml"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

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
# MAGIC Models can be directly deployed, or instead leverage generated notebooks to boostrap projects with best-practices, saving you weeks of efforts.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using Databricks Auto ML with our engine dataset
# MAGIC 
# MAGIC Auto ML is available in the "Machine Learning" space. All we have to do is start a new Auto-ML experimentation.
# MAGIC * Select the ML Compute Cluster
# MAGIC * Slect the ML problem type (Classification, Regression or Forecasting)
# MAGIC * Select the feature table we just created.  
# MAGIC * Our prediction target is the `failure` column.  
# MAGIC * Provide appropriate name to the experiment
# MAGIC * Under "Advance Configuration", you can set the `Timeout` to 10 mins instead of 120 mins for this excercise.
# MAGIC * Click on **Start AutoML**, and Databricks will do the rest. <br><br>
# MAGIC 
# MAGIC <img style="float: right" width="800" src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/Automl.png?raw=true"/>
# MAGIC 
# MAGIC While this is done using the UI, you can also leverage the [python API](https://docs.databricks.com/applications/machine-learning/automl.html#automl-python-api-1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Best Model  
# MAGIC ### AutoML will run numerous ML experiments and will provide you notebooks for each experiment.  
# MAGIC <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/experiments.png?raw=true" width = 1200/>  
# MAGIC 
# MAGIC ### Click on *View notebook for best model*  or *View data exploration notebook* to get to respective notebooks  
# MAGIC <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/bestmodel.png?raw=true" width = 1200/>  
# MAGIC 
# MAGIC ### To register the best model for deployment. Select the experiment with the best score and then click on *Register Model*
# MAGIC <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/registeration.png?raw=true" width = 1200/>  
# MAGIC 
# MAGIC ### Register the model with a new name. This will register the model with version 1.
# MAGIC <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/register model.png?raw=true" width = "400"/>  
# MAGIC 
# MAGIC ### Under Model registery, change the state of the version 1 to production
# MAGIC <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/model lifecycle.png?raw=true" width = "600"/>  
# MAGIC 
# MAGIC ### Your model is now ready to be deployed for realtime or batch inference.
