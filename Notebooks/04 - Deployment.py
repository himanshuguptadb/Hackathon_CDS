# Databricks notebook source
# MAGIC %md
# MAGIC # Deploying Machine Learning models using Databricks
# MAGIC 
# MAGIC 
# MAGIC <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/ML_E2E_Pipeline4.png?raw=true" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Deploy the ML model you recently staged to production for realtime inference
# MAGIC 
# MAGIC Model serving is available in the "Machine Learning" space. <br>
# MAGIC 
# MAGIC * Click on **Models** under "Machine Learning" space. <br>
# MAGIC <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/ML_menu.png?raw=true">
# MAGIC * Click on the model you want to deploy for inference.
# MAGIC * Click on the **Use model for inference** button on the top right corner of your screen.<br>
# MAGIC <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/Model_inference.png?raw=true">
# MAGIC * In the popup window, select the model version, provide an endpoint name and click **create endpoint**. This will start the endpoint deploymnet process for inference. <br>
# MAGIC <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/create_endpoint.png?raw=true">
# MAGIC * Once endpoint is deployed, status will change to **ready**. Click on **Query endpoint** to do a quick test of the endpoint <br>
# MAGIC <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/Inference_ready.png?raw=true">
# MAGIC * In the popup window under **Browser**, click on **Show Example** and then **Send Request** <br>
# MAGIC <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/Model_query.png?raw=true">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Navigate to **05 - Dashboard** to deploy the model for real time inference.
