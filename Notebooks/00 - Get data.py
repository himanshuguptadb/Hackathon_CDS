# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Get Data
# MAGIC 
# MAGIC 
# MAGIC ### Using the Data UI:
# MAGIC 
# MAGIC 1. Download the [data csv file](https://github.com/IBM/telco-customer-churn-on-icp4d/blob/master/data/Telco-Customer-Churn.csv) from github
# MAGIC 2. Upload data to DBFS in your workspace:
# MAGIC   * In production, it is highly recommended to upload the data to an adls location and mount it to the workspace. 
# MAGIC   * For simplicity and demo purpose, we will go simple & use the UI. Please refer to [the documentation](https://docs.microsoft.com/en-us/azure/databricks/data/data) for more details on how to upload data to dbfs. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using command line

# COMMAND ----------

# DBTITLE 1,Create a DBFS data location 
#%fs mkdirs /FileStore/<Put Team Name Here>/telematics_data

# COMMAND ----------

# DBTITLE 1,Upload files to the DBFS data location
# MAGIC %md
# MAGIC 
# MAGIC Please refer to [the documentation](https://docs.microsoft.com/en-us/azure/databricks/data/data) for more details on how to upload data to dbfs

# COMMAND ----------

# DBTITLE 1,Check if the file exists
# %fs ls /FileStore/<Put Team Name Here>/telematics_data/

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Read data and store it in a delta table
# MAGIC 
# MAGIC Team database is already created with required privilleges to create table
# MAGIC 
# MAGIC Sample table creation script
# MAGIC 
# MAGIC product_df = spark.read.csv("/FileStore/<Put Team Name Here>/telematics_data/product.csv", header="true", inferSchema="true")  
# MAGIC product_df.write.format("delta").mode("overwrite").saveAsTable("catalog_name.database_name.table_name")

# COMMAND ----------

# DBTITLE 1,Create product table


# COMMAND ----------

# DBTITLE 1,Create events table


# COMMAND ----------

# DBTITLE 1,Create product failure table


# COMMAND ----------

# DBTITLE 1,Create weather table


# COMMAND ----------

# DBTITLE 1,Check table is created
# MAGIC %sql
# MAGIC use catalog <Catalog Name>;
# MAGIC 
# MAGIC select count(*) from <database name>.<table name>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
