# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Get Data
# MAGIC 
# MAGIC 1. Download the [data csv file](https://github.com/himanshuguptadb/Telematics_Datagen/blob/master/Data/Data.zip) from github
# MAGIC 
# MAGIC   * In production, it is highly recommended to upload the data to an adls location and use it in workspace. 
# MAGIC   * For simplicity and demo purpose, we will go simple & use the UI. Please refer to below steps to load data and create tables.
# MAGIC   
# MAGIC ### Important Tip  
# MAGIC Keep two browser tabs/windows open for databricks. So you can use one for following instructions and other for performing actual work 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Data using UI
# MAGIC 
# MAGIC 1. Below steps need to performed for each csv/data file **individually**.
# MAGIC 2. Click <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/data-icon.png?raw=true" width="30"> **Data** in the sidebar.
# MAGIC 3. Click on **Add** in the top right corner and from the dropdown select **Add Data**.<br>  
# MAGIC <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/Add_Data.png?raw=true" width="400">
# MAGIC 4. Databricks provide multiple Native integrations to load for various sources. For this exercise, we will use **Upload data**. <br>
# MAGIC <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/Add_Data_Options.png?raw=true" width="1300">
# MAGIC 5. Click on **browse data** to open file browser window. Navigate to the correct folder to load the files. Pick the file you want to create the table for. <br>
# MAGIC <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/Upload_Data.png?raw=true" width="500">
# MAGIC 6. Complete the table creation process by providing the **catalog name*, **schema name** and **table name**. Column names are prepopulated based on the header row in the csv. <br>
# MAGIC <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/Select_catalog_schema.png?raw=true" width="1400">
# MAGIC 7. Click on **create table** button in the bottom left corner of your screen. <br>
# MAGIC <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/Create_Table.png?raw=true" width="1400">
# MAGIC 8. Table creation process is complete. You can review the table details in the data explorer window by click on <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/data-icon.png?raw=true" width="30">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Navigate to **01 - EDA** notebook for Analyzing the data.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
