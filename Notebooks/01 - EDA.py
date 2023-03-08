# Databricks notebook source
# MAGIC %md
# MAGIC # Exploratory Data Analysis
# MAGIC 
# MAGIC The goal is to produce a service that can predict in real-time whether a engine will fail.
# MAGIC 
# MAGIC Our first step is to analyze the data, to understand data quality issues and what kind of features we will need to create.
# MAGIC 
# MAGIC <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/ML_E2E_Pipeline1.png?raw=true" width="1200">

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Using Bamboolib API on Apache Spark
# MAGIC ### All the insights. None of the code. 
# MAGIC Bamboolib is a UI component that allows for no-code data analysis and transformations from within a Databricks notebook. 
# MAGIC 
# MAGIC * Low-code/no-code 
# MAGIC * Can be embedded into Databricks notebooks
# MAGIC * Useful for team members of all skillsets
# MAGIC * Makes data wrangling and exploration fast and easy
# MAGIC * Generates python code can be customized and shared

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initializing *bamboolib*  
# MAGIC The next step is to import the library and initiate it. After running the code block below, you should be able to see the *bamboolib* UI.

# COMMAND ----------

import bamboolib as bam

bam

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Loading Data
# MAGIC 
# MAGIC *bamboolib* provides multiple methods for reading data. If you want to test out the widget's functionality, there are some dummy datasets that you can load. This is the quickest way for testing. 
# MAGIC 
# MAGIC The other two methods are; reading data from file in DBFS and loading data from a database table. In this section we will go through the steps for loading data from database table.
# MAGIC 
# MAGIC 
# MAGIC ## Load Engine Event Data
# MAGIC 
# MAGIC To import data:
# MAGIC 
# MAGIC 1. Click on "Load database table" button
# MAGIC 1. Enter "Catalog Name"
# MAGIC 1. Enter "database/schema name"
# MAGIC 1. Enter "engine events table name"
# MAGIC 1. For dataframe name, provide appropriate name to differentiate with other names
# MAGIC 1. click on "Copy Code" and execute the code in below cell
# MAGIC 
# MAGIC 
# MAGIC To access data exploration view in Bamboolib, we have two options. 
# MAGIC 
# MAGIC - The first option is to search for "Explore data frame" action. 
# MAGIC - The second and simpler way is you just click the "explore dataframe" button.

# COMMAND ----------

#Copy Code here and execute
engine_events_df = spark.table("<catalog>.<schema>.<table>").toPandas()
engine_events_df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Explore Data
# MAGIC 
# MAGIC ### Glimpse Overview
# MAGIC 
# MAGIC The first tab is the "Glimpse" overview. You can inspect missing data, data type and number unique values in this view. We can dive into more details by clicking on any of the columns. Let's click on "oil_temperature" column and bamboolib opens a new tab for this column. 
# MAGIC 
# MAGIC One of the tab we have is bi-variate plots. Bi-variate plots are great for showing the relationship between two variables in the dataset. These plots, permit us to see at a glance, the degree at the pattern of relationship between two variables. In this example, we see the bi-variate plots between the columns that we selected. In this case it is oil_temperature. And also another column that we can compare with. Let's select "intake_temperature" for example. Bamboolibs shows graphs/tables for the relationship between these two variables. 
# MAGIC 
# MAGIC The last two tabs are showing information about each column.
# MAGIC 
# MAGIC 
# MAGIC ### Predictive Power Score
# MAGIC 
# MAGIC Another important metric in data exploration is the predictive power score. The predictive power score or PPS is an asymmetric data type agnostic score that can detect linear or non-linear relationship between two columns. The score ranges from zero, which means no predictive power at all, to one, which means perfect predictive power. It can be used as an alternative to the correlation matrix.  
# MAGIC 
# MAGIC To view Predictive Power Score; 
# MAGIC - **Click on Predictors tab.** In the predictors tab. You can see a predictive power score for the selected column compared to other columns in the dataset.  In this example, we are seeing the predictive power scores for "oil_temperature" as you can see. This shows that the "rpm" column has highest predictive score for predicting oil_temperature column. While other columns, for example, location and altitude_uom are not predictive at all because the predictive power is zero. (Depending on how big is your sample, there might be another attribute with higher pps score compared to rpm)
# MAGIC 
# MAGIC While you can check predictive power score for a single variable. Sometimes you want to view an overview of the whole predictive power scores for a single dataset.
# MAGIC 
# MAGIC To view Predictive Power Scores for all columns in the dataset;
# MAGIC - **Click Predictor patterns tab** (Under Explore data tab).  You will see a matrix of predictive score between all variables in your dataset. 
# MAGIC - You can click on a cell and it will show you more details about the relationship between the two variables. 
# MAGIC - You can check density, plot, predictive power plus, and scatterplots to get more details. 
# MAGIC 
# MAGIC 
# MAGIC ### Correlation Matrix
# MAGIC 
# MAGIC To view the correlation matrix for the dataset;
# MAGIC 
# MAGIC - **Click on Correlation Matrix tab**. By default Bamboolib shows the correlation matrix for all numeric columns. The correlation matrix is color coded.  While positive correlation is shown in blue color. The negative correlation is shown as red.
# MAGIC 
# MAGIC - If we want to investigate the correlation between any of these two variables, we can **click on the correlation matrix cell** and it's going to show you more details about the relationship between these two variables.  
# MAGIC 
# MAGIC - In the detailed view, we can see density plot predictive power plot and scatter plot between two variables.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore Engine Event data  
# MAGIC 
# MAGIC ### Key take aways from Engine Event data -   
# MAGIC a. What all columns need data type fix?  
# MAGIC b. Are there Column with Null Values?  
# MAGIC c. Do we have columns where we need to standardize data?  
# MAGIC d. What is the granularity of the data

# COMMAND ----------

#Change the below code to work for Engine Event Data
engine_events_df = spark.table("<catalog>.<schema>.<table>").toPandas()
engine_events_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore Engine Master data
# MAGIC 
# MAGIC ### Key take aways from Engine master data -   
# MAGIC a. What all columns need data type fix?  
# MAGIC b. Are there Column with Null Values?  
# MAGIC c. Do we have columns where we need to standardize data?  
# MAGIC d. What is the granularity of the data

# COMMAND ----------

#Change the below code to work for Engine Master Data
engine_master_df = spark.table("<catalog>.<schema>.<table>").toPandas()
engine_master_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore Engine Faliure data
# MAGIC 
# MAGIC ### Key take aways from Engine Faliure data -   
# MAGIC a. What all columns need data type fix?  
# MAGIC b. Are there Column with Null Values?  
# MAGIC c. Do we have columns where we need to standardize data?  
# MAGIC d. What is the granularity of the data

# COMMAND ----------

#Change the below code to work for Engine Faliure Data
engine_failure_df = spark.table("<catalog>.<schema>.<table>").toPandas()
engine_failure_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore Weather data
# MAGIC 
# MAGIC ### Key take aways from Weather data -   
# MAGIC a. What all columns need data type fix?  
# MAGIC b. Are there Column with Null Values?  
# MAGIC c. Do we have columns where we need to standardize data?  
# MAGIC d. What is the granularity of the data

# COMMAND ----------

#Change the below code to work for Engine Weather Data
engine_weather_df = spark.table("<catalog>.<schema>.<table>").toPandas()
engine_weather_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Navigate to **02 - ETL** notebook for transforming the data.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
