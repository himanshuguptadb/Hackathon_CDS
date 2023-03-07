# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Data Transformation with bamboolib
# MAGIC 
# MAGIC In this notebook we're going to perform data transformation on the dataset using features of bamboolib on Databricks.  Data wrangling and transformation requires a lot of time and usually involves many repetitive tasks. While required data transformation tasks depend on the project, there are some common transformations that are used in most of the data analysis projects. bamboolib offers all typical data transformations such as filtering, sorting, selecting,  dropping columns, aggregations and joins.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Medallion Architecture
# MAGIC 
# MAGIC Fundamental to the lakehouse view of ETL/ELT is the usage of a multi-hop data architecture known as the medallion architecture.
# MAGIC <img src="https://github.com/brickmeister/workshop_production_delta/blob/main/img/Multi-Hop%20Delta%20Lake.png?raw=true"> <br>
# MAGIC The Medallion Architecture is a common best practice to curating data in a data lake, providing 3 levels of refinement and consumption-readiness of the data. The 3 levels also provide convenient boundaries for data protection/access.
# MAGIC - __Bronze__ - data as raw as possible
# MAGIC - __Silver__ - a chance to cleanse data, adjust schema, fix broken records, apply business logic, join with other sources, redact sensitive info
# MAGIC - __Gold__ -business level aggregates suitable to serve to Analysts and Reporting layer
# MAGIC 
# MAGIC <i>And we can do this all in the data lake - in cloud object storage - without the need to move the data into a proprietary data warehouse - thus maintaining coherency and a single source of truth for our data</i>
# MAGIC 
# MAGIC See the following links below for more detail.
# MAGIC 
# MAGIC * [Medallion Architecture](https://databricks.com/solutions/data-pipelines)
# MAGIC * [Cost Savings with the Medallion Architecture](https://techcommunity.microsoft.com/t5/analytics-on-azure/how-to-reduce-infrastructure-costs-by-up-to-80-with-azure/ba-p/1820280)
# MAGIC * [Change Data Capture Streams with the Medallion Architecture](https://databricks.com/blog/2021/06/09/how-to-simplify-cdc-with-delta-lakes-change-data-feed.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer to Silver Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initializing *bamboolib*  
# MAGIC The next step is to import the library and initiate it. After running the code block below, you should be able to see the *bamboolib* UI.

# COMMAND ----------

import bamboolib as bam

bam

# COMMAND ----------

# MAGIC 
# MAGIC %md
# MAGIC 
# MAGIC ## Data Transformation Engine Event Data
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
# MAGIC Bamboolib supports a wide range of data transformation actions. You can see the whole list in **"Search Actions" dropdown**. In this demo, we are doing to demonstrate the following transformations.
# MAGIC 
# MAGIC 
# MAGIC <br/>
# MAGIC 
# MAGIC ### 1 - Replace null string from data
# MAGIC 
# MAGIC Bamboolib provides a quick action to replace text strings like null from data. In the intake_temperature column, there are rows with missing values as "null". Let's replace these missing values with "". 
# MAGIC 
# MAGIC 
# MAGIC 1. In the search actions dropdown, search for **"Find and replace text in column"** and click on action. 
# MAGIC 
# MAGIC 1. On the right panel, select *intake_temperature* in column list.
# MAGIC 
# MAGIC 1. Enter *null* in the **Find** field
# MAGIC 
# MAGIC 1. Leave the **replace with** field as is
# MAGIC 
# MAGIC 1. Keep the column name as before to overwrite the original column. If you want to create a new column, you can enter a name for the new column.
# MAGIC 
# MAGIC 1. Click on execute.
# MAGIC 
# MAGIC Repeat these steps for other columns where *null* value exists
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC <br/>
# MAGIC 
# MAGIC ### 2 - Change Column Data Type
# MAGIC 
# MAGIC Bamboolib provides a quick action to transform data types. Let's covert *intake_temperature* to float type.
# MAGIC 
# MAGIC To change data type;
# MAGIC 
# MAGIC 1. In the search actions dropdown, search for **"Change column data type"** and click on action. 
# MAGIC 
# MAGIC 1. On the right panel, select *intake_temperature* in column list.
# MAGIC 
# MAGIC 1. Select "float" for data type.
# MAGIC 
# MAGIC 1. Keep the column name as before to overwrite the original column. If you want to create a new column, you can enter a name for the new column.
# MAGIC 
# MAGIC 1. Click on execute.
# MAGIC 
# MAGIC Repeat these steps for other columns to change the data type of the column.
# MAGIC 
# MAGIC <br/>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ### 3 - Impute Missing Data
# MAGIC 
# MAGIC Another common transformation is missing data handling. In the *intake_temperature* column, there are some missing values. Let's replace these missing values with mean value. 
# MAGIC 
# MAGIC 
# MAGIC 1. In the search actions dropdown, search for **"Missing"**. There are couple of actions that you can use for missing values. You can drop missing values, find and replace them or drop columns with missing values.
# MAGIC 
# MAGIC 1. Select **"Find and replace missing values"** action.
# MAGIC 
# MAGIC 1. On the right panel, select column "intake_temperature" in "Replace missing values in" dropdown.
# MAGIC 
# MAGIC 1. Select **"Mean of Column"** in the "With" dropdown.
# MAGIC 
# MAGIC 1. Click on execute.
# MAGIC 
# MAGIC 
# MAGIC <br/>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ### 4 - Filter Rows
# MAGIC 
# MAGIC Another common transformation is filtered rows so we can filter out some of the rows that we don't want. In this example, let's say we want to filter out the rows that do not have exhaust temperature information, which have null values.
# MAGIC 
# MAGIC To filter these rows:
# MAGIC 
# MAGIC 1. In the search actions dropdown, search for **"Filter rows"** and click on action. 
# MAGIC 
# MAGIC 1. On the right panel, select *exhaust_temperature* in column list.
# MAGIC 
# MAGIC 1. Select "drop rows"
# MAGIC 
# MAGIC 1. Select "is missing" from the condition list.
# MAGIC 
# MAGIC 1. Keep the default dataframe name
# MAGIC 
# MAGIC 1. Click on execute.
# MAGIC 
# MAGIC <br/>

# COMMAND ----------

#Copy Code to load engine event data
engine_events_df = spark.table("<catalog_name>.<database_name>.<table_name>").toPandas()
engine_events_df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Export/Copy Auto-Generated Code
# MAGIC 
# MAGIC Execute the exported code in the cell below cell to save the state of the changes made to engine event dataframe

# COMMAND ----------

#Paste Auto-Generated Code to reproduce the transformations later



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Load UOM conversion table
# MAGIC 
# MAGIC There are two options to load the UOM conversion data
# MAGIC 1. Follow the steps used to load engine event data
# MAGIC 1. Copy the python code for engine event and replace the table name with "uom_conversion" and change data frame name

# COMMAND ----------

#Copy Code to load uom conversion data
uom_conversion_df = spark.table("<catalog_name>.<database_name>.<table_name>").toPandas()
uom_conversion_df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Standardize altitude data from meters to feet
# MAGIC 
# MAGIC <br/>
# MAGIC 
# MAGIC ### 1 - Join / Merge two data frames
# MAGIC 
# MAGIC Bamboolib provides a quick action to join two tables. Let's join engine_event and uom_conversion tables. Both the tables are loaded as pandas dataframe
# MAGIC 
# MAGIC To join data;
# MAGIC 
# MAGIC 1. In the search actions dropdown, search for **"Join / Merge dataframes"** and click on action. 
# MAGIC 
# MAGIC 1. Select join type "Inner join"
# MAGIC 
# MAGIC 1. In between the dataframes, uom_conversion dataframe is already selected. Choose engine_event_df as the other dataframe
# MAGIC 
# MAGIC 1. In based on the keys choose the appropriate columns to join on.
# MAGIC 
# MAGIC 1. Keep all the columns
# MAGIC 
# MAGIC 1. Choose a new dataframe name which has the combined data
# MAGIC 
# MAGIC 1. Click on execute.
# MAGIC 
# MAGIC <br/>
# MAGIC 
# MAGIC ### 2 - Create new calculated column
# MAGIC 
# MAGIC Bamboolib provides a quick action to create a new calculated column. Let's create a new column altitude_feet, which is a multiplication of altitude and conversion_factor
# MAGIC 
# MAGIC To create new column data;
# MAGIC 
# MAGIC 1. In the search actions dropdown, search for **"New column Formula"** and click on action. 
# MAGIC 
# MAGIC 1. In New column Name, enter *altitude_feet*
# MAGIC 
# MAGIC 1. Under column formula, provide the appropriate calculation
# MAGIC 
# MAGIC 1. Click on execute.
# MAGIC 
# MAGIC <br/>
# MAGIC 
# MAGIC ### 3 - Select / Drop Columns
# MAGIC 
# MAGIC Let's drop the *altitude* column. 
# MAGIC 
# MAGIC 1. In the search actions dropdown, search for "Select or drop columns" and click on action. 
# MAGIC 
# MAGIC 1. On the right panel, select the columns that we want to drop, which is going to be altitude. 
# MAGIC 
# MAGIC 1. Keep the default dataframe name
# MAGIC 
# MAGIC 1. Click on execute.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Export/Copy Auto-Generated Code
# MAGIC 
# MAGIC Execute the exported code in the cell below cell to save the state of the changes made to engine event dataframe

# COMMAND ----------

#Paste Auto-Generated Code to reproduce the transformations later



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Save cleaned engine event data as silver table.
# MAGIC Save the transformed engine event data as a table. This cleaned table can now be used in visualizations, feature engineering etc..

# COMMAND ----------

engine_event_standard_df_spark = spark.createDataFrame(<enter the final engine event dataframe name>)
engine_event_standard_df_spark.write.mode("overwrite").format("delta").saveAsTable("<catalog_name>.<database_name>.<table_name>")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Transformation Engine data
# MAGIC 
# MAGIC Use the information you gathered duing EDA to clean and transform engine data. 

# COMMAND ----------

engine_df = spark.table("<catalog_name>.<database_name>.<table_name>").toPandas()
engine_df

# COMMAND ----------

#Paste Auto-Generated Code to reproduce the transformations later


# COMMAND ----------

engine_df_spark = spark.createDataFrame(<enter the final engine dataframe name>)
engine_df_spark.write.mode("overwrite").format("delta").saveAsTable("<catalog_name>.<database_name>.<table_name>")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Data Transformation weather data
# MAGIC 
# MAGIC Use the information you gathered duing EDA to clean and transform weather data. 

# COMMAND ----------

weather_df = spark.table("<catalog_name>.<database_name>.<table_name>").toPandas()
weather_df

# COMMAND ----------

#Paste Auto-Generated Code to reproduce the transformations later


# COMMAND ----------

weather_df_spark = spark.createDataFrame(<enter the final weather dataframe name>)
weather_df_spark.write.mode("overwrite").format("delta").saveAsTable("<catalog_name>.<database_name>.<table_name>")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Data Transformation Engine Failure data
# MAGIC 
# MAGIC Use the information you gathered duing EDA to clean and transform engine failure data. 

# COMMAND ----------

engine_failure_df = spark.table("<catalog_name>.<database_name>.<table_name>").toPandas()
engine_failure_df

# COMMAND ----------

#Paste Auto-Generated Code to reproduce the transformations later

# COMMAND ----------

engine_failure_df_spark = spark.createDataFrame(<enter the final engine faliure dataframe name>)
engine_failure_df_spark.write.mode("overwrite").format("delta").saveAsTable("<catalog_name>.<database_name>.<table_name>")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer to Gold Layer
# MAGIC 
# MAGIC Create one or mutiple aggregate/feature table combining all the available data which can be used for visualizations or building machine learning models.

# COMMAND ----------

engine_faliure_silver_df = spark.table("<catalog_name>.<database_name>.<table_name>").toPandas()
weather_data_silver_df = spark.table("<catalog_name>.<database_name>.<table_name>").toPandas()
engine_event_silver_df = spark.table("<catalog_name>.<database_name>.<table_name>").toPandas()
engine_silver_df = spark.table("<catalog_name>.<database_name>.<table_name>").toPandas()

# COMMAND ----------

engine_event_silver_df

# COMMAND ----------

#Paste Auto-Generated Code to reproduce the transformations later

# COMMAND ----------

engine_event_agg_df_spark = spark.createDataFrame(engine_event_agg_df)
engine_event_agg_df_spark.write.mode("overwrite").format("delta").saveAsTable("<catalog_name>.<database_name>.<table_name>")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Navigate to **03 - Train ML Model** notebook for building a machine learning model
