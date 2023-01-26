# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Data Transformation with bamboolib
# MAGIC 
# MAGIC In this notebook we're going to perform data transformation on the dataset using features of bamboolib on Databricks.  Data wrangling and transformation requires a lot of time and usually involves many repetitive tasks. While required data transformation tasks depend on the project, there are some common transformations that are used in most of the data analysis projects. bamboolib offers all typical data transformations such as filtering, sorting, selecting or dropping columns, aggregations and joins.
# MAGIC 
# MAGIC 
# MAGIC **Requirements:**
# MAGIC * This notebook requires minimum **DBR version 11.0** on AWS and Azure and DBR version 11.1 on GCP 

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
# MAGIC 1. For dataframe name, proviate appropriate name to differentiate with other names
# MAGIC 1. click on "Copy Code" and execute the code in below cell
# MAGIC 
# MAGIC Bamboolib supports a wide range of data transformation actions. You can see the whole list in **"Search Actions" dropdown**. In this demo, we are doing to demonstrate the following transformations.
# MAGIC 
# MAGIC 
# MAGIC <br/>
# MAGIC 
# MAGIC ### 1 - Replace null string from data
# MAGIC 
# MAGIC Another common transformation is missing data handling. In the Fault_Code column, there are rows with missing values as "null". Let's replace these missing values with "". 
# MAGIC 
# MAGIC 
# MAGIC 1. In the search actions dropdown, search for **"Find and replace missing values"** and click on action. 
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
# MAGIC Bamboolib provides a quick action to transform data types. Let's covert *Survived* column to boolean type and convert *intake_temperature* to float type.
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
# MAGIC Another common transformation is missing data handling. In the *oil_temperature* column, there are some missing values. Let's replace these missing values with mean value. 
# MAGIC 
# MAGIC 
# MAGIC 1. In the search actions dropdown, search for **"Missing"**. There are couple of actions that you can use for missing values. You can drop missing values, find and replace them or drop columns with missing values.
# MAGIC 
# MAGIC 1. Select **"Find and replace missing values"** action.
# MAGIC 
# MAGIC 1. On the right panel, select column "oil_temperature" in "Replace missing values in" dropdown.
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
# MAGIC 1. Select "is missing" from the condition list.
# MAGIC 
# MAGIC 1. Keep the default dataframe name
# MAGIC 
# MAGIC 1. Click on execute.
# MAGIC 
# MAGIC <br/>

# COMMAND ----------

#Copy Code to load engine event data
engine_events_df = spark.table("himanshu_gupta_demos.hackathon.engine_events").toPandas()
engine_events_df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Export/Copy Auto-Generated Code
# MAGIC 
# MAGIC Exdcute the exported code in the cell below cell to save the state of the changes made to engine event dataframe

# COMMAND ----------

import pandas as pd; import numpy as np
# Step: Manipulate strings of 'intake_temperature' via Find 'null' and Replace with ''
engine_events_df["intake_temperature"] = engine_events_df["intake_temperature"].str.replace('null', '', regex=False)

# Step: Manipulate strings of 'oil_temperature' via Find 'null' and Replace with ''
engine_events_df["oil_temperature"] = engine_events_df["oil_temperature"].str.replace('null', '', regex=False)

# Step: Manipulate strings of 'exhaust_temperature' via Find 'null' and Replace with ''
engine_events_df["exhaust_temperature"] = engine_events_df["exhaust_temperature"].str.replace('null', '', regex=False)

# Step: Manipulate strings of 'fault_code' via Find 'null' and Replace with ''
engine_events_df["fault_code"] = engine_events_df["fault_code"].str.replace('null', '', regex=False)

# Step: Change data type of oil_temperature to Float
engine_events_df['oil_temperature'] = pd.to_numeric(engine_events_df['oil_temperature'], downcast='float', errors='coerce')

# Step: Change data type of exhaust_temperature to Float
engine_events_df['exhaust_temperature'] = pd.to_numeric(engine_events_df['exhaust_temperature'], downcast='float', errors='coerce')

# Step: Change data type of intake_temperature to Float
engine_events_df['intake_temperature'] = pd.to_numeric(engine_events_df['intake_temperature'], downcast='float', errors='coerce')

# Step: Replace missing values
engine_events_df[['oil_temperature']] = engine_events_df[['oil_temperature']].fillna(engine_events_df[['oil_temperature']].mean())

# Step: Replace missing values
engine_events_df[['exhaust_temperature']] = engine_events_df[['exhaust_temperature']].fillna(engine_events_df[['exhaust_temperature']].mean())

# Step: Replace missing values
engine_events_df[['intake_temperature']] = engine_events_df[['intake_temperature']].fillna(engine_events_df[['intake_temperature']].mean())



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
uom_conversion_df = spark.table("himanshu_gupta_demos.hackathon.uom_conversion").toPandas()
uom_conversion_df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Standardize altiture data from meters to feet
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
# MAGIC Exdcute the exported code in the cell below cell to save the state of the changes made to engine event dataframe

# COMMAND ----------

import pandas as pd; import numpy as np
# Step: Inner Join with engine_events_df where uom=altitude_uom
engine_event_standard_df = pd.merge(uom_conversion_df, engine_events_df, how='inner', left_on=['uom'], right_on=['altitude_uom'])

# Step: Create new column 'altitude_feet' from formula 'Conversion_rate_feet*altitude'
engine_event_standard_df['altitude_feet'] = engine_event_standard_df['Conversion_rate_feet']*engine_event_standard_df['altitude']

# Step: Drop columns
engine_event_standard_df = engine_event_standard_df.drop(columns=['uom', 'Conversion_rate_feet', 'altitude'])

# Step: Drop columns
engine_event_standard_df = engine_event_standard_df.drop(columns=['altitude_uom'])



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Save cleaned engine event data as silver table. 

# COMMAND ----------

engine_event_standard_df_spark = spark.createDataFrame(engine_event_standard_df)
engine_event_standard_df_spark.write.mode("overwrite").format("delta").saveAsTable("himanshu_gupta_demos.hackathon.engine_event_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Transformation Engine data

# COMMAND ----------

#Copy Code to load engine event data
engine_df = spark.table("himanshu_gupta_demos.hackathon.engine").toPandas()
engine_df

# COMMAND ----------

import pandas as pd; import numpy as np
# Step: Change data type of build_date to Datetime
engine_df['build_date'] = pd.to_datetime(engine_df['build_date'], infer_datetime_format=True)

# Step: Change data type of ship_date to Datetime
engine_df['ship_date'] = pd.to_datetime(engine_df['ship_date'], infer_datetime_format=True)

# Step: Change data type of engine_config to String/Text
engine_df['engine_config'] = engine_df['engine_config'].astype('string')

# Step: Change data type of user_application to String/Text
engine_df['user_application'] = engine_df['user_application'].astype('string')

# Step: Manipulate strings of 'inservice_date' via Find 'null' and Replace with ''
engine_df["inservice_date"] = engine_df["inservice_date"].str.replace('null', '', regex=False)

# Step: Change data type of inservice_date to Datetime
engine_df['inservice_date'] = pd.to_datetime(engine_df['inservice_date'], infer_datetime_format=True)

# Step: Convert 'user_application' to uppercase
engine_df['user_application'] = engine_df['user_application'].str.upper()



# COMMAND ----------

engine_df_spark = spark.createDataFrame(engine_df)
engine_df_spark.write.mode("overwrite").format("delta").saveAsTable("himanshu_gupta_demos.hackathon.engine_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Data Transformation weather data

# COMMAND ----------

weather_df = spark.table("himanshu_gupta_demos.hackathon.weather_data").toPandas()
weather_df

# COMMAND ----------

import pandas as pd; import numpy as np
# Step: Change data type of City to String/Text
weather_df['City'] = weather_df['City'].astype('string')

# Step: Change data type of Date to Datetime
weather_df['Date'] = pd.to_datetime(weather_df['Date'], infer_datetime_format=True)

# Step: Change data type of Highs to Integer
weather_df['Highs'] = pd.to_numeric(weather_df['Highs'], downcast='integer', errors='coerce')

# Step: Manipulate strings of 'Lows' via Find 'M' and Replace with ''
weather_df["Lows"] = weather_df["Lows"].str.replace('M', '', regex=False)

# Step: Change data type of Lows to Integer
weather_df['Lows'] = pd.to_numeric(weather_df['Lows'], downcast='integer', errors='coerce')

# Step: Manipulate strings of 'Average' via Find 'M' and Replace with ''
weather_df["Average"] = weather_df["Average"].str.replace('M', '', regex=False)

# Step: Change data type of Average to Float
weather_df['Average'] = pd.to_numeric(weather_df['Average'], downcast='float', errors='coerce')

# Step: Drop rows where ((Highs is missing) or (Lows is missing)) or (Average is missing)
weather_df = weather_df.loc[~(((weather_df['Highs'].isna()) | (weather_df['Lows'].isna())) | (weather_df['Average'].isna()))]



# COMMAND ----------

weather_df_spark = spark.createDataFrame(weather_df)
weather_df_spark.write.mode("overwrite").format("delta").saveAsTable("himanshu_gupta_demos.hackathon.weather_data_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Data Transformation Engine Faliure data

# COMMAND ----------

engine_faliure_df = spark.table("himanshu_gupta_demos.hackathon.engine_faliure").toPandas()
engine_faliure_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer to Gold Layer

# COMMAND ----------

engine_faliure_df = spark.table("himanshu_gupta_demos.hackathon.engine_faliure").toPandas()
weather_silver_df = spark.table("himanshu_gupta_demos.hackathon.weather_data_silver").toPandas()
engine_event_silver_df = spark.table("himanshu_gupta_demos.hackathon.engine_event_silver").toPandas()
engine_silver_df = spark.table("himanshu_gupta_demos.hackathon.engine_silver").toPandas()

# COMMAND ----------

engine_event_silver_df

# COMMAND ----------

import pandas as pd; import numpy as np
# Step: Change data type of event_timestamp to String/Text
engine_event_silver_df['event_timestamp'] = engine_event_silver_df['event_timestamp'].dt.strftime('%Y-%m-%d')

# Step: Change data type of event_timestamp to Datetime
engine_event_silver_df['event_timestamp'] = pd.to_datetime(engine_event_silver_df['event_timestamp'], infer_datetime_format=True)

# Step: Group by and aggregate
engine_event_agg_df = engine_event_silver_df.groupby(['engine_serial_number', 'event_timestamp', 'location']).agg({**{'event_timestamp': ['size']}, **{'fault_code': ['count']}, **{col: ['mean', 'median', 'min', 'max'] for col in ['rpm', 'oil_temperature', 'exhaust_temperature', 'intake_temperature', 'altitude_feet']}})
engine_event_agg_df.columns = ['_'.join(multi_index) for multi_index in engine_event_agg_df.columns.ravel()]
engine_event_agg_df = engine_event_agg_df.reset_index()

# Step: Inner Join with engine_silver_df where engine_serial_number=engine_serial_number
engine_event_agg_df = pd.merge(engine_event_agg_df, engine_silver_df, how='inner', on=['engine_serial_number'])

# Step: Left Join with weather_silver_df where event_timestamp=Date, location=City
engine_event_agg_df = pd.merge(engine_event_agg_df, weather_silver_df, how='left', left_on=['event_timestamp', 'location'], right_on=['Date', 'City'])

# Step: Left Join with engine_faliure_df where engine_serial_number=engine_serial_number, event_timestamp=event_timestamp
engine_event_agg_df = pd.merge(engine_event_agg_df, engine_faliure_df, how='left', on=['engine_serial_number', 'event_timestamp'])

# Step: Drop columns
engine_event_agg_df = engine_event_agg_df.drop(columns=['City', 'Date'])

# Step: Change data type of Failed to String/Text
engine_event_agg_df['Failed'] = engine_event_agg_df['Failed'].astype('string')

# Step: Replace missing values
engine_event_agg_df[['Failed']] = engine_event_agg_df[['Failed']].fillna('0')

# Step: Manipulate strings of 'Failed' via Find '1.0' and Replace with '1'
engine_event_agg_df["Failed"] = engine_event_agg_df["Failed"].str.replace('1.0', '1', regex=False)



# COMMAND ----------

engine_event_agg_df_spark = spark.createDataFrame(engine_event_agg_df)
engine_event_agg_df_spark.write.mode("overwrite").format("delta").saveAsTable("himanshu_gupta_demos.hackathon.engine_events_gold")

# COMMAND ----------

engine_event_agg_df
