# Databricks notebook source
import dbldatagen as dg
import re

MARGIN_PATTERN= re.compile(r"\s*\|")  # margin detection pattern for stripMargin
def stripMargin(s):
  """  strip margin removes leading space in multi line string before '|' """
  return "\n".join(re.split(MARGIN_PATTERN, s))

# COMMAND ----------

# MAGIC %md
# MAGIC Generate Engine Master Data

# COMMAND ----------

import dbldatagen as dg
import pyspark.sql.functions as F

shuffle_partitions_requested = 8

spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 20000)

UNIQUE_ENGINES = 400
ENGINE_MIN_VALUE = 10000000

spark.catalog.clearCache()  # clear cache so that if we run multiple times to check
                            # performance, we're not relying on cache
shuffle_partitions_requested = 8
partitions_requested = 8
data_rows = UNIQUE_ENGINES

engine_dataspec = (dg.DataGenerator(spark, rows=data_rows, partitions=partitions_requested)
            #Generating engine serial number
            .withColumn("engine_serial_number","integer", minValue=ENGINE_MIN_VALUE, uniqueValues=UNIQUE_ENGINES)
                   
            .withColumn("build_date", "date", data_range=dg.DateRange("2022-01-01 00:00:00", "2022-09-30 11:55:00", "days=10"), random=True)
            
            .withColumn("ship_date", "date", expr="date_add(build_date, cast(floor(rand() * 50 + 1) as int))", baseColumn="build_date")
                   
            .withColumn("inservice_date", "date",  expr="date_add(ship_date, cast(floor(rand() * 50 + 1) as int))",  baseColumn="ship_date", percentNulls=0.05)
                   
            .withColumn("engine_config", "string", values = ["V1","V2","V3"], random=True)
                   
            .withColumn("user_application", "string", values = ["Transit Bus","Semi Truck", "semitruck", "School Bus"], random=True)
            )

df_engines = (engine_dataspec.build()
                .dropDuplicates(["engine_serial_number"])
                .cache()
               )

effective_engines = df_engines.count()

print(stripMargin(f"""revised engines : {df_engines.count()},
       |   unique engines: {df_engines.select(F.countDistinct('engine_serial_number')).take(1)[0][0]}""")
     )

display(df_engines)

# COMMAND ----------

# MAGIC %md
# MAGIC Generate Engine Event Data

# COMMAND ----------

import dbldatagen as dg
import pyspark.sql.functions as F
import datetime

AVG_EVENTS_PER_ENGINE = 1000

spark.catalog.clearCache()
shuffle_partitions_requested = 8
partitions_requested = 8
NUM_DAYS=30
data_rows = AVG_EVENTS_PER_ENGINE * UNIQUE_ENGINES * NUM_DAYS

spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 20000)


# use random seed method of 'hash_fieldname' for better spread - default in later builds
events_dataspec = (dg.DataGenerator(spark, rows=data_rows, partitions=partitions_requested)
                   
             # use same logic as per customers dataset to ensure matching keys - but make them random
            .withColumn("engine_serial_number","integer", minValue=ENGINE_MIN_VALUE, uniqueValues=UNIQUE_ENGINES, random=True)
            
            .withColumn("event_date", "timestamp",baseColumn=["engine_serial_number"],
                         data_range=dg.DateRange("2022-11-01 00:00:00",
                                                 "2022-12-31 11:59:59",
                                                 "days=2"),
                        random=True,omit=True)
            .withColumn("event_start_hour", "integer",baseColumn=["engine_serial_number","event_date"], 
                         minValue= 0, maxValue= 12,
                        random=False,omit=True)
                   
            .withColumn("event_interval", "integer", minValue = 0, maxValue= 21600, step = 300,
                        random=False,omit=True)
     
            .withColumn("event_timestamp", "string", expr = ("from_unixtime(unix_timestamp(event_date) + event_start_hour*3600 + event_interval)"), 
                            baseColumn=["event_start_hour", "event_date"])
                   
            .withColumn("altitude_type", "string",
                        baseColumn=["engine_serial_number"], values=[ "Low", "Medium", "High" ], omit=True
                        )
            .withColumn("profile", "string",
                        baseColumn=["engine_serial_number","event_date"], values=[ "Good", "Medium", "Bad" ], weights=[50, 25, 5 ], random=False, omit=True
                        )
            .withColumn("good_profile", "decimal(7,2)", minValue=1.0, maxValue=1.33, step = .02, baseColumn=["engine_serial_number","event_date"],
                        distribution=dg.distributions.Gamma(shape=0.75, scale=2.0),
                        random=True, omit=True)
                        
            .withColumn("medium_profile", "decimal(7,2)", minValue=1.33, maxValue=1.66, step = .03, baseColumn=["engine_serial_number","event_date"],
                        distribution=dg.distributions.Gamma(shape=0.75, scale=2.0),
                        random=True, omit=True)
                        
            .withColumn("bad_profile", "decimal(7,2)", minValue=1.75, maxValue=2.0, step = .04, baseColumn=["engine_serial_number","event_date"],
                        distribution=dg.distributions.Gamma(shape=0.75, scale=2.0),
                        random=True, omit=True)
            
                   
            .withColumn("altitude_low", "integer", minValue=0.0, maxValue=1257.0, step = 10, baseColumn=["engine_serial_number","event_date"],
                        distribution="normal",
                        random=True, omit=True)
                        
            .withColumn("altitude_medium", "integer", minValue=320.0, maxValue=1628.0, step = 20, baseColumn=["engine_serial_number","event_date"],
                        distribution="normal",
                        random=True, omit=True)
                        
            .withColumn("altitude_high", "integer", minValue=3350.0, maxValue=14433.0, step = 50, baseColumn=["engine_serial_number","event_date"],
                        distribution="normal",
                        random=True, omit=True)
                             
            .withColumn("altitude", "integer", baseColumn=["altitude_type","altitude_low","altitude_medium","altitude_high","profile","good_profile","medium_profile","bad_profile"],
                        expr= """
                              case when altitude_type = "Low"
                                   then altitude_low * (case when profile = "Good" then good_profile else case when profile = "Medium" then medium_profile else bad_profile end end)
                                   else case when altitude_type = "Medium"
                                             then altitude_medium * (case when profile = "Good" then good_profile else case when profile = "Medium" then medium_profile else bad_profile end end)
                                             else altitude_high * (case when profile = "Good" then good_profile else case when profile = "Medium" then medium_profile else bad_profile end end)
                              end end
                               """)
                   
             .withColumn("altitude_uom", "string", baseColumn=["altitude_type"],
                        expr= """
                              case when altitude_type = "Low"
                                   then "ft"
                                   else case when altitude_type = "Medium"
                                             then "meters"
                                             else "ft"
                              end end
                               """)
                   
             .withColumn("location", "string", baseColumn=["altitude_type"],
                        expr= """
                              case when altitude_type = "Low"
                                   then "Columbus"
                                   else case when altitude_type = "Medium"
                                             then "New York"
                                             else "Denver"
                              end end
                               """)
                   
            .withColumn("rpm_temp","integer", baseColumn=["engine_serial_number","event_date","altitude_type"],  minValue=0, maxValue=3000,
                        distribution="normal",
                        random=True, omit=True)

            .withColumn("rpm", "integer", baseColumn=["profile","good_profile","medium_profile","bad_profile","rpm_temp"],
                        expr= """
                              case when profile = "Good"
                                   then rpm_temp * good_profile
                                   else case when profile = "Medium"
                                             then rpm_temp * medium_profile
                                             else rpm_temp * bad_profile
                              end end
                               """)

            .withColumn("oil_temperature_temp","decimal(7,2)", baseColumn=["engine_serial_number","event_date","rpm"],  minValue=50.0, maxValue=250.0, step = 5,
                        distribution="normal", percentNulls=0.05,
                        random=True, omit=True)
                   
            .withColumn("oil_temperature", "decimal(7,2)", baseColumn=["profile","good_profile","medium_profile","bad_profile","oil_temperature_temp"],
                        expr= """
                              case when profile = "Good"
                                   then oil_temperature_temp * good_profile
                                   else case when profile = "Medium"
                                             then oil_temperature_temp * medium_profile
                                             else oil_temperature_temp * bad_profile
                              end end
                               """)

            .withColumn("exhaust_temperature_temp","decimal(7,2)", baseColumn=["engine_serial_number","event_date","rpm"],  minValue=392.0, maxValue=1293.0,
                        distribution="normal", percentNulls=0.05,
                        random=True, omit=True)
                   
            .withColumn("exhaust_temperature", "decimal(7,2)", baseColumn=["profile","good_profile","medium_profile","bad_profile","exhaust_temperature_temp"],
                        expr= """
                              case when profile = "Good"
                                   then exhaust_temperature_temp * good_profile
                                   else case when profile = "Medium"
                                             then exhaust_temperature_temp * medium_profile 
                                             else exhaust_temperature_temp * bad_profile
                              end end
                               """)
                   
            .withColumn("intake_temperature_ratio", "decimal(7,2)", minValue=.1, maxValue=.3, step = .02, baseColumn=["engine_serial_number","event_date"],
                        distribution=dg.distributions.Gamma(shape=0.75, scale=2.0),
                        random=True, omit=True)
                   
            .withColumn("intake_temperature", "decimal(7,2)", baseColumn=["profile","good_profile","medium_profile","bad_profile","exhaust_temperature_temp","intake_temperature_ratio"],
                        expr= """
                              case when profile = "Good"
                                   then exhaust_temperature_temp * good_profile * intake_temperature_ratio
                                   else case when profile = "Medium"
                                             then exhaust_temperature_temp * medium_profile * intake_temperature_ratio
                                             else exhaust_temperature_temp * bad_profile * intake_temperature_ratio
                              end end
                               """)
            .withColumn("fault_code","string",baseColumn=["oil_temperature","intake_temperature","profile","exhaust_temperature"],
                        expr= """
                              case when oil_temperature > 370
                                   then "F1393"
                                   else case when intake_temperature > 370
                                             then "F2367"
                                             else case when exhaust_temperature > 1900
                                             then "F3456"
                              end end end
                               """)
                   
                   
            .withColumn("event_type", "string",
                        baseColumn=["fault_code"],
                        expr= """
                              case when fault_code != ""
                                   then "FC"
                                   else "HB"
                              end
                               """)
            

            )

df_events = events_dataspec.build()

#Drop duplicates
df_eventsp = df_events.toPandas()
df_eventsp = df_eventsp.sort_values(by='fault_code', ascending=False)
df_eventsp = df_eventsp.drop_duplicates(subset=['engine_serial_number', 'event_timestamp'], keep='first')

display(df_eventsp)

# COMMAND ----------

import pandas as pd; import numpy as np
# Step: Select columns
df_eventsp_fc = df_eventsp[['engine_serial_number', 'event_timestamp', 'fault_code']]

# Step: Change data type of event_timestamp to Datetime
df_eventsp_fc['event_timestamp'] = pd.to_datetime(df_eventsp_fc['event_timestamp'], infer_datetime_format=True)

# Step: Keep rows where fault_code is one of: F1393, F3456, 2367
df_eventsp_fc_filter = df_eventsp_fc.loc[df_eventsp_fc['fault_code'].isin(['F1393', 'F3456', '2367'])]

# Step: Change data type of event_timestamp to String/Text
df_eventsp_fc_filter['event_timestamp'] = df_eventsp_fc_filter['event_timestamp'].dt.strftime('%Y-%m-%d')

# Step: Change data type of event_timestamp to Datetime
df_eventsp_fc_filter['event_timestamp'] = pd.to_datetime(df_eventsp_fc_filter['event_timestamp'], infer_datetime_format=True)

# Step: Group by engine_serial_number, event_timestamp and calculate new column(s)
df_eventsp_fc_filter_count = df_eventsp_fc_filter.groupby(['engine_serial_number', 'event_timestamp']).agg(fault_code_size=('fault_code', 'size')).reset_index()

# Step: Group by engine_serial_number and calculate new column(s)
df_eventsp_fc_filter_count_final = df_eventsp_fc_filter_count.groupby(['engine_serial_number']).agg(fault_code_size_sum=('fault_code_size', 'sum')).reset_index()

# Step: Keep rows where fault_code_size_sum >= 10
df_eventsp_fc_filter_count_final = df_eventsp_fc_filter_count_final.loc[df_eventsp_fc_filter_count_final['fault_code_size_sum'] >= 50]

# Step: Inner Join with df_eventsp_fc_filter_count where engine_serial_number=engine_serial_number
df_eventsp_fc_filter_count_final_faliure = pd.merge(df_eventsp_fc_filter_count_final, df_eventsp_fc_filter_count[['engine_serial_number', 'event_timestamp']], how='inner', on=['engine_serial_number'])

# Step: Drop columns
df_eventsp_fc_filter_count_final_faliure_ready = df_eventsp_fc_filter_count_final_faliure.drop(columns=['fault_code_size_sum'])

# Step: Group by engine_serial_number, event_timestamp and calculate new column(s)
df_eventsp_fc_filter_count_final_faliure_ready_download = df_eventsp_fc_filter_count_final_faliure_ready.groupby(['engine_serial_number', 'event_timestamp']).agg(Failed=('engine_serial_number', 'size')).reset_index()

df_eventsp_fc_filter_count_final_faliure_ready_download

# COMMAND ----------

df_eventsp_fc_filter_count_final_faliure_ready_download.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Engine Faliure Data

# COMMAND ----------

spark.catalog.clearCache()

NO_OF_ENGINES_FAILED = 150
AVERAGE_NO_OF_DAYS_FAILED = 3

data_rows = NO_OF_ENGINES_FAILED * AVERAGE_NO_OF_DAYS_FAILED

spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 20000)


# use random seed method of 'hash_fieldname' for better spread - default in later builds
faliure_dataspec = (dg.DataGenerator(spark, rows=data_rows, partitions=partitions_requested,
                   randomSeed=42, randomSeedMethod="hash_fieldname")
             # use same logic as per customers dataset to ensure matching keys - but make them random
            .withColumn("engine_serial_number","integer", minValue=ENGINE_MIN_VALUE,
                        uniqueValues=UNIQUE_ENGINES, random=True)
                   
            .withColumn("fail_date", "date", data_range=dg.DateRange("2022-11-01 00:00:00", "2022-12-31 11:55:00", "days=1"))

            # use specific random seed to get better spread of values
            .withColumn("fail","string",baseColumn=["engine_serial_number","fail_date"],
                        values=[ 1], random=True)

            

            )

df_events = faliure_dataspec.build()

display(df_events)

# COMMAND ----------

spark.udf.register("withEnhancedEventTime", withEnhancedEventTime)
