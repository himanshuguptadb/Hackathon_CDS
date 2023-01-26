# Databricks notebook source
# MAGIC %md # Run a Query

# COMMAND ----------

# DBTITLE 1,You can run a SQL query in a notebook cell
# MAGIC %sql
# MAGIC select * 
# MAGIC from himanshu_gupta_demos.hackathon.weather_data

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="files/cummins_hackathon_images/Query_Editor_Preview.png" width="600"> 
# MAGIC <img src="files/cummins_hackathon_images/Select_Query_Editor.png" width="600"> 
# MAGIC <img src="files/cummins_hackathon_images/SQL_Persona_Selection.png" width="600"> 

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://github.com/josh-melton-db/hackathon-images/blob/main/Query-Editor-Preview.png?raw=true" width="600">

# COMMAND ----------

# in DBSQL, you can save a query separately, which will be the foundation for our visualizations

# COMMAND ----------

# MAGIC %md # Build a Visualization

# COMMAND ----------

# once you have your query running, you can create a visualiztion of that data

# COMMAND ----------

# there are many different types of visualizations that can be a applied to the results of your query

# COMMAND ----------

# you can add various visualizations to dashboards

# COMMAND ----------

# MAGIC %md # Create a Dashboard

# COMMAND ----------

# dashboards provide an end-user facing interface for your visualizations

# COMMAND ----------

# dashboards can be set to refresh on a regular schedule

# COMMAND ----------

# MAGIC %md # Add a Filter

# COMMAND ----------

# when the visualizations on the dashboard are derived from the same query, they can share a filter

# COMMAND ----------

# MAGIC %md # Create an alert

# COMMAND ----------

# queries can also be the basis of an alert, which can take the form of an email or a teams notification

# COMMAND ----------

# alerts are based on whether query results match a criteria that you specify

# COMMAND ----------


