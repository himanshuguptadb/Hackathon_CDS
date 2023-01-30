# Databricks notebook source
# MAGIC %md # Run a Query

# COMMAND ----------

# DBTITLE 1,You can run a SQL query in a notebook cell, and add visualizations within the notebook
# MAGIC %sql
# MAGIC select * 
# MAGIC from himanshu_gupta_demos.hackathon.weather_data

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# DBTITLE 1,However, for a more interactive querying and reporting environment open the Databricks SQL persona
# MAGIC %md
# MAGIC <span>Select "SQL" in the persona switcher</span><br>
# MAGIC <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/SQL-Persona-Selection.png?raw=true" width="600">
# MAGIC <br><br>
# MAGIC <br><span>Select "SQL Editor" in the side menu</span><br>
# MAGIC <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/Select-Query-Editor.png?raw=true" width="600">

# COMMAND ----------

# MAGIC %md
# MAGIC <span>Make sure you're connected to a SQL warehouse. Find the database and table you'd like to query</span><br>
# MAGIC <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/Query-Editor-Preview.png?raw=true" width="600">
# MAGIC <br>
# MAGIC <br><br>
# MAGIC <span>In DBSQL you can save a query separately, which will be the foundation for our visualizations</span><br>
# MAGIC <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/save-query.png?raw=true" width="600">

# COMMAND ----------

# MAGIC %md # Build a Visualization

# COMMAND ----------

# MAGIC %md
# MAGIC <span>Once you have your query running, you can create a visualiztion of that data</span><br>
# MAGIC <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/add-visualization.png?raw=true" width="600">

# COMMAND ----------

# MAGIC %md
# MAGIC <span>There are many different types of visualizations that can be used to show the results of your query</span><br>
# MAGIC <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/create-visualization.png?raw=true" width="600">

# COMMAND ----------

# MAGIC %md # Create a Dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC <span>Dashboards provide an end-user facing interface for your visualizations</span><br>
# MAGIC <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/create-dashboard.png?raw=true" width="600">

# COMMAND ----------

# MAGIC %md
# MAGIC <span>You can add visualizations to a dashboard</span><br>
# MAGIC <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/add-visualization-to-dashboard.png?raw=true" width="600">

# COMMAND ----------

# MAGIC %md
# MAGIC <span>Dashboards can be set to refresh on a regular schedule</span><br>
# MAGIC <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/schedule-dash-refresh.png?raw=true" width="600">

# COMMAND ----------

# MAGIC %md # Add a Filter

# COMMAND ----------

# MAGIC %md
# MAGIC <span>When the visualizations on the dashboard are derived from the same query, they can share a filter. Try creating a dashboard filter on your visualization</span><br>
# MAGIC <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/add-filter.png?raw=true" width="600">

# COMMAND ----------

# MAGIC %md # Create an alert

# COMMAND ----------

# MAGIC %md
# MAGIC <span>Queries can also be the basis of an automated alert, which can take the form of an email or a teams notification</span><br>
# MAGIC <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/create-alert.png?raw=true" width="600">

# COMMAND ----------

# MAGIC %md
# MAGIC <span>Alerts are based on whether query results match a criteria that you specify</span><br>
# MAGIC <img src="https://github.com/himanshuguptadb/Hackathon_CDS/blob/master/Images/alert-threshold.png?raw=true" width="600">

# COMMAND ----------


