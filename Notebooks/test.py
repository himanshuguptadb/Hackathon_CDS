# Databricks notebook source
import bamboolib as bam
bam

# COMMAND ----------

engine_events_df = spark.table("himanshu_gupta_demos.hackathon.engine_events").toPandas()
engine_events_df
