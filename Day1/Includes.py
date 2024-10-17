# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

input_path="/Volumes/suchith_databricks/default/raw/"

# COMMAND ----------

def add_ingestion(df):
    df1=df.withColumn("ingestion_time",current_date())
    return df1

# COMMAND ----------


