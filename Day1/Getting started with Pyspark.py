# Databricks notebook source
print("hello")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Spark Core.
# MAGIC
# MAGIC RDD Dataframe (Resilient distributed dataset)

# COMMAND ----------

# MAGIC %md
# MAGIC DataFrame: Structured API

# COMMAND ----------

data=[(1,'a',10),(2,'b',20)]
schema=["id","name","age"]
df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

data=[(1,'a',10),(2,'b',20)]
schema="id int,name string,age int"
df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

data=[(1,'a',10),(2,'b',20)]

# COMMAND ----------

df=spark.createDataFrame(data)

# COMMAND ----------

display(df)

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC DataFrame Functions
# MAGIC .select
# MAGIC .alias
# MAGIC .withcolumnrenamed
# MAGIC .withcolumnsrenamed

# COMMAND ----------



# COMMAND ----------

df.select("*")

# COMMAND ----------

df.select("*").display()

# COMMAND ----------

df.select("id","age").display()

# COMMAND ----------

df1=df.select("id","age")

# COMMAND ----------

df1.display()

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(col("id").alias("emp_id"),"age").display()

# COMMAND ----------

help(df.withColumnRenamed)

# COMMAND ----------

df.withColumnRenamed("id","emp_id").display()

# COMMAND ----------

df_new = df.withColumnsRenamed({"id":"emp_id","name":"emp_name","age":"emp_age"})

# COMMAND ----------

display(df_new)

# COMMAND ----------

df.withColumn("age",current_date()).display()

# COMMAND ----------

df.withColumn("Current_date",current_date()).display()

# COMMAND ----------


