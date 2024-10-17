# Databricks notebook source
df_sales=spark.table("sales")

# COMMAND ----------

df_customers=spark.table("customers")

# COMMAND ----------

df_customers.display()

# COMMAND ----------

df_customers.filter("customer_id=2").display()

# COMMAND ----------

from pyspark.sql.functions import *
df_customers.where(col("customer_id")==2).display()

# COMMAND ----------

df_customers.sort("customer_city").display()

# COMMAND ----------

df_customers.sort(col("customer_city").desc()).display()

# COMMAND ----------

display(df_sales)

# COMMAND ----------

df_joined = df_sales.join(df_customers,"customer_id","inner")
 

# COMMAND ----------

display(df_joined)

# COMMAND ----------

df_joined.groupBy("customer_id").count().orderBy("customer_id").display()

# COMMAND ----------

df_join= df_sales.join(df_customers,df_sales["customer_id"]==df_customers["customer_id"])

# COMMAND ----------

display(df_join)

# COMMAND ----------


