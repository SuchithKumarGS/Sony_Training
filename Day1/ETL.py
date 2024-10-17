# Databricks notebook source
# MAGIC %run /Workspace/Users/suchithkumargs98@gmail.com/Day1/Includes

# COMMAND ----------

# DBTITLE 1,Read
df_sales=spark.read.csv(f"{input_path}order_dates.csv", header=True,inferSchema=True)

# COMMAND ----------

df_sales1= add_ingestion(df_sales)

# COMMAND ----------

df_sales1.display()

# COMMAND ----------

df_sales1.write.mode("overwrite").saveAsTable("order_dates")

# COMMAND ----------


