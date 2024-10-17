# Databricks notebook source
df=spark.read.csv("/Volumes/suchith_databricks/default/raw/sales.csv", header=True,inferSchema=True)

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.saveAsTable("sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales

# COMMAND ----------

df_customers=spark.read.json("/Volumes/suchith_databricks/default/raw/customers.json")

# COMMAND ----------

display(df_customers)

# COMMAND ----------

df_customers.write.saveAsTable("customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers

# COMMAND ----------

# MAGIC %run /Workspace/Users/suchithkumargs98@gmail.com/Day1/Includes

# COMMAND ----------

df_sales=spark.read.csv(f"{input_path}products.json")
