# Databricks notebook source
# DBTITLE 1,Querying the files
# MAGIC %sql
# MAGIC select * from file_format.`path`

# COMMAND ----------

# DBTITLE 1,CATS
# MAGIC %sql
# MAGIC create table customers as
# MAGIC select *,current_timestamp() as ingestion_time from json.`/Volumes/suchith_databricks/default/raw/customers.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers

# COMMAND ----------

# MAGIC %sql
# MAGIC create table products as
# MAGIC select *,current_timestamp() as ingestion_time from json.`/Volumes/suchith_databricks/default/raw/products.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from products

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales s
# MAGIC left join customers c on s.customer_id=c.customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select c.customer_id,count(*)
# MAGIC from sales s  
# MAGIC inner join customers c on c.customer_id= s.customer_id
# MAGIC group by c.customer_id
# MAGIC order by c.customer_id

# COMMAND ----------


