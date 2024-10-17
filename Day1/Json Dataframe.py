# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

json_data = {
	"id": "0001",
	"type": "donut",
	"name": "Cake",
	"ppu": 0.55,
	"batters":
		{
			"batter":
				[
					{ "id": "1001", "type": "Regular" },
					{ "id": "1002", "type": "Chocolate" },
					{ "id": "1003", "type": "Blueberry" },
					{ "id": "1004", "type": "Devil's Food" }
				]
		},
	"topping":
		[
			{ "id": "5001", "type": "None" },
			{ "id": "5002", "type": "Glazed" },
			{ "id": "5005", "type": "Sugar" },
			{ "id": "5007", "type": "Powdered Sugar" },
			{ "id": "5006", "type": "Chocolate with Sprinkles" },
			{ "id": "5003", "type": "Chocolate" },
			{ "id": "5004", "type": "Maple" }
		]
}

# COMMAND ----------

df = spark.createDataFrame([json_data])
display(df)

# COMMAND ----------

df1 = df.withColumn("batters", explode("batters.batter"))

# COMMAND ----------

display(df1)

# COMMAND ----------

df2 = df1.withColumn("batter_type", col("batters.type")) \
         .withColumn("batter_id", col("batters.id"))

display(df2)

# COMMAND ----------

df2.withColumn("topping",explode("topping")).display()

# COMMAND ----------

df3 = df2.withColumn("topping", explode("topping")) \
         .withColumn("topping_type", col("topping.type")) \
         .withColumn("topping_id", col("topping.id"))

display(df3)

# COMMAND ----------


