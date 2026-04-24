# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.functions import col
df = spark.read.csv(
    "/Volumes/dev/00_landing_zone/01_managed_volume/sales_transactions_2026_04_10_14_17_47.csv",
    header=True,
    inferSchema=True
)


# COMMAND ----------

df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("dev.01_bronze.sales_transactions")

# COMMAND ----------

cust = spark.table("dev.01_bronze.sales_transactions")

# COMMAND ----------

cust.printSchema()
display(cust.limit(10))
df.count()

# COMMAND ----------


