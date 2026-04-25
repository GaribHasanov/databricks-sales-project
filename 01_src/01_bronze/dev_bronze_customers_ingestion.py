# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.functions import col
df = spark.read.csv(
    "/Volumes/dev/00_landing_zone/01_managed_volume/sales_customers_2026_04_10_12_45_34.csv",
    header=True,
    inferSchema=True
)


# COMMAND ----------

df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("dev.01_bronze.sales_customers")

# COMMAND ----------

cust = spark.table("dev.01_bronze.sales_customers")
display(cust)

# COMMAND ----------


