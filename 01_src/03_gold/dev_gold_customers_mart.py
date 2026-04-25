# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.functions import col

# COMMAND ----------

silver_table = "dev.02_silver.sales_customers"
gold_table = "dev.03_gold.sales_customers"


# COMMAND ----------

df = spark.table(silver_table)

# COMMAND ----------

df.printSchema()
display(df.limit(10))
df.count()

# COMMAND ----------

df_clean = df.dropna(subset=["customerID"]).fillna("unknown", subset=["email_address"])

# COMMAND ----------

df_clean = df_clean.dropDuplicates(["customerID"])

# COMMAND ----------

df_clean.printSchema()
display(df_clean.limit(10))

# COMMAND ----------

df_clean.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(gold_table)

# COMMAND ----------


