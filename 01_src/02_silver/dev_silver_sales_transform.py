# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.functions import col

# COMMAND ----------

bronze_table = "dev.01_bronze.sales_transactions"
silver_table = "dev.02_silver.sales_transactions"

# COMMAND ----------

df = spark.table(bronze_table)

# COMMAND ----------

df.printSchema()
display(df.limit(10))
df.count()

# COMMAND ----------

df_clean = df.dropna(subset=["transactionID"]).fillna("unknown", subset=["customerID"])

# COMMAND ----------

df_clean = (

    df_clean
    .withColumn("customerID", F.col("customerID").cast("integer") )
    .withColumn("paymentMethod", F.lower(F.col("paymentMethod")) )
    )

# COMMAND ----------

df_clean = df_clean.dropDuplicates(["transactionID"])

# COMMAND ----------

df_clean.printSchema()
display(df_clean.limit(10))

# COMMAND ----------

df_clean.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(silver_table)

# COMMAND ----------


