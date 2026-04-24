# Databricks notebook source

# MAGIC %md
# MAGIC # BUSINESS LAYER (SILVER → GOLD)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## LOAD CURATED DATA

# COMMAND ----------

df_txn = spark.read.format("delta")\
    .load("abfss://silver@storagedatapractice.dfs.core.windows.net/transactions_clean")

df_monthly = spark.read.format("delta")\
    .load("abfss://silver@storagedatapractice.dfs.core.windows.net/monthly_metrics")

display(df_txn)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CUSTOMER SPENDING SUMMARY

# COMMAND ----------

df_customer_spend = df_txn.groupBy("account_id")\
    .agg(
        sum("transaction_amount").alias("total_spent"),
        count("*").alias("total_txns")
    )

# SEGMENT USERS
df_customer_spend = df_customer_spend.withColumn(
    "customer_band",
    when(col("total_spent") > 8000, "PREMIUM")
    .when(col("total_spent") > 3000, "STANDARD")
    .otherwise("BASIC")
)

display(df_customer_spend)

# COMMAND ----------

# MAGIC %md
# MAGIC ## TOP PERFORMERS

# COMMAND ----------

df_top = df_customer_spend.orderBy(col("total_spent").desc()).limit(15)

display(df_top)

# COMMAND ----------

# MAGIC %md
# MAGIC ## TREND ANALYSIS

# COMMAND ----------

windowSpec = Window.orderBy("txn_year", "txn_month")

df_trend = df_monthly.withColumn(
    "cumulative_volume",
    sum("monthly_volume").over(windowSpec)
)

display(df_trend)

# COMMAND ----------

# MAGIC %md
# MAGIC ## HIGH VALUE TRANSACTION INSIGHTS

# COMMAND ----------

df_high_value = df_txn.filter(col("transaction_amount") > 1000)

df_high_summary = df_high_value.groupBy("txn_year")\
    .agg(
        count("*").alias("high_value_count"),
        sum("transaction_amount").alias("high_value_total")
    )

display(df_high_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC # WRITE FINAL DATASETS

# COMMAND ----------

df_customer_spend.write.format("delta")\
    .mode("overwrite")\
    .save("abfss://gold@storagedatapractice.dfs.core.windows.net/customer_summary")

df_top.write.format("delta")\
    .mode("overwrite")\
    .save("abfss://gold@storagedatapractice.dfs.core.windows.net/top_customers")

df_trend.write.format("delta")\
    .mode("overwrite")\
    .save("abfss://gold@storagedatapractice.dfs.core.windows.net/business_trends")

df_high_summary.write.format("delta")\
    .mode("overwrite")\
    .save("abfss://gold@storagedatapractice.dfs.core.windows.net/high_value_insights")

# COMMAND ----------

# MAGIC %md
# MAGIC # GOLD LAYER READY FOR REPORTING
