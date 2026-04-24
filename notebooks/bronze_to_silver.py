
# Databricks notebook source

# ============================================
# INITIAL SETUP - STORAGE AUTHENTICATION
# ============================================

spark.conf.set("fs.azure.account.auth.type.storagedatapractice.dfs.core.windows.net","OAuth")

spark.conf.set("fs.azure.account.oauth.provider.type.storagedatapractice.dfs.core.windows.net",
"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")

spark.conf.set("fs.azure.account.oauth2.client.id.storagedatapractice.dfs.core.windows.net",
"your-client-id")

spark.conf.set("fs.azure.account.oauth2.client.secret.storagedatapractice.dfs.core.windows.net",
"your-client-secret")

spark.conf.set("fs.azure.account.oauth2.client.endpoint.storagedatapractice.dfs.core.windows.net",
"https://login.microsoftonline.com/your-tenant-id/oauth2/token")

dbutils.fs.ls("abfss://bronze@storagedatapractice.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %md
# MAGIC # RAW DATA EXTRACTION (BRONZE → SILVER)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## LOAD SOURCE FILES

# COMMAND ----------

df_accounts = spark.read.format("csv")\
    .option("header", True)\
    .option("inferSchema", True)\
    .load("abfss://bronze@storagedatapractice.dfs.core.windows.net/accounts")

df_transactions = spark.read.format("csv")\
    .option("header", True)\
    .option("inferSchema", True)\
    .option("recursiveFileLookup", True)\
    .load("abfss://bronze@storagedatapractice.dfs.core.windows.net/transactions")

display(df_accounts)
display(df_transactions)

# COMMAND ----------

# MAGIC %md
# MAGIC # DATA PREPARATION

# COMMAND ----------

# REMOVE DUPLICATES BASED ON BUSINESS KEYS
df_accounts = df_accounts.dropDuplicates(["account_id"])
df_transactions = df_transactions.dropDuplicates()

# HANDLE NULL VALUES
df_transactions = df_transactions.fillna({
    "transaction_amount": 0
})

# FILTER OUT INVALID RECORDS
df_transactions = df_transactions.filter(col("transaction_amount") >= 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DERIVED FEATURES

# COMMAND ----------

# TRANSACTION SIZE CATEGORY
df_transactions = df_transactions.withColumn(
    "txn_size",
    when(col("transaction_amount") > 2000, "LARGE")
    .when(col("transaction_amount") > 500, "MEDIUM")
    .otherwise("SMALL")
)

# TRANSACTION DATE BREAKDOWN
df_transactions = df_transactions.withColumn("txn_date", to_date("transaction_time"))\
                                 .withColumn("txn_year", year("txn_date"))\
                                 .withColumn("txn_month", month("txn_date"))

display(df_transactions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATA ENRICHMENT

# COMMAND ----------

df_combined = df_transactions.join(df_accounts, "account_id", "left")

display(df_combined)

# COMMAND ----------

# MAGIC %md
# MAGIC ## WINDOW ANALYTICS

# COMMAND ----------

win_spec = Window.partitionBy("account_id").orderBy(col("transaction_amount").desc())

df_combined = df_combined.withColumn(
    "txn_rank",
    row_number().over(win_spec)
)

# FLAG TOP TRANSACTION PER ACCOUNT
df_combined = df_combined.withColumn(
    "is_top_txn",
    when(col("txn_rank") == 1, 1).otherwise(0)
)

display(df_combined)

# COMMAND ----------

# MAGIC %md
# MAGIC ## AGGREGATED SNAPSHOT

# COMMAND ----------

df_metrics = df_combined.groupBy("txn_year", "txn_month")\
    .agg(
        sum("transaction_amount").alias("monthly_volume"),
        count("*").alias("txn_count"),
        avg("transaction_amount").alias("avg_txn_value")
    )

display(df_metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC # STORE CURATED DATA (SILVER)

# COMMAND ----------

df_combined.write.format("delta")\
    .mode("overwrite")\
    .save("abfss://silver@storagedatapractice.dfs.core.windows.net/transactions_clean")

df_metrics.write.format("delta")\
    .mode("overwrite")\
    .save("abfss://silver@storagedatapractice.dfs.core.windows.net/monthly_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC # SILVER PROCESS COMPLETED
