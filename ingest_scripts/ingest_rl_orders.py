# Databricks notebook source
# Use secrets DBUtil to get Snowflake credentials.
user = dbutils.secrets.get("sf_scope", "sf_user")
password = dbutils.secrets.get("sf_scope", "sf_psw")

# snowflake connection options
options = {
  "sfUrl": "epampartner.snowflakecomputing.com",
  "sfUser": user,
  "sfPassword": password,
  "sfDatabase": "SNOWFLAKE_SAMPLE_DATA",
  "sfSchema": "TPCH_SF1",
  "sfWarehouse": "DEMO_WH"
}

# COMMAND ----------

# Read the data written by the previous cell back.
df = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.orders") \
  .load()
#display(df)

# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable("raw_area.orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from raw_area.orders
# MAGIC --truncate table raw_area.orders
# MAGIC --ALTER TABLE raw_area.orders SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------


