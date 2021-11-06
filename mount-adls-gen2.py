# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://test-data-lake-mk@splunkteststore.blob.core.windows.net",
  mount_point = "/mnt/delta-lake",
  extra_configs = {"fs.azure.sas.test-data-lake-mk.splunkteststore.blob.core.windows.net":dbutils.secrets.get(scope = "db_scope", key = "sas_key")})

# COMMAND ----------

# dbutils.fs.ls("/mnt/delta-lake")
# dbutils.fs.unmount("/mnt/delta-lake")

# COMMAND ----------

# bash
# databricks secrets create-scope --scope db_scope
# databricks secrets put --scope db_scope --key splunkteststore_acc_key
# databricks secrets put --scope db_scope --key splunkteststore_acc_key
# databricks secrets list --scope sf_scope

# COMMAND ----------

# MAGIC %sql
# MAGIC create table default.customer_test
# MAGIC (
# MAGIC   c_custkey    INT,
# MAGIC   c_name       STRING,
# MAGIC   c_address    STRING,
# MAGIC   c_nationkey  INT,
# MAGIC   c_phone      STRING,
# MAGIC   c_acctbal    FLOAT,
# MAGIC   c_mktsegment STRING,
# MAGIC   c_comment    STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/delta-lake/customer_test'
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into default.customer_test
# MAGIC select * from raw_area.customer
