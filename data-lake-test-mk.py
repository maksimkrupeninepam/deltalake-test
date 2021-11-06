# Databricks notebook source
# MAGIC %python
# MAGIC select 1

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://test-data-lake-mk@splunkteststore.blob.core.windows.net",
  mount_point = "/mnt/data-lake",
  extra_configs = {"fs.azure.sas.test-data-lake-mk.splunkteststore.blob.core.windows.net":dbutils.secrets.get(scope = "data-lake-test-mk", key = "adlsgen2-sas-mk")})

# COMMAND ----------

# MAGIC %bash
# MAGIC databricks secrets list

# COMMAND ----------


