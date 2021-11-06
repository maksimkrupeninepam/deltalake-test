-- Databricks notebook source
MERGE INTO business_layer.nation
USING (
SELECT 
  n_nationkey,
  n_name,
  n_regionkey,
  n_comment,
  raw_commit_version
from  
(SELECT 
  n_nationkey,
  n_name,
  n_regionkey,
  n_comment,
  _commit_version as raw_commit_version
  ,row_number() over (partition by n_nationkey order by _commit_version desc) as rn
FROM table_changes('raw_area.nation',0)
where _commit_version > ifnull((select max(max_raw_commit_version) from business_layer.cdc_log where table_name = 'nation'),0) and _change_type = 'insert')
where rn = 1) src
  ON src.n_nationkey = nation.n_nationkey
  WHEN MATCHED THEN
    UPDATE SET n_name = src.n_name, n_regionkey = src.n_regionkey, n_comment = src.n_comment, raw_commit_version = src.raw_commit_version
  WHEN NOT MATCHED THEN
    INSERT (n_nationkey, n_name, n_regionkey, n_comment, raw_commit_version) VALUES (n_nationkey, n_name, n_regionkey, n_comment, raw_commit_version)

-- COMMAND ----------

insert into business_layer.cdc_log
  select
    'nation',
    max(_commit_version),
    current_timestamp()
  from table_changes('raw_area.nation',0)

-- COMMAND ----------

select n_nationkey, n_name, _change_type, _commit_version, _commit_timestamp
from table_changes('raw_area.nation',0)

-- COMMAND ----------


