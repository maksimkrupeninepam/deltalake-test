-- Databricks notebook source
MERGE INTO business_layer.region
USING (
  select
    r_regionkey,
    r_name,
    r_comment   
  from  
    (SELECT
       r_regionkey,
       r_name,
       r_comment,
       row_number() over (partition by r_regionkey order by _commit_version desc) as rn
     from table_changes('raw_area.region',0))
  where rn = 1) src
  on src.r_regionkey = region.r_regionkey
  WHEN MATCHED THEN
    UPDATE SET r_name = src.r_name, r_comment = src.r_comment
  WHEN NOT MATCHED THEN
    INSERT *

-- COMMAND ----------

insert into business_layer.cdc_log
  select
    'region',
    max(_commit_version),
    current_timestamp()
  from table_changes('raw_area.region',0)

-- COMMAND ----------


