-- Databricks notebook source
MERGE INTO business_layer.supplier
USING (
  select
    s_suppkey,
    s_name,
    s_address,
    s_nationkey,
    s_phone,
    s_acctbal,
    s_comment   
  from  
    (SELECT
       s_suppkey,
       s_name,
       s_address,
       s_nationkey,
       s_phone,
       s_acctbal,
       s_comment,
       row_number() over (partition by s_suppkey order by _commit_version desc) as rn
     from table_changes('raw_area.supplier',0))
  where rn = 1) src
  on src.s_suppkey = supplier.s_suppkey
  WHEN MATCHED THEN
    UPDATE SET s_name = src.s_name, s_address = src.s_address, s_nationkey = src.s_nationkey, s_phone = src.s_phone, s_acctbal = src.s_acctbal, s_comment =                                src.s_comment 
  WHEN NOT MATCHED THEN
    INSERT *

-- COMMAND ----------

insert into business_layer.cdc_log
  select
    'supplier',
    max(_commit_version),
    current_timestamp()
  from table_changes('raw_area.supplier',0)

-- COMMAND ----------


