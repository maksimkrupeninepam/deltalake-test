-- Databricks notebook source
MERGE INTO business_layer.customer
USING (
  select
    c_custkey,
    c_name,
    c_address,
    c_nationkey,
    c_phone,
    c_acctbal,
    c_mktsegment,
    c_comment   
  from  
    (SELECT
       c_custkey,
       c_name,
       c_address,
       c_nationkey,
       c_phone,
       c_acctbal,
       c_mktsegment,
       c_comment,
       row_number() over (partition by c_custkey 
           order by _commit_version desc) as rn
     from table_changes('raw_area.customer',0))
  where rn = 1) src
  on src.c_custkey = customer.c_custkey
  WHEN MATCHED THEN
    UPDATE SET c_name = src.c_name, c_address =
    src.c_address, c_nationkey = src.c_nationkey,
    c_phone = src.c_phone, c_acctbal = src.c_acctbal,
    c_mktsegment = src.c_mktsegment, c_comment = src.c_comment 
  WHEN NOT MATCHED THEN
    INSERT *

-- COMMAND ----------

insert into business_layer.cdc_log
  select
    'customer',
    max(_commit_version),
    current_timestamp()
  from table_changes('raw_area.customer',0)

-- COMMAND ----------


