-- Databricks notebook source
MERGE INTO business_layer.partsupp
USING (
  select
    ps_partkey,
    ps_suppkey,
    ps_availqty,
    ps_supplycost,
    ps_comment   
  from  
    (SELECT
       ps_partkey,
       ps_suppkey,
       ps_availqty,
       ps_supplycost,
       ps_comment,
       row_number() over (partition by ps_partkey, ps_suppkey order by _commit_version desc) as rn
     from table_changes('raw_area.partsupp',0))
  where rn = 1) src
  on src.ps_partkey = partsupp.ps_partkey and src.ps_suppkey = partsupp.ps_suppkey
  WHEN MATCHED THEN
    UPDATE SET ps_availqty = src.ps_availqty, ps_supplycost = src.ps_supplycost, ps_comment = src.ps_comment
  WHEN NOT MATCHED THEN
    INSERT *

-- COMMAND ----------

insert into business_layer.cdc_log
  select
    'partsupp',
    max(_commit_version),
    current_timestamp()
  from table_changes('raw_area.partsupp',0)
