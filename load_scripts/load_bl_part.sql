-- Databricks notebook source
MERGE INTO business_layer.part
USING (
  select
    p_partkey,
    p_name,
    p_mfgr,
    p_brand,
    p_type,
    p_size,
    p_container,
    p_retailprice,
    p_comment   
  from  
    (SELECT
       p_partkey,
       p_name,
       p_mfgr,
       p_brand,
       p_type,
       p_size,
       p_container,
       p_retailprice,
       p_comment,
       row_number() over (partition by p_partkey order by _commit_version desc) as rn
     from table_changes('raw_area.part',0))
  where rn = 1) src
  on src.p_partkey = part.p_partkey
  WHEN MATCHED THEN
    UPDATE SET p_name = src.p_name, p_mfgr = src.p_mfgr, p_brand = src.p_brand, p_type = src.p_type, p_size = src.p_size, p_container =                                src.p_container, p_retailprice = src.p_retailprice, p_comment = src.p_comment 
  WHEN NOT MATCHED THEN
    INSERT *

-- COMMAND ----------

insert into business_layer.cdc_log
  select
    'part',
    max(_commit_version),
    current_timestamp()
  from table_changes('raw_area.part',0)

-- COMMAND ----------


