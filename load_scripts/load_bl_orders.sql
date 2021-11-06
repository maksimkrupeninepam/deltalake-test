-- Databricks notebook source
MERGE INTO business_layer.orders
USING (
  select
    o_orderkey,
    o_custkey,
    o_orderstatus,
    o_totalprice,
    o_orderdate,
    o_orderpriority,
    o_clerk,
    o_shippriority,
    o_comment   
  from  
    (SELECT
       o_orderkey,
       o_custkey,
       o_orderstatus,
       o_totalprice,
       o_orderdate,
       o_orderpriority,
       o_clerk,
       o_shippriority,
       o_comment,
       row_number() over (partition by o_orderkey order by _commit_version desc) as rn
     from table_changes('raw_area.orders',1))
  where rn = 1) src
  on src.o_orderkey = orders.o_orderkey
  WHEN MATCHED THEN
    UPDATE SET o_custkey = src.o_custkey, o_orderstatus = src.o_orderstatus, o_totalprice = src.o_totalprice, o_orderdate = src.o_orderdate, o_orderpriority =                                src.o_orderpriority, o_clerk = src.o_clerk, o_shippriority = src.o_shippriority, o_comment = src.o_comment
  WHEN NOT MATCHED THEN
    INSERT *

-- COMMAND ----------

insert into business_layer.cdc_log
  select
    'orders',
    max(_commit_version),
    current_timestamp()
  from table_changes('raw_area.orders',1)

-- COMMAND ----------


