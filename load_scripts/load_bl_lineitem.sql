-- Databricks notebook source
MERGE INTO business_layer.lineitem
USING (
  select
    l_orderkey,
    l_partkey,
    l_suppkey,
    l_linenumber,
    l_quantity,
    l_extendedprice,
    l_discount,
    l_tax,
    l_returnflag,
    l_linestatus,
    l_shipdate,
    l_commitdate,
    l_receiptdate,
    l_shipinstruct,
    l_shipmode,
    l_comment   
  from  
    (SELECT
       l_orderkey,
       l_partkey,
       l_suppkey,
       l_linenumber,
       l_quantity,
       l_extendedprice,
       l_discount,
       l_tax,
       l_returnflag,
       l_linestatus,
       l_shipdate,
       l_commitdate,
       l_receiptdate,
       l_shipinstruct,
       l_shipmode,
       l_comment,
       row_number() over (partition by l_orderkey, l_linenumber order by _commit_version desc) as rn
     from table_changes('raw_area.lineitem',0))
  where rn = 1) src
  on src.l_orderkey = lineitem.l_orderkey and src.l_linenumber = lineitem.l_linenumber
  WHEN MATCHED THEN
    UPDATE SET l_partkey = src.l_partkey, l_suppkey = src.l_suppkey, l_quantity = src.l_quantity, l_extendedprice = src.l_extendedprice, l_discount =                                src.l_discount, l_tax = src.l_tax, l_returnflag = src.l_returnflag, l_linestatus = src.l_linestatus, l_shipdate = src.l_shipdate, l_commitdate = src.l_commitdate, l_receiptdate = src.l_receiptdate, l_shipinstruct = src.l_shipinstruct, l_shipmode = src.l_shipmode, l_comment = src.l_comment
  WHEN NOT MATCHED THEN
    INSERT *

-- COMMAND ----------

insert into business_layer.cdc_log
  select
    'lineitem',
    max(_commit_version),
    current_timestamp()
  from table_changes('raw_area.lineitem',0)
