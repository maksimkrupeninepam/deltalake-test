-- Databricks notebook source
truncate table data_mart.dim_supplier

-- COMMAND ----------

insert into data_mart.dim_supplier
select 
  s_suppkey,
  s_name,
  s_address,
  n_name as c_nation,
  r_name as c_region,
  s_phone,
  s_acctbal,
  s_comment
from business_layer.supplier
join business_layer.nation on n_nationkey = s_nationkey
join business_layer.region on n_regionkey = r_regionkey
