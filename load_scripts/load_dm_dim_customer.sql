-- Databricks notebook source
truncate table data_mart.dim_customer

-- COMMAND ----------

insert into data_mart.dim_customer
select 
  c_custkey,
  c_name,
  c_address,
  n_name as c_nation,
  r_name as c_region,
  c_phone,
  c_acctbal,
  c_mktsegment,
  c_comment
from business_layer.customer
join business_layer.nation on n_nationkey = c_nationkey
join business_layer.region on n_regionkey = r_regionkey
