-- Databricks notebook source
truncate table data_mart.fact_orders

-- COMMAND ----------

insert into data_mart.fact_orders
select
  o_custkey,
  p_partkey as o_partkey,
  s_suppkey as o_suppkey,
  date_part('YEAR', o_orderdate) as d_year,
  round(sum(l_extendedprice),2) as o_totalprice,
  round(sum(l_discount),2) as o_discount,
  round(sum(l_tax),2) as o_tax,
  count(*) as o_number_of_line
from business_layer.orders
join business_layer.lineitem on o_orderkey = l_orderkey
join data_mart.dim_customer on o_custkey = c_custkey
join data_mart.dim_part on l_partkey = p_partkey
join data_mart.dim_supplier on l_suppkey = s_suppkey
group by
  o_custkey,
  p_partkey,
  s_suppkey,
  date_part('YEAR', o_orderdate)

-- COMMAND ----------


