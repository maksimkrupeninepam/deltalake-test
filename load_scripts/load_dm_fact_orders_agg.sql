-- Databricks notebook source
truncate table data_mart.fact_orders_agg

-- COMMAND ----------

insert into data_mart.fact_orders_agg
select
  d_year,
  round(sum(o_totalprice),2) as o_totalprice,
  round(sum(o_discount),2) as o_discount,
  round(sum(o_tax),2) as o_tax,
  sum(o_number_of_line) as o_number_of_line
from data_mart.fact_orders
group by d_year

-- COMMAND ----------


