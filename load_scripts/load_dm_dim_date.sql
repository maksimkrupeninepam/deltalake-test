-- Databricks notebook source
truncate table data_mart.dim_date

-- COMMAND ----------

insert into data_mart.dim_date
select
  distinct date_part('YEAR', o_orderdate) as d_year 
from business_layer.orders

-- COMMAND ----------


