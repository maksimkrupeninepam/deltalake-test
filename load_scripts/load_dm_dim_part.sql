-- Databricks notebook source
truncate table data_mart.dim_part

-- COMMAND ----------

insert into data_mart.dim_part
select 
  *
from business_layer.part
