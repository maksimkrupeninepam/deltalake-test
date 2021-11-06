-- Databricks notebook source
create database data_mart

-- COMMAND ----------

create table data_mart.dim_customer
(
  c_custkey    INT,
  c_name       STRING,
  c_address    STRING,
  c_nation     STRING,
  c_region     STRING,
  c_phone      STRING,
  c_acctbal    FLOAT,
  c_mktsegment STRING,
  c_comment    STRING
)
USING DELTA
LOCATION '/mnt/data-mart/dim_customer'
TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

create table data_mart.dim_supplier
(
  s_suppkey   INT,
  s_name      STRING,
  s_address   STRING,
  s_nation    STRING,
  s_region    STRING,
  s_phone     STRING,
  s_acctbal   FLOAT,
  s_comment   STRING
)
USING DELTA
LOCATION '/mnt/data-mart/dim_supplier'
TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

create table data_mart.dim_part
(
  p_partkey     INT,
  p_name        STRING,
  p_mfgr        STRING,
  p_brand       STRING,
  p_type        STRING,
  p_size        INT,
  p_container   STRING,
  p_retailprice FLOAT,
  p_comment     STRING
)
USING DELTA
LOCATION '/mnt/data-mart/dim_part'
TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

create table data_mart.fact_orders
(
  o_custkey       INT,
  o_partkey       INT,
  o_suppkey       INT,
  d_year          INT,
  o_totalprice    FLOAT,
  o_discount      FLOAT,
  o_tax           FLOAT,
  o_number_of_line INT
)
USING DELTA
LOCATION '/mnt/data-mart/fact_orders'
TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

create table data_mart.dim_date
(
  d_year       INT
)
USING DELTA
LOCATION '/mnt/data-mart/dim_date'
TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

create table data_mart.fact_orders_agg
(
  d_year          INT,
  o_totalprice    FLOAT,
  o_discount      FLOAT,
  o_tax           FLOAT,
  o_number_of_line INT
)
USING DELTA
LOCATION '/mnt/data-mart/fact_orders_agg'
TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

create table data_mart.report_tpch_16
(
  p_brand      STRING,
  p_type       STRING,
  p_size       INT,
  supplier_cnt INT
)
USING DELTA
LOCATION '/mnt/data-mart/report_tpch_16'
TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------


