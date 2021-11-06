-- Databricks notebook source
create database business_layer

-- COMMAND ----------

create table business_layer.nation
(
  n_nationkey INT,
  n_name      STRING,
  n_regionkey INT,
  n_comment   STRING,
  raw_commit_version INT
)
USING DELTA
LOCATION '/mnt/business-layer/nation'
TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

create table business_layer.customer
(
  c_custkey    INT,
  c_name       STRING,
  c_address    STRING,
  c_nationkey  INT,
  c_phone      STRING,
  c_acctbal    FLOAT,
  c_mktsegment STRING,
  c_comment    STRING
)
USING DELTA
LOCATION '/mnt/business-layer/customer'
TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

create table business_layer.cdc_log
(
  table_name STRING,
  max_raw_commit_version STRING,
  batch_datatime TIMESTAMP
)
USING DELTA
LOCATION '/mnt/business-layer/cdc_log'
TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

create table business_layer.region
(
  r_regionkey int,
  r_name      string,
  r_comment   string
)
USING DELTA
LOCATION '/mnt/business-layer/region'
TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

create table business_layer.supplier
(
  s_suppkey   INT,
  s_name      STRING,
  s_address   STRING,
  s_nationkey INT,
  s_phone     STRING,
  s_acctbal   FLOAT,
  s_comment   STRING
)
USING DELTA
LOCATION '/mnt/business-layer/supplier'
TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

create table business_layer.part
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
LOCATION '/mnt/business-layer/part'
TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

create table business_layer.partsupp
(
  ps_partkey    INT,
  ps_suppkey    INT,
  ps_availqty   INT,
  ps_supplycost FLOAT,
  ps_comment    STRING
)
USING DELTA
LOCATION '/mnt/business-layer/partsupp'
TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

create table business_layer.orders
(
  o_orderkey      INT,
  o_custkey       INT,
  o_orderstatus   STRING,
  o_totalprice    FLOAT,
  o_orderdate     DATE,
  o_orderpriority STRING,
  o_clerk         STRING,
  o_shippriority  INT,
  o_comment       STRING
)
USING DELTA
LOCATION '/mnt/business-layer/orders'
TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

create table business_layer.lineitem
(
  l_orderkey      INT,
  l_partkey       INT,
  l_suppkey       INT,
  l_linenumber    FLOAT,
  l_quantity      FLOAT,
  l_extendedprice FLOAT,
  l_discount      FLOAT,
  l_tax           FLOAT,
  l_returnflag    STRING,
  l_linestatus    STRING,
  l_shipdate      DATE,
  l_commitdate    DATE,
  l_receiptdate   DATE,
  l_shipinstruct  STRING,
  l_shipmode      STRING,
  l_comment       STRING
)
USING DELTA
LOCATION '/mnt/business-layer/lineitem'
TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

SELECT * FROM business_layer.cdc_log

-- COMMAND ----------

SELECT * FROM business_layer.region order by r_name

-- COMMAND ----------

truncate table business_layer.nation;
truncate table business_layer.region;
truncate table business_layer.customer;
truncate table business_layer.supplier;
truncate table business_layer.part;
truncate table business_layer.partsupp;
truncate table business_layer.orders;
truncate table business_layer.lineitem;
truncate table business_layer.cdc_log;

-- COMMAND ----------

select count(*) from business_layer.lineitem

-- COMMAND ----------


