-- Databricks notebook source
create database raw_area

-- COMMAND ----------

create table raw_area.region
(
  r_regionkey int,
  r_name      string,
  r_comment   string
)
USING DELTA
LOCATION '/mnt/raw-area/region'
TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

create table raw_area.nation
(
  n_nationkey INT,
  n_name      STRING,
  n_regionkey INT,
  n_comment   STRING
)
USING DELTA
LOCATION '/mnt/raw-area/nation'
TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

create table raw_area.lineitem
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
LOCATION '/mnt/raw-area/lineitem'
TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

create table raw_area.orders
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
LOCATION '/mnt/raw-area/orders'
TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

create table raw_area.customer
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
LOCATION '/mnt/raw-area/customer'
TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

create table raw_area.supplier
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
LOCATION '/mnt/raw-area/supplier'
TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

create table raw_area.part
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
LOCATION '/mnt/raw-area/part'
TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

create table raw_area.partsupp
(
  ps_partkey    INT,
  ps_suppkey    INT,
  ps_availqty   INT,
  ps_supplycost FLOAT,
  ps_comment    STRING
)
USING DELTA
LOCATION '/mnt/raw-area/partsupp'
TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

select count(*) from raw_area.orders


-- COMMAND ----------

truncate table raw_area.nation;
truncate table raw_area.region;
truncate table raw_area.customer;
truncate table raw_area.supplier;
truncate table raw_area.part;
truncate table raw_area.partsupp;
truncate table raw_area.orders;
truncate table raw_area.lineitem;

-- COMMAND ----------

SELECT * FROM table_changes('raw_area.nation',0)
where _commit_version = (select max(_commit_version) from table_changes('raw_area.nation',0)) 

-- COMMAND ----------

drop table raw_area.nation;
drop table raw_area.region;
drop table raw_area.customer;
drop table raw_area.part;
drop table raw_area.supplier;
drop table raw_area.partsupp;
drop table raw_area.orders;
drop table raw_area.lineitem;

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.rm("/mnt/raw-area/nation",recurse=true)
-- MAGIC dbutils.fs.rm("/mnt/raw-area/region",recurse=true)
-- MAGIC dbutils.fs.rm("/mnt/raw-area/customer",recurse=true)
-- MAGIC dbutils.fs.rm("/mnt/raw-area/part",recurse=true)
-- MAGIC dbutils.fs.rm("/mnt/raw-area/supplier",recurse=true)
-- MAGIC dbutils.fs.rm("/mnt/raw-area/partsupp",recurse=true)
-- MAGIC dbutils.fs.rm("/mnt/raw-area/orders",recurse=true)
-- MAGIC dbutils.fs.rm("/mnt/raw-area/lineitem",recurse=true)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.rm("/mnt/data-mart/fact_orders",recurse=true)

-- COMMAND ----------

select count(*) from raw_area.lineitem;

-- COMMAND ----------


