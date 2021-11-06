create table region
(
  r_regionkey INT,
  r_name      VARCHAR(25),
  r_comment   VARCHAR(152)
);

create table lineitem
(
  l_orderkey      INT not null,
  l_partkey       INT not null,
  l_suppkey       INT not null,
  l_linenumber    INT not null,
  l_quantity      FLOAT not null,
  l_extendedprice FLOAT not null,
  l_discount      FLOAT not null,
  l_tax           FLOAT not null,
  l_returnflag    CHAR(1),
  l_linestatus    CHAR(1),
  l_shipdate      DATE,
  l_commitdate    DATE,
  l_receiptdate   DATE,
  l_shipinstruct  CHAR(25),
  l_shipmode      CHAR(10),
  l_comment       VARCHAR(44)
);

create table nation
(
  n_nationkey INTEGER not null,
  n_name      CHAR(25),
  n_regionkey INTEGER,
  n_comment   VARCHAR(152)
);


create table customer
(
  c_custkey    INTEGER not null,
  c_name       VARCHAR(25),
  c_address    VARCHAR(40),
  c_nationkey  INTEGER,
  c_phone      CHAR(15),
  c_acctbal    FLOAT,
  c_mktsegment CHAR(10),
  c_comment    VARCHAR(117)
);

create table supplier
(
  s_suppkey   INTEGER not null,
  s_name      CHAR(25),
  s_address   VARCHAR(40),
  s_nationkey INTEGER,
  s_phone     CHAR(15),
  s_acctbal   FLOAT,
  s_comment   VARCHAR(101)
);


create table part
(
  p_partkey     INTEGER not null,
  p_name        VARCHAR(55),
  p_mfgr        CHAR(25),
  p_brand       CHAR(10),
  p_type        VARCHAR(25),
  p_size        INTEGER,
  p_container   CHAR(10),
  p_retailprice FLOAT,
  p_comment     VARCHAR(23)
);


create table partsupp
(
  ps_partkey    INTEGER not null,
  ps_suppkey    INTEGER not null,
  ps_availqty   INTEGER,
  ps_supplycost FLOAT not null,
  ps_comment    VARCHAR(199)
);


create table orders
(
  o_orderkey      INTEGER not null,
  o_custkey       INTEGER not null,
  o_orderstatus   CHAR(1),
  o_totalprice    FLOAT,
  o_orderdate     DATE,
  o_orderpriority CHAR(15),
  o_clerk         CHAR(15),
  o_shippriority  INTEGER,
  o_comment       VARCHAR(79)
);
