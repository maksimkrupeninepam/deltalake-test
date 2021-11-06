-- Databricks notebook source
create or replace table default.perf_results (q_number int, start_time timestamp)

-- COMMAND ----------

truncate table default.perf_results

-- COMMAND ----------

insert into default.perf_results select 1, current_timestamp();
-- 1
select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	raw_area.lineitem
where
	l_shipdate <='1998-12-01' -- interval '91' day (3)
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus;
 
insert into default.perf_results select 2, current_timestamp();


--2
select
	s_acctbal,
	s_name,
	n_name,
	p_partkey,
	p_mfgr,
	s_address,
	s_phone,
	s_comment
from
	raw_area.part,
	raw_area.supplier,
	raw_area.partsupp,
	raw_area.nation,
	raw_area.region
where
	p_partkey = ps_partkey
	and s_suppkey = ps_suppkey
	and p_size = 45
	and p_type like '%BRASS'
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'AFRICA'
	and ps_supplycost = (
		select
			min(ps_supplycost)
		from
			raw_area.partsupp,
			raw_area.supplier,
			raw_area.nation,
			raw_area.region
		where
			p_partkey = ps_partkey
			and s_suppkey = ps_suppkey
			and s_nationkey = n_nationkey
			and n_regionkey = r_regionkey
			and r_name = 'AFRICA'
	)
order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey
	limit 100;

insert into default.perf_results select 3, current_timestamp();

--3
select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	raw_area.customer,
	raw_area.orders,
	raw_area.lineitem
where
	c_mktsegment = 'AUTOMOBILE'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate <'1995-03-01'
	and l_shipdate >'1995-03-01'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate
limit 10;

insert into default.perf_results select 4, current_timestamp();

--4
select
	o_orderpriority,
	count(*) as order_count
from
	raw_area.orders
where
	o_orderdate >='1993-10-01'
	and o_orderdate < add_months('1993-10-01',3)
	and exists (
		select
			*
		from
			raw_area.lineitem
		where
			l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate
	)
group by
	o_orderpriority
order by
	o_orderpriority;
 

insert into default.perf_results select 5, current_timestamp();

--5
select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	raw_area.customer,
	raw_area.orders,
	raw_area.lineitem,
	raw_area.supplier,
	raw_area.nation,
	raw_area.region
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'EUROPE'
	and o_orderdate >='1997-01-01'
	and o_orderdate < add_months('1997-01-01',12)
group by
	n_name
order by
	revenue desc;
 

insert into default.perf_results select 6, current_timestamp();

--6
select
	sum(l_extendedprice * l_discount) as revenue
from
	raw_area.lineitem
where
	l_shipdate >='1996-01-01'
	and l_shipdate < add_months('1996-01-01',12)
	and l_discount between 0.06 - 0.01 and 0.06 + 0.01
	and l_quantity < 24;
 

insert into default.perf_results select 7, current_timestamp();

--7
select
	supp_nation,
	cust_nation,
	l_year,
	sum(volume) as revenue
from
	(
		select
			n1.n_name as supp_nation,
			n2.n_name as cust_nation,
			extract(year from l_shipdate) as l_year,
			l_extendedprice * (1 - l_discount) as volume
		from
			raw_area.supplier,
			raw_area.lineitem,
			raw_area.orders,
			raw_area.customer,
			raw_area.nation n1,
			raw_area.nation n2
		where
			s_suppkey = l_suppkey
			and o_orderkey = l_orderkey
			and c_custkey = o_custkey
			and s_nationkey = n1.n_nationkey
			and c_nationkey = n2.n_nationkey
			and (
				(n1.n_name = 'INDIA' and n2.n_name = 'SAUDI ARABIA')
				or (n1.n_name = 'SAUDI ARABIA' and n2.n_name = 'INDIA')
			)
			and l_shipdate between'1995-01-01' and'1996-12-31'
	) as shipping
group by
	supp_nation,
	cust_nation,
	l_year
order by
	supp_nation,
	cust_nation,
	l_year;
 

insert into default.perf_results select 8, current_timestamp();

--8
select
	o_year,
	sum(case
		when nation = 'CHINA' then volume
		else 0
	end) / sum(volume) as mkt_share
from
	(
		select
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) as volume,
			n2.n_name as nation
		from
			raw_area.part,
			raw_area.supplier,
			raw_area.lineitem,
			raw_area.orders,
			raw_area.customer,
			raw_area.nation n1,
			raw_area.nation n2,
			raw_area.region
		where
			p_partkey = l_partkey
			and s_suppkey = l_suppkey
			and l_orderkey = o_orderkey
			and o_custkey = c_custkey
			and c_nationkey = n1.n_nationkey
			and n1.n_regionkey = r_regionkey
			and r_name = 'ASIA'
			and s_nationkey = n2.n_nationkey
			and o_orderdate between'1995-01-01' and'1996-12-31'
			and p_type = 'STANDARD BURNISHED BRASS'
	) as all_nations
group by
	o_year
order by
	o_year;
 

insert into default.perf_results select 9, current_timestamp();

--9
select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			n_name as nation,
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
		from
			raw_area.part,
			raw_area.supplier,
			raw_area.lineitem,
			raw_area.partsupp,
			raw_area.orders,
			raw_area.nation
		where
			s_suppkey = l_suppkey
			and ps_suppkey = l_suppkey
			and ps_partkey = l_partkey
			and p_partkey = l_partkey
			and o_orderkey = l_orderkey
			and s_nationkey = n_nationkey
			and p_name like '%thistle%'
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc;
 

insert into default.perf_results select 10, current_timestamp();

--10
select
	c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from
	raw_area.customer,
	raw_area.orders,
	raw_area.lineitem,
	raw_area.nation
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate >='1993-06-01'
	and o_orderdate < add_months('1993-06-01',3)
	and l_returnflag = 'R'
	and c_nationkey = n_nationkey
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
order by
	revenue desc
limit 20;

insert into default.perf_results select 11, current_timestamp();

--11
select
	ps_partkey,
	sum(ps_supplycost * ps_availqty) val
from
	raw_area.partsupp,
	raw_area.supplier,
	raw_area.nation
where
	ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = 'UNITED KINGDOM'
group by
	ps_partkey having
		sum(ps_supplycost * ps_availqty) > (
			select
				sum(ps_supplycost * ps_availqty) * 0.0001000000
			from
				raw_area.partsupp,
				raw_area.supplier,
				raw_area.nation
			where
				ps_suppkey = s_suppkey
				and s_nationkey = n_nationkey
				and n_name = 'UNITED KINGDOM'
		)
order by
	val desc;
 
insert into default.perf_results select 12, current_timestamp();


--12
select
	l_shipmode,
	sum(case
		when o_orderpriority = '1-URGENT'
			or o_orderpriority = '2-HIGH'
			then 1
		else 0
	end) as high_line_count,
	sum(case
		when o_orderpriority <> '1-URGENT'
			and o_orderpriority <> '2-HIGH'
			then 1
		else 0
	end) as low_line_count
from
	raw_area.orders,
	raw_area.lineitem
where
	o_orderkey = l_orderkey
	and l_shipmode in ('RAIL', 'FOB')
	and l_commitdate < l_receiptdate
	and l_shipdate < l_commitdate
	and l_receiptdate >='1995-01-01'
	and l_receiptdate < add_months('1995-01-01',12)
group by
	l_shipmode
order by
	l_shipmode;
 
insert into default.perf_results select 13, current_timestamp();


--13
select
	c_count,
	count(*) as custdist
from
	(
		select
			c_custkey,
			count(o_orderkey) as c_count
		from
			raw_area.customer left outer join raw_area.orders on
				c_custkey = o_custkey
				and o_comment not like '%unusual%accounts%'
		group by
			c_custkey
	) as c_orders
group by
	c_count
order by
	custdist desc,
	c_count desc;
 

insert into default.perf_results select 14, current_timestamp();

--14
select
	100.00 * sum(case
		when p_type like 'PROMO%'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
	raw_area.lineitem,
	raw_area.part
where
	l_partkey = p_partkey
	and l_shipdate >='1994-09-01'
	and l_shipdate < add_months('1994-09-01',1);
 
insert into default.perf_results select 15, current_timestamp();

--15
create view default.revenue0 (supplier_no, total_revenue) as
	select
		l_suppkey as,
		sum(l_extendedprice * (1 - l_discount)) as total_revenue
	from
		raw_area.lineitem
	where
		l_shipdate >='1995-01-01'
		and l_shipdate < add_months('1995-01-01',3)
	group by
		l_suppkey;


select
	s_suppkey,
	s_name,
	s_address,
	s_phone,
	total_revenue
from
	raw_area.supplier,
	default.revenue0
where
	s_suppkey = supplier_no
	and total_revenue = (
		select
			max(total_revenue)
		from
			revenue0
	)
order by
	s_suppkey;

drop view default.revenue0;
 

insert into default.perf_results select 16, current_timestamp();

--16
select
	p_brand,
	p_type,
	p_size,
	count(distinct ps_suppkey) as supplier_cnt
from
	raw_area.partsupp,
	raw_area.part
where
	p_partkey = ps_partkey
	and p_brand <> 'Brand#45'
	and p_type not like 'MEDIUM BRUSHED%'
	and p_size in (21, 47, 45, 26, 38, 41, 43, 20)
	and ps_suppkey not in (
		select
			s_suppkey
		from
			raw_area.supplier
		where
			s_comment like '%Customer%Complaints%'
	)
group by
	p_brand,
	p_type,
	p_size
order by
	supplier_cnt desc,
	p_brand,
	p_type,
	p_size;
 
insert into default.perf_results select 17, current_timestamp();


--17
select
	sum(l_extendedprice) / 7.0 as avg_yearly
from
	raw_area.lineitem,
	raw_area.part
where
	p_partkey = l_partkey
	and p_brand = 'Brand#14'
	and p_container = 'MED CAN'
	and l_quantity < (
		select
			0.2 * avg(l_quantity)
		from
			raw_area.lineitem
		where
			l_partkey = p_partkey
	);
 

insert into default.perf_results select 18, current_timestamp();

--18
select
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice,
	sum(l_quantity)
from
	raw_area.customer,
	raw_area.orders,
	raw_area.lineitem
where
	o_orderkey in (
		select
			l_orderkey
		from
			raw_area.lineitem
		group by
			l_orderkey having
				sum(l_quantity) > 313
	)
	and c_custkey = o_custkey
	and o_orderkey = l_orderkey
group by
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice
order by
	o_totalprice desc,
	o_orderdate
limit 100;

insert into default.perf_results select 19, current_timestamp();

--19
select
	sum(l_extendedprice* (1 - l_discount)) as revenue
from
	raw_area.lineitem,
	raw_area.part
where
	(
		p_partkey = l_partkey
		and p_brand = 'Brand#42'
		and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
		and l_quantity >= 9 and l_quantity <= 9 + 10
		and p_size between 1 and 5
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = 'Brand#21'
		and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
		and l_quantity >= 15 and l_quantity <= 15 + 10
		and p_size between 1 and 10
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = 'Brand#11'
		and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
		and l_quantity >= 30 and l_quantity <= 30 + 10
		and p_size between 1 and 15
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	);
 

insert into default.perf_results select 20, current_timestamp();

--20
select
	s_name,
	s_address
from
	raw_area.supplier,
	raw_area.nation
where
	s_suppkey in (
		select
			ps_suppkey
		from
			raw_area.partsupp
		where
			ps_partkey in (
				select
					p_partkey
				from
					raw_area.part
				where
					p_name like 'ivory%'
			)
			and ps_availqty > (
				select
					0.5 * sum(l_quantity)
				from
					raw_area.lineitem
				where
					l_partkey = ps_partkey
					and l_suppkey = ps_suppkey
					and l_shipdate >='1993-01-01'
					and l_shipdate < add_months('1993-01-01',12)
			)
	)
	and s_nationkey = n_nationkey
	and n_name = 'ALGERIA'
order by
	s_name;
 

insert into default.perf_results select 21, current_timestamp();

--21
select
	s_name,
	count(*) as numwait
from
	raw_area.supplier,
	raw_area.lineitem l1,
	raw_area.orders,
	raw_area.nation
where
	s_suppkey = l1.l_suppkey
	and o_orderkey = l1.l_orderkey
	and o_orderstatus = 'F'
	and l1.l_receiptdate > l1.l_commitdate
	and exists (
		select
			*
		from
			raw_area.lineitem l2
		where
			l2.l_orderkey = l1.l_orderkey
			and l2.l_suppkey <> l1.l_suppkey
	)
	and not exists (
		select
			*
		from
			raw_area.lineitem l3
		where
			l3.l_orderkey = l1.l_orderkey
			and l3.l_suppkey <> l1.l_suppkey
			and l3.l_receiptdate > l3.l_commitdate
	)
	and s_nationkey = n_nationkey
	and n_name = 'KENYA'
group by
	s_name
order by
	numwait desc,
	s_name
limit 100;

insert into default.perf_results select 22, current_timestamp();

--22
select
	cntrycode,
	count(*) as numcust,
	sum(c_acctbal) as totacctbal
from
	(
		select
			substring(c_phone, 1, 2) as cntrycode,
			c_acctbal
		from
			raw_area.customer
		where
			substring(c_phone, 1, 2) in
				('14', '21', '24', '33', '28', '17', '10')
			and c_acctbal > (
				select
					avg(c_acctbal)
				from
					raw_area.customer
				where
					c_acctbal > 0.00
					and substring(c_phone, 1, 2) in
						('14', '21', '24', '33', '28', '17', '10')
			)
			and not exists (
				select
					*
				from
					raw_area.orders
				where
					o_custkey = c_custkey
			)
	) as custsale
group by
	cntrycode
order by
	cntrycode;

insert into default.perf_results select 100, current_timestamp();

select * from default.perf_results order by 2;


-- COMMAND ----------

select * from default.perf_results order by 2;

-- COMMAND ----------


