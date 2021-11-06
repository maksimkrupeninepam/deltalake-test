-- Databricks notebook source
truncate table data_mart.report_tpch_16

-- COMMAND ----------

insert into data_mart.report_tpch_16
select
	p_brand,
	p_type,
	p_size,
	count(distinct ps_suppkey) as supplier_cnt
from
	business_layer.partsupp,
	data_mart.dim_part
where
	p_partkey = ps_partkey
	and p_brand <> 'Brand#45'
	and p_type not like 'MEDIUM BRUSHED%'
	and p_size in (21, 47, 45, 26, 38, 41, 43, 20)
	and ps_suppkey not in (
		select
			s_suppkey
		from
			data_mart.dim_supplier
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
	p_size

-- COMMAND ----------


