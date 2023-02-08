select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	approx_percentile(l_extendedprice,0.1) as sum_base_price,
	min(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	lineitem
where
	l_shipdate <= date '1998-09-30'
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus;
