select
	l_returnflag,
	l_linestatus,
	collect_list(l_quantity) as sum_qty,
	min(l_extendedprice) as sum_base_price,
	max(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	max(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	lineitem
where
	l_shipdate <= date '1998-08-05'
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus;
