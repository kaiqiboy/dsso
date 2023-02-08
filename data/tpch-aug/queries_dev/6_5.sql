select
	approx_percentile(l_extendedprice * l_discount,0.1) as revenue
from
	lineitem
where
	l_shipdate >= date '1994-01-01'
	and l_shipdate < date '1995-01-01'
	and l_discount between 0.06 - 0.01 and 0.06 + 0.01
	and l_quantity < 24;
