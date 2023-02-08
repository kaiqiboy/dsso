select
	approx_percentile(l_extendedprice * l_discount,0.1) as revenue
from
	lineitem
where
	l_shipdate >= date '1996-01-01'
	and l_shipdate < date '1996-12-31'
	and l_discount between 0.02 - 0.01 and 0.02 + 0.01
	and l_quantity < 25;
