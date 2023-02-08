select
	approx_percentile(l_extendedprice * l_discount,0.1) as revenue
from
	lineitem
where
	l_shipdate >= date '1997-01-01'
	and l_shipdate < date '1998-01-01'
	and l_discount between 0.08 - 0.01 and 0.08 + 0.01
	and l_quantity < 25;
