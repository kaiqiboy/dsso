select
	max(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= date '1994-01-01'
	and l_shipdate < date '1995-01-01'
	and l_discount between 0.09 - 0.01 and 0.09 + 0.01
	and l_quantity < 25;