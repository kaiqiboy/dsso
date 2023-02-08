-- using 1671172672 as a seed to the RNG


select
	max(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= date '1997-01-01'
	and l_shipdate < date '1998-01-01'
	and l_discount between 0.08 - 0.01 and 0.08 + 0.01
	and l_quantity < 25;

