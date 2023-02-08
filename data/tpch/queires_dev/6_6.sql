-- using 1671172684 as a seed to the RNG


select
	max(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= date '1995-01-01'
	and l_shipdate < date '1996-01-01'
	and l_discount between 0.09 - 0.01 and 0.09 + 0.01
	and l_quantity < 25;

