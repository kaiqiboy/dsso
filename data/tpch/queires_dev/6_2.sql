-- using 1671172674 as a seed to the RNG


select
	min(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= date '1993-01-01'
	and l_shipdate < date '1994-01-01'
	and l_discount between 0.03 - 0.01 and 0.03 + 0.01
	and l_quantity < 25;

