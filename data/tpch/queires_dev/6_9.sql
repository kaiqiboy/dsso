-- using 1671172691 as a seed to the RNG


select
	min(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= date '1997-01-01'
	and l_shipdate < date '1998-01-01'
	and l_discount between 0.07 - 0.01 and 0.07 + 0.01
	and l_quantity < 25;

