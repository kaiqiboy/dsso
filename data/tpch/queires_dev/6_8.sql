-- using 1671172688 as a seed to the RNG


select
	count(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= date '1996-01-01'
	and l_shipdate < date '1996-12-31'
	and l_discount between 0.07 - 0.01 and 0.07 + 0.01
	and l_quantity < 24;

