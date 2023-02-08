-- using 1671172687 as a seed to the RNG


select
	100.00 * avg(case
		when p_type like 'PROMO%'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) / count(l_extendedprice * (1 - l_discount)) as promo_revenue
from
	lineitem,
	part
where
	l_partkey = p_partkey
	and l_shipdate >= date '1994-06-01'
	and l_shipdate < date '1994-07-01'

