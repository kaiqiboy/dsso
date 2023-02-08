-- using 1671172685 as a seed to the RNG


select
	100.00 * avg(case
		when p_type like 'PROMO%'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) / min(l_extendedprice * (1 - l_discount)) as promo_revenue
from
	lineitem,
	part
where
	l_partkey = p_partkey
	and l_shipdate >= date '1993-11-01'
	and l_shipdate < date '1993-12-01'

