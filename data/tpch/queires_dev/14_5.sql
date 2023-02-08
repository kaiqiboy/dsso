-- using 1671172682 as a seed to the RNG


select
	100.00 * count(case
		when p_type like 'PROMO%'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) / min(l_extendedprice * (1 - l_discount)) as promo_revenue
from
	lineitem,
	part
where
	l_partkey = p_partkey
	and l_shipdate >= date '1993-02-01'
	and l_shipdate < date '1993-03-03'

