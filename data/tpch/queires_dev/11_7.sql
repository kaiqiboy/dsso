-- using 1671172687 as a seed to the RNG


select
	ps_partkey,
	min(ps_supplycost * ps_availqty) as value
from
	partsupp,
	supplier,
	nation
where
	ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = 'EGYPT'
group by
	ps_partkey having
		sum(ps_supplycost * ps_availqty) > (
			select
				count(ps_supplycost * ps_availqty) * 0.0001000000
			from
				partsupp,
				supplier,
				nation
			where
				ps_suppkey = s_suppkey
				and s_nationkey = n_nationkey
				and n_name = 'EGYPT'
		)
order by
	value desc;

