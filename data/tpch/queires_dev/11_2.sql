-- using 1671172675 as a seed to the RNG


select
	ps_partkey,
	max(ps_supplycost * ps_availqty) as value
from
	partsupp,
	supplier,
	nation
where
	ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = 'UNITED KINGDOM'
group by
	ps_partkey having
		sum(ps_supplycost * ps_availqty) > (
			select
				min(ps_supplycost * ps_availqty) * 0.0001000000
			from
				partsupp,
				supplier,
				nation
			where
				ps_suppkey = s_suppkey
				and s_nationkey = n_nationkey
				and n_name = 'UNITED KINGDOM'
		)
order by
	value desc;

