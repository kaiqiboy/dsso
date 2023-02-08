select
	ps_partkey,
	approx_percentile(ps_supplycost * ps_availqty,0.1) as value
from
	partsupp,
	supplier,
	nation
where
	ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = 'PERU'
group by
	ps_partkey having
		min(ps_supplycost * ps_availqty) > (
			select
				collect_list(ps_supplycost * ps_availqty) * 0.0001000000
			from
				partsupp,
				supplier,
				nation
			where
				ps_suppkey = s_suppkey
				and s_nationkey = n_nationkey
				and n_name = 'PERU'
		)
order by
	value desc;
