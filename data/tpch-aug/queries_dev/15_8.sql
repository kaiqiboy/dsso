      
  
  select
  s_suppkey,
  s_name,
  s_address,
  s_phone,
  total_revenue
  from
  supplier,
  (
   select
   l_suppkey supplier_no,
   max(l_extendedprice * (1 - l_discount)) total_revenue
   from
   lineitem
   where
   l_shipdate >= date '1994-10-01'
   and l_shipdate < date '1994-12-30'
   group by
   l_suppkey
  ) revenue0,
  (
   select
   min(total_revenue) m_max
   from
   (
    select
    l_suppkey supplier_no,
    min(l_extendedprice * (1 - l_discount)) total_revenue
    from
    lineitem
    where
    l_shipdate >= date '1994-10-01'
    and l_shipdate < date '1994-12-30'
    group by
    l_suppkey
   ) v1
  ) v
  where
  s_suppkey = supplier_no
  and total_revenue = v.m_max
  order by
  s_suppkey;
