-- using 1671172673 as a seed to the RNG
      
  

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
   avg(l_extendedprice * (1 - l_discount)) total_revenue
   from
   lineitem
   where
   l_shipdate >= date '1997-10-01'
   and l_shipdate < date '1997-12-30'

   group by
   l_suppkey
  ) revenue0,
  (
   select
   avg(total_revenue) m_max
   from
   (
    select
    l_suppkey supplier_no,
    count(l_extendedprice * (1 - l_discount)) total_revenue
    from
    lineitem
    where
    l_shipdate >= date '1997-10-01'
    and l_shipdate < date '1997-12-30'

    group by
    l_suppkey
   ) v1
  ) v
  where
  s_suppkey = supplier_no
  and total_revenue = v.m_max
  order by
  s_suppkey;

