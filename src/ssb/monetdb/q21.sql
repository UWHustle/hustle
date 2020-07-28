SELECT SUM(lo_revenue), p_brand1
-- SELECT lo_orderkey
FROM  lineorder, ddate, part, supplier
WHERE lo_orderdate = d_datekey
  AND lo_partkey = p_partkey
  AND lo_suppkey = s_suppkey
  AND p_category = 'MFGR#12'
  AND s_region = 'AMERICA'
GROUP BY p_brand1
ORDER BY p_brand1;