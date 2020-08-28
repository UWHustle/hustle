SELECT SUM(lo_revenue), d_year, p_brand1
-- SELECT lo_orderdate, lo_partkey, lo_suppkey
FROM  lineorder, ddate, part, supplier
WHERE lo_orderdate = d_datekey
  AND lo_partkey = p_partkey
  AND lo_suppkey = s_suppkey
  AND p_category = 'MFGR#12'
  AND s_region = 'AMERICA'
GROUP BY d_year, p_brand1
ORDER BY d_year, p_brand1;