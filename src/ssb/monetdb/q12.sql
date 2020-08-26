SELECT SUM(lo_revenue) AS revenue
FROM lineorder, ddate
WHERE lo_orderdate = d_datekey
  AND d_yearmonthnum = 199401
  AND lo_discount BETWEEN 4 AND 6
  AND lo_quantity BETWEEN 26 AND 35;