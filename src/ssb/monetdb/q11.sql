SELECT SUM(lo_revenue) AS revenue
FROM lineorder, ddate
WHERE lo_orderdate = d_datekey
  AND d_year = 1993
  AND lo_discount BETWEEN 1 AND 3
  AND lo_quantity < 25;

