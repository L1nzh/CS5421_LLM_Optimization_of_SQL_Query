WITH bucket_stats AS (
SELECT
  COUNT(CASE WHEN ss_quantity BETWEEN 1 AND 20 THEN 1 END) AS cnt1,
  AVG(CASE WHEN ss_quantity BETWEEN 1 AND 20 THEN ss_ext_discount_amt END) AS avg_d1,
  AVG(CASE WHEN ss_quantity BETWEEN 1 AND 20 THEN ss_net_paid END) AS avg_p1,
  COUNT(CASE WHEN ss_quantity BETWEEN 21 AND 40 THEN 1 END) AS cnt2,
  AVG(CASE WHEN ss_quantity BETWEEN 21 AND 40 THEN ss_ext_discount_amt END) AS avg_d2,
  AVG(CASE WHEN ss_quantity BETWEEN 21 AND 40 THEN ss_net_paid END) AS avg_p2,
  COUNT(CASE WHEN ss_quantity BETWEEN 41 AND 60 THEN 1 END) AS cnt3,
  AVG(CASE WHEN ss_quantity BETWEEN 41 AND 60 THEN ss_ext_discount_amt END) AS avg_d3,
  AVG(CASE WHEN ss_quantity BETWEEN 41 AND 60 THEN ss_net_paid END) AS avg_p3,
  COUNT(CASE WHEN ss_quantity BETWEEN 61 AND 80 THEN 1 END) AS cnt4,
  AVG(CASE WHEN ss_quantity BETWEEN 61 AND 80 THEN ss_ext_discount_amt END) AS avg_d4,
  AVG(CASE WHEN ss_quantity BETWEEN 61 AND 80 THEN ss_net_paid END) AS avg_p4,
  COUNT(CASE WHEN ss_quantity BETWEEN 81 AND 100 THEN 1 END) AS cnt5,
  AVG(CASE WHEN ss_quantity BETWEEN 81 AND 100 THEN ss_ext_discount_amt END) AS avg_d5,
  AVG(CASE WHEN ss_quantity BETWEEN 81 AND 100 THEN ss_net_paid END) AS avg_p5
FROM store_sales
)
SELECT
  CASE WHEN cnt1 > 62316685 THEN avg_d1 ELSE avg_p1 END bucket1,
  CASE WHEN cnt2 > 19045798 THEN avg_d2 ELSE avg_p2 END bucket2,
  CASE WHEN cnt3 > 365541424 THEN avg_d3 ELSE avg_p3 END bucket3,
  CASE WHEN cnt4 > 216357808 THEN avg_d4 ELSE avg_p4 END bucket4,
  CASE WHEN cnt5 > 184483884 THEN avg_d5 ELSE avg_p5 END bucket5
FROM reason
CROSS JOIN bucket_stats
WHERE r_reason_sk = 1
