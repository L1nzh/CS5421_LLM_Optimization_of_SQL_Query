SELECT
  dt.d_year,
  i.i_brand_id brand_id,
  i.i_brand brand,
  SUM(ss.ss_ext_sales_price) sum_agg
FROM (
  SELECT d_date_sk, d_year
  FROM date_dim
  WHERE d_moy = 11
) dt
INNER JOIN store_sales ss
  ON dt.d_date_sk = ss.ss_sold_date_sk
INNER JOIN (
  SELECT i_item_sk, i_brand_id, i_brand
  FROM item
  WHERE i_manufact_id = 128
) i
  ON ss.ss_item_sk = i.i_item_sk
GROUP BY dt.d_year, i.i_brand, i.i_brand_id
ORDER BY dt.d_year, sum_agg DESC, brand_id
LIMIT 100
