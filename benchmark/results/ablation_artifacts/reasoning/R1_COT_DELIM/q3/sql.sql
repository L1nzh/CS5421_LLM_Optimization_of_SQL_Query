SELECT
  dt.d_year,
  item.i_brand_id AS brand_id,
  item.i_brand AS brand,
  SUM(ss_ext_sales_price) AS sum_agg
FROM store_sales
INNER JOIN (
    SELECT d_date_sk, d_year
    FROM date_dim
    WHERE d_moy = 11
) dt ON dt.d_date_sk = store_sales.ss_sold_date_sk
INNER JOIN (
    SELECT i_item_sk, i_brand_id, i_brand
    FROM item
    WHERE i_manufact_id = 128
) item ON store_sales.ss_item_sk = item.i_item_sk
GROUP BY dt.d_year, item.i_brand, item.i_brand_id
ORDER BY dt.d_year, sum_agg DESC, brand_id
LIMIT 100