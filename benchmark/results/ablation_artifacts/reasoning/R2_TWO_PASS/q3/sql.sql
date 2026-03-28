WITH filtered_items AS (
    SELECT i_item_sk, i_brand, i_brand_id FROM item WHERE i_manufact_id = 128
),
filtered_dates AS (
    SELECT d_date_sk, d_year FROM date_dim WHERE d_moy = 11
),
preagg_store_sales AS (
    SELECT ss_item_sk, ss_sold_date_sk, SUM(ss_ext_sales_price) AS partial_sum
    FROM store_sales
    GROUP BY ss_item_sk, ss_sold_date_sk
)
SELECT
    fd.d_year,
    fi.i_brand_id AS brand_id,
    fi.i_brand AS brand,
    SUM(ps.partial_sum) AS sum_agg
FROM filtered_items fi
INNER JOIN preagg_store_sales ps ON fi.i_item_sk = ps.ss_item_sk
INNER JOIN filtered_dates fd ON ps.ss_sold_date_sk = fd.d_date_sk
GROUP BY fd.d_year, fi.i_brand, fi.i_brand_id
ORDER BY fd.d_year, sum_agg DESC, fi.i_brand_id
LIMIT 100