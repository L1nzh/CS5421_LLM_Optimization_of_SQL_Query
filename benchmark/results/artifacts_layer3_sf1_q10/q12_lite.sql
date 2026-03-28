SELECT i_item_id, i_item_desc, i_category, i_class, i_current_price, SUM(ws_ext_sales_price) AS itemrevenue, SUM(ws_ext_sales_price)*100/SUM(SUM(ws_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
FROM web_sales
INNER JOIN item ON ws_item_sk = i_item_sk AND i_category IN ('Jewelry', 'Sports', 'Books')
INNER JOIN date_dim ON ws_sold_date_sk = d_date_sk AND d_date BETWEEN CAST('2001-01-12' AS DATE) AND (CAST('2001-01-12' AS DATE) + INTERVAL '30 days')
GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio
LIMIT 100
