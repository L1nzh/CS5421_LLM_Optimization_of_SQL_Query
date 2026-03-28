WITH target_month AS (
    SELECT d_month_seq FROM date_dim WHERE d_year = 2000 AND d_moy = 1 LIMIT 1
),
category_avg_prices AS (
    SELECT i_category, AVG(i_current_price) AS avg_cat_price
    FROM item
    GROUP BY i_category
)
SELECT
    a.ca_state state,
    COUNT(*) cnt
FROM customer_address a
INNER JOIN customer c ON a.ca_address_sk = c.c_current_addr_sk
INNER JOIN store_sales s ON c.c_customer_sk = s.ss_customer_sk
INNER JOIN date_dim d ON s.ss_sold_date_sk = d.d_date_sk
INNER JOIN target_month tm ON d.d_month_seq = tm.d_month_seq
INNER JOIN item i ON s.ss_item_sk = i.i_item_sk
INNER JOIN category_avg_prices cap ON i.i_category = cap.i_category
WHERE i.i_current_price > 1.2 * cap.avg_cat_price
GROUP BY a.ca_state
HAVING COUNT(*) >= 10
ORDER BY cnt
LIMIT 100