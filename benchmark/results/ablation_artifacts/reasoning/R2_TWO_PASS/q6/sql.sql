WITH target_month AS (
    SELECT d_month_seq
    FROM date_dim
    WHERE d_year = 2000 AND d_moy = 1
    LIMIT 1
),
category_avg_price AS (
    SELECT i_category, AVG(i_current_price) AS avg_price
    FROM item
    GROUP BY i_category
),
valid_customer_sales AS (
    SELECT ss_customer_sk, COUNT(*) AS sales_count
    FROM store_sales s
    JOIN date_dim d ON s.ss_sold_date_sk = d.d_date_sk
    JOIN target_month tm ON d.d_month_seq = tm.d_month_seq
    JOIN item i ON s.ss_item_sk = i.i_item_sk
    JOIN category_avg_price cap ON i.i_category = cap.i_category
    WHERE i.i_current_price > 1.2 * cap.avg_price
    GROUP BY ss_customer_sk
)
SELECT a.ca_state AS state, SUM(vcs.sales_count) AS cnt
FROM valid_customer_sales vcs
JOIN customer c ON vcs.ss_customer_sk = c.c_customer_sk
JOIN customer_address a ON c.c_current_addr_sk = a.ca_address_sk
GROUP BY a.ca_state
HAVING SUM(vcs.sales_count) >= 10
ORDER BY cnt
LIMIT 100