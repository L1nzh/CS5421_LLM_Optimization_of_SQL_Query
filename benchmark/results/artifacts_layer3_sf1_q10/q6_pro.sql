WITH cat_avg AS (
    SELECT i_category, AVG(i_current_price) AS avg_cat_price
    FROM item
    GROUP BY i_category
),
target_month AS (
    SELECT DISTINCT d_month_seq
    FROM date_dim
    WHERE d_year = 2000 AND d_moy = 1
)
SELECT
    a.ca_state state,
    count(*) cnt
FROM customer_address a
INNER JOIN customer c ON a.ca_address_sk = c.c_current_addr_sk
INNER JOIN store_sales s ON c.c_customer_sk = s.ss_customer_sk
INNER JOIN date_dim d ON s.ss_sold_date_sk = d.d_date_sk
INNER JOIN item i ON s.ss_item_sk = i.i_item_sk
INNER JOIN cat_avg ON i.i_category = cat_avg.i_category
INNER JOIN target_month ON d.d_month_seq = target_month.d_month_seq
WHERE i.i_current_price > 1.2 * cat_avg.avg_cat_price
GROUP BY a.ca_state
HAVING count(*) >= 10
ORDER BY cnt
LIMIT 100
