SELECT
  a.ca_state state,
  count(*) cnt
FROM customer_address a
INNER JOIN customer c ON a.ca_address_sk = c.c_current_addr_sk
INNER JOIN store_sales s ON c.c_customer_sk = s.ss_customer_sk
INNER JOIN date_dim d ON s.ss_sold_date_sk = d.d_date_sk
INNER JOIN item i ON s.ss_item_sk = i.i_item_sk
WHERE d.d_month_seq = (
  SELECT DISTINCT d_month_seq
  FROM date_dim
  WHERE d_year = 2000 AND d_moy = 1
)
AND i.i_current_price > 1.2 * (
  SELECT avg(j.i_current_price)
  FROM item j
  WHERE j.i_category = i.i_category
)
GROUP BY a.ca_state
HAVING count(*) >= 10
ORDER BY cnt
LIMIT 100