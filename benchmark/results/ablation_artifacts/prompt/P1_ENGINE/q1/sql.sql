WITH customer_total_return AS
( SELECT
    sr_customer_sk AS ctr_customer_sk,
    sr_store_sk AS ctr_store_sk,
    sum(sr_return_amt) AS ctr_total_return,
    avg(sum(sr_return_amt)) OVER (PARTITION BY sr_store_sk) * 1.2 AS ctr_store_threshold
  FROM store_returns
  INNER JOIN date_dim ON sr_returned_date_sk = d_date_sk
  WHERE d_year = 2000
  GROUP BY sr_customer_sk, sr_store_sk)
SELECT c_customer_id
FROM customer_total_return ctr1
INNER JOIN store ON s_store_sk = ctr1.ctr_store_sk
INNER JOIN customer ON ctr1.ctr_customer_sk = c_customer_sk
WHERE ctr1.ctr_total_return > ctr1.ctr_store_threshold
  AND s_state = 'TN'
ORDER BY c_customer_id
LIMIT 100