WITH tn_stores AS (
    SELECT s_store_sk FROM store WHERE s_state = 'TN'
),
customer_total_return AS (
    SELECT
        sr_customer_sk AS ctr_customer_sk,
        sr_store_sk AS ctr_store_sk,
        SUM(sr_return_amt) AS ctr_total_return
    FROM store_returns
    INNER JOIN date_dim ON sr_returned_date_sk = d_date_sk
    WHERE d_year = 2000
      AND sr_store_sk IN (SELECT s_store_sk FROM tn_stores)
    GROUP BY sr_customer_sk, sr_store_sk
),
ctr_with_store_avg AS (
    SELECT
        ctr_customer_sk,
        ctr_total_return,
        AVG(ctr_total_return) OVER (PARTITION BY ctr_store_sk) AS store_avg_return
    FROM customer_total_return
)
SELECT c.c_customer_id
FROM ctr_with_store_avg ctr
INNER JOIN customer c ON ctr.ctr_customer_sk = c.c_customer_sk
WHERE ctr.ctr_total_return > ctr.store_avg_return * 1.2
ORDER BY c.c_customer_id
LIMIT 100