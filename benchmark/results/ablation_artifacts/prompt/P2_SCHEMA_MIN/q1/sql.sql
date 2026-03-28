WITH cust_store_return AS (
    SELECT
        sr_customer_sk,
        sr_store_sk,
        SUM(sr_return_amt) AS ctr_total_return,
        AVG(SUM(sr_return_amt)) OVER (PARTITION BY sr_store_sk) * 1.2 AS store_threshold
    FROM store_returns
    JOIN date_dim ON sr_returned_date_sk = d_date_sk
    JOIN store ON sr_store_sk = s_store_sk
    WHERE d_year = 2000 AND s_state = 'TN'
    GROUP BY sr_customer_sk, sr_store_sk
)
SELECT c.c_customer_id
FROM cust_store_return csr
JOIN customer c ON csr.sr_customer_sk = c.c_customer_sk
WHERE csr.ctr_total_return > csr.store_threshold
ORDER BY c.c_customer_id
LIMIT 100