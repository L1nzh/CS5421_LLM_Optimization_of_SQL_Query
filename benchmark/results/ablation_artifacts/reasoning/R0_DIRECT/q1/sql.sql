WITH tn_stores AS (
    SELECT s_store_sk
    FROM store
    WHERE s_state = 'TN'
),
customer_total_return AS (
    SELECT
        sr_customer_sk AS ctr_customer_sk,
        sr_store_sk AS ctr_store_sk,
        SUM(sr_return_amt) AS ctr_total_return
    FROM store_returns
    INNER JOIN date_dim ON sr_returned_date_sk = d_date_sk
    INNER JOIN tn_stores ON sr_store_sk = s_store_sk
    WHERE d_year = 2000
    GROUP BY sr_customer_sk, sr_store_sk
),
store_return_threshold AS (
    SELECT
        ctr_store_sk,
        AVG(ctr_total_return) * 1.2 AS threshold
    FROM customer_total_return
    GROUP BY ctr_store_sk
)
SELECT c.c_customer_id
FROM customer_total_return ctr1
INNER JOIN store_return_threshold srt
    ON ctr1.ctr_store_sk = srt.ctr_store_sk
    AND ctr1.ctr_total_return > srt.threshold
INNER JOIN customer c ON ctr1.ctr_customer_sk = c.c_customer_sk
ORDER BY c.c_customer_id
LIMIT 100