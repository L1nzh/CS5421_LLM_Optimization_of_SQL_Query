WITH customer_total_return AS (
    SELECT
        sr_customer_sk AS ctr_customer_sk,
        sr_store_sk AS ctr_store_sk,
        SUM(sr_return_amt) AS ctr_total_return
    FROM store_returns
    INNER JOIN date_dim ON store_returns.sr_returned_date_sk = date_dim.d_date_sk
    WHERE date_dim.d_year = 2000
    GROUP BY sr_customer_sk, sr_store_sk
)
SELECT c_customer_id
FROM customer_total_return ctr1
INNER JOIN store ON ctr1.ctr_store_sk = store.s_store_sk
INNER JOIN customer ON ctr1.ctr_customer_sk = customer.c_customer_sk
WHERE store.s_state = 'TN'
AND ctr1.ctr_total_return > (
    SELECT AVG(ctr_total_return) * 1.2
    FROM customer_total_return ctr2
    WHERE ctr2.ctr_store_sk = ctr1.ctr_store_sk
)
ORDER BY c_customer_id
LIMIT 100