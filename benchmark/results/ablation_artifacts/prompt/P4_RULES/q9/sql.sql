WITH bucket_aggs AS (
    SELECT
        CASE
            WHEN ss_quantity BETWEEN 1 AND 20 THEN 1
            WHEN ss_quantity BETWEEN 21 AND 40 THEN 2
            WHEN ss_quantity BETWEEN 41 AND 60 THEN 3
            WHEN ss_quantity BETWEEN 61 AND 80 THEN 4
            WHEN ss_quantity BETWEEN 81 AND 100 THEN 5
        END AS bucket_num,
        COUNT(*) AS cnt,
        AVG(ss_ext_discount_amt) AS avg_discount,
        AVG(ss_net_paid) AS avg_net_paid
    FROM store_sales
    WHERE ss_quantity BETWEEN 1 AND 100
    GROUP BY bucket_num
)
SELECT
    MAX(CASE WHEN bucket_num = 1 THEN CASE WHEN cnt > 62316685 THEN avg_discount ELSE avg_net_paid END END) AS bucket1,
    MAX(CASE WHEN bucket_num = 2 THEN CASE WHEN cnt > 19045798 THEN avg_discount ELSE avg_net_paid END END) AS bucket2,
    MAX(CASE WHEN bucket_num = 3 THEN CASE WHEN cnt > 365541424 THEN avg_discount ELSE avg_net_paid END END) AS bucket3,
    MAX(CASE WHEN bucket_num = 4 THEN CASE WHEN cnt > 216357808 THEN avg_discount ELSE avg_net_paid END END) AS bucket4,
    MAX(CASE WHEN bucket_num = 5 THEN CASE WHEN cnt > 184483884 THEN avg_discount ELSE avg_net_paid END END) AS bucket5
FROM bucket_aggs
CROSS JOIN reason
WHERE r_reason_sk = 1