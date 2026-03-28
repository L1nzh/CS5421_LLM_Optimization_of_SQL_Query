WITH bucket_aggs AS (
    SELECT
        CASE
            WHEN ss_quantity BETWEEN 1 AND 20 THEN 1
            WHEN ss_quantity BETWEEN 21 AND 40 THEN 2
            WHEN ss_quantity BETWEEN 41 AND 60 THEN 3
            WHEN ss_quantity BETWEEN 61 AND 80 THEN 4
            WHEN ss_quantity BETWEEN 81 AND 100 THEN 5
        END AS bucket_num,
        COUNT(*) AS row_cnt,
        AVG(ss_ext_discount_amt) AS avg_discount,
        AVG(ss_net_paid) AS avg_net_paid
    FROM store_sales
    WHERE ss_quantity BETWEEN 1 AND 100
    GROUP BY 1
)
SELECT
    (SELECT CASE WHEN row_cnt > 62316685 THEN avg_discount ELSE avg_net_paid END FROM bucket_aggs WHERE bucket_num = 1) AS bucket1,
    (SELECT CASE WHEN row_cnt > 19045798 THEN avg_discount ELSE avg_net_paid END FROM bucket_aggs WHERE bucket_num = 2) AS bucket2,
    (SELECT CASE WHEN row_cnt > 365541424 THEN avg_discount ELSE avg_net_paid END FROM bucket_aggs WHERE bucket_num = 3) AS bucket3,
    (SELECT CASE WHEN row_cnt > 216357808 THEN avg_discount ELSE avg_net_paid END FROM bucket_aggs WHERE bucket_num = 4) AS bucket4,
    (SELECT CASE WHEN row_cnt > 184483884 THEN avg_discount ELSE avg_net_paid END FROM bucket_aggs WHERE bucket_num = 5) AS bucket5
FROM reason
WHERE r_reason_sk = 1