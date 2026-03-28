WITH sales_bucket_aggs AS (
    SELECT
        CASE
            WHEN ss_quantity BETWEEN 1 AND 20 THEN 1
            WHEN ss_quantity BETWEEN 21 AND 40 THEN 2
            WHEN ss_quantity BETWEEN 41 AND 60 THEN 3
            WHEN ss_quantity BETWEEN 61 AND 80 THEN 4
            WHEN ss_quantity BETWEEN 81 AND 100 THEN 5
        END AS bucket,
        COUNT(*) AS row_count,
        AVG(ss_ext_discount_amt) AS avg_ext_discount_amt,
        AVG(ss_net_paid) AS avg_net_paid
    FROM store_sales
    WHERE ss_quantity BETWEEN 1 AND 100
    GROUP BY 1
)
SELECT
    MAX(CASE WHEN bucket = 1 THEN CASE WHEN row_count > 62316685 THEN avg_ext_discount_amt ELSE avg_net_paid END END) AS bucket1,
    MAX(CASE WHEN bucket = 2 THEN CASE WHEN row_count > 19045798 THEN avg_ext_discount_amt ELSE avg_net_paid END END) AS bucket2,
    MAX(CASE WHEN bucket = 3 THEN CASE WHEN row_count > 365541424 THEN avg_ext_discount_amt ELSE avg_net_paid END END) AS bucket3,
    MAX(CASE WHEN bucket = 4 THEN CASE WHEN row_count > 216357808 THEN avg_ext_discount_amt ELSE avg_net_paid END END) AS bucket4,
    MAX(CASE WHEN bucket = 5 THEN CASE WHEN row_count > 184483884 THEN avg_ext_discount_amt ELSE avg_net_paid END END) AS bucket5
FROM reason
CROSS JOIN sales_bucket_aggs
WHERE r_reason_sk = 1