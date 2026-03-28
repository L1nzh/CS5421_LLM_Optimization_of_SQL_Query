WITH range_aggs AS (
    SELECT
        COUNT(*) FILTER (WHERE ss_quantity BETWEEN 1 AND 20) AS cnt1,
        AVG(ss_ext_discount_amt) FILTER (WHERE ss_quantity BETWEEN 1 AND 20) AS avg_ext1,
        AVG(ss_net_paid) FILTER (WHERE ss_quantity BETWEEN 1 AND 20) AS avg_net1,
        COUNT(*) FILTER (WHERE ss_quantity BETWEEN 21 AND 40) AS cnt2,
        AVG(ss_ext_discount_amt) FILTER (WHERE ss_quantity BETWEEN 21 AND 40) AS avg_ext2,
        AVG(ss_net_paid) FILTER (WHERE ss_quantity BETWEEN 21 AND 40) AS avg_net2,
        COUNT(*) FILTER (WHERE ss_quantity BETWEEN 41 AND 60) AS cnt3,
        AVG(ss_ext_discount_amt) FILTER (WHERE ss_quantity BETWEEN 41 AND 60) AS avg_ext3,
        AVG(ss_net_paid) FILTER (WHERE ss_quantity BETWEEN 41 AND 60) AS avg_net3,
        COUNT(*) FILTER (WHERE ss_quantity BETWEEN 61 AND 80) AS cnt4,
        AVG(ss_ext_discount_amt) FILTER (WHERE ss_quantity BETWEEN 61 AND 80) AS avg_ext4,
        AVG(ss_net_paid) FILTER (WHERE ss_quantity BETWEEN 61 AND 80) AS avg_net4,
        COUNT(*) FILTER (WHERE ss_quantity BETWEEN 81 AND 100) AS cnt5,
        AVG(ss_ext_discount_amt) FILTER (WHERE ss_quantity BETWEEN 81 AND 100) AS avg_ext5,
        AVG(ss_net_paid) FILTER (WHERE ss_quantity BETWEEN 81 AND 100) AS avg_net5
    FROM store_sales
)
SELECT
    CASE WHEN cnt1 > 62316685 THEN avg_ext1 ELSE avg_net1 END AS bucket1,
    CASE WHEN cnt2 > 19045798 THEN avg_ext2 ELSE avg_net2 END AS bucket2,
    CASE WHEN cnt3 > 365541424 THEN avg_ext3 ELSE avg_net3 END AS bucket3,
    CASE WHEN cnt4 > 216357808 THEN avg_ext4 ELSE avg_net4 END AS bucket4,
    CASE WHEN cnt5 > 184483884 THEN avg_ext5 ELSE avg_net5 END AS bucket5
FROM reason CROSS JOIN range_aggs
WHERE r_reason_sk = 1
