SELECT
    CASE WHEN cnt1 > 62316685 THEN avg_d1 ELSE avg_n1 END AS bucket1,
    CASE WHEN cnt2 > 19045798 THEN avg_d2 ELSE avg_n2 END AS bucket2,
    CASE WHEN cnt3 > 365541424 THEN avg_d3 ELSE avg_n3 END AS bucket3,
    CASE WHEN cnt4 > 216357808 THEN avg_d4 ELSE avg_n4 END AS bucket4,
    CASE WHEN cnt5 > 184483884 THEN avg_d5 ELSE avg_n5 END AS bucket5
FROM reason
CROSS JOIN (
    SELECT
        COUNT(*) FILTER (WHERE ss_quantity BETWEEN 1 AND 20) AS cnt1,
        AVG(ss_ext_discount_amt) FILTER (WHERE ss_quantity BETWEEN 1 AND 20) AS avg_d1,
        AVG(ss_net_paid) FILTER (WHERE ss_quantity BETWEEN 1 AND 20) AS avg_n1,
        COUNT(*) FILTER (WHERE ss_quantity BETWEEN 21 AND 40) AS cnt2,
        AVG(ss_ext_discount_amt) FILTER (WHERE ss_quantity BETWEEN 21 AND 40) AS avg_d2,
        AVG(ss_net_paid) FILTER (WHERE ss_quantity BETWEEN 21 AND 40) AS avg_n2,
        COUNT(*) FILTER (WHERE ss_quantity BETWEEN 41 AND 60) AS cnt3,
        AVG(ss_ext_discount_amt) FILTER (WHERE ss_quantity BETWEEN 41 AND 60) AS avg_d3,
        AVG(ss_net_paid) FILTER (WHERE ss_quantity BETWEEN 41 AND 60) AS avg_n3,
        COUNT(*) FILTER (WHERE ss_quantity BETWEEN 61 AND 80) AS cnt4,
        AVG(ss_ext_discount_amt) FILTER (WHERE ss_quantity BETWEEN 61 AND 80) AS avg_d4,
        AVG(ss_net_paid) FILTER (WHERE ss_quantity BETWEEN 61 AND 80) AS avg_n4,
        COUNT(*) FILTER (WHERE ss_quantity BETWEEN 81 AND 100) AS cnt5,
        AVG(ss_ext_discount_amt) FILTER (WHERE ss_quantity BETWEEN 81 AND 100) AS avg_d5,
        AVG(ss_net_paid) FILTER (WHERE ss_quantity BETWEEN 81 AND 100) AS avg_n5
    FROM store_sales
    WHERE ss_quantity BETWEEN 1 AND 100
) AS aggs
WHERE r_reason_sk = 1