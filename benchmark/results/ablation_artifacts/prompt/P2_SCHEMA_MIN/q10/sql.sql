WITH eligible_customers AS (
    SELECT ss_customer_sk AS c_customer_sk
    FROM store_sales
    INNER JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
    WHERE date_dim.d_year = 2002 AND date_dim.d_moy BETWEEN 1 AND 4
    GROUP BY ss_customer_sk
    INTERSECT
    SELECT customer_sk
    FROM (
        SELECT ws_bill_customer_sk AS customer_sk
        FROM web_sales
        INNER JOIN date_dim ON web_sales.ws_sold_date_sk = date_dim.d_date_sk
        WHERE date_dim.d_year = 2002 AND date_dim.d_moy BETWEEN 1 AND 4
        UNION ALL
        SELECT cs_ship_customer_sk AS customer_sk
        FROM catalog_sales
        INNER JOIN date_dim ON catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
        WHERE date_dim.d_year = 2002 AND date_dim.d_moy BETWEEN 1 AND 4
    ) combined_sales
    GROUP BY customer_sk
)
SELECT
    cd_gender,
    cd_marital_status,
    cd_education_status,
    COUNT(*) cnt1,
    cd_purchase_estimate,
    COUNT(*) cnt2,
    cd_credit_rating,
    COUNT(*) cnt3,
    cd_dep_count,
    COUNT(*) cnt4,
    cd_dep_employed_count,
    COUNT(*) cnt5,
    cd_dep_college_count,
    COUNT(*) cnt6
FROM customer c
INNER JOIN eligible_customers ec ON c.c_customer_sk = ec.c_customer_sk
INNER JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
INNER JOIN customer_demographics ON c.c_current_cdemo_sk = customer_demographics.cd_demo_sk
WHERE ca.ca_county IN ('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County', 'La Porte County')
GROUP BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count
ORDER BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count
LIMIT 100