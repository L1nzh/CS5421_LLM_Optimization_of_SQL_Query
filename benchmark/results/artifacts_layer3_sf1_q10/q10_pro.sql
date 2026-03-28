WITH eligible_customers AS (
    SELECT s.csk
    FROM (
        SELECT DISTINCT ss_customer_sk AS csk
        FROM store_sales
        JOIN date_dim ON ss_sold_date_sk = d_date_sk
        WHERE d_year = 2002 AND d_moy BETWEEN 1 AND 4
    ) s
    INNER JOIN (
        SELECT DISTINCT ws_bill_customer_sk AS csk
        FROM web_sales
        JOIN date_dim ON ws_sold_date_sk = d_date_sk
        WHERE d_year = 2002 AND d_moy BETWEEN 1 AND 4
        UNION
        SELECT DISTINCT cs_ship_customer_sk AS csk
        FROM catalog_sales
        JOIN date_dim ON cs_sold_date_sk = d_date_sk
        WHERE d_year = 2002 AND d_moy BETWEEN 1 AND 4
    ) wc ON s.csk = wc.csk
)
SELECT
  cd_gender,
  cd_marital_status,
  cd_education_status,
  count(*) cnt1,
  cd_purchase_estimate,
  count(*) cnt2,
  cd_credit_rating,
  count(*) cnt3,
  cd_dep_count,
  count(*) cnt4,
  cd_dep_employed_count,
  count(*) cnt5,
  cd_dep_college_count,
  count(*) cnt6
FROM customer c
INNER JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
INNER JOIN customer_demographics ON cd_demo_sk = c.c_current_cdemo_sk
INNER JOIN eligible_customers ec ON c.c_customer_sk = ec.csk
WHERE ca_county IN ('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County', 'La Porte County')
GROUP BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count
ORDER BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count
LIMIT 100
