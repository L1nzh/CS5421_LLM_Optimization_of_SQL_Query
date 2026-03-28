WITH qualifying_customers AS (
    SELECT DISTINCT c.c_customer_sk
    FROM customer c
    JOIN store_sales ss ON c.c_customer_sk = ss.ss_customer_sk
    JOIN date_dim d_ss ON ss.ss_sold_date_sk = d_ss.d_date_sk
    WHERE d_ss.d_year = 2002 AND d_ss.d_moy BETWEEN 1 AND 4
    AND EXISTS (
        SELECT 1
        FROM web_sales ws
        JOIN date_dim d_ws ON ws.ws_sold_date_sk = d_ws.d_date_sk
        WHERE ws.ws_bill_customer_sk = c.c_customer_sk
          AND d_ws.d_year = 2002 AND d_ws.d_moy BETWEEN 1 AND 4
        UNION ALL
        SELECT 1
        FROM catalog_sales cs
        JOIN date_dim d_cs ON cs.cs_sold_date_sk = d_cs.d_date_sk
        WHERE cs.cs_ship_customer_sk = c.c_customer_sk
          AND d_cs.d_year = 2002 AND d_cs.d_moy BETWEEN 1 AND 4
    )
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
JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics ON cd_demo_sk = c.c_current_cdemo_sk
JOIN qualifying_customers qc ON c.c_customer_sk = qc.c_customer_sk
WHERE ca.ca_county IN ('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County', 'La Porte County')
GROUP BY cd_gender,
  cd_marital_status,
  cd_education_status,
  cd_purchase_estimate,
  cd_credit_rating,
  cd_dep_count,
  cd_dep_employed_count,
  cd_dep_college_count
ORDER BY cd_gender,
  cd_marital_status,
  cd_education_status,
  cd_purchase_estimate,
  cd_credit_rating,
  cd_dep_count,
  cd_dep_employed_count,
  cd_dep_college_count
LIMIT 100