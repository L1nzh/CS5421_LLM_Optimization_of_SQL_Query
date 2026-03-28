WITH valid_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2002
      AND d_moy BETWEEN 1 AND 4
),
store_eligible_customers AS (
    SELECT DISTINCT ss_customer_sk AS c_customer_sk
    FROM store_sales
    WHERE ss_sold_date_sk IN (SELECT d_date_sk FROM valid_dates)
),
web_catalog_eligible_customers AS (
    SELECT DISTINCT ws_bill_customer_sk AS c_customer_sk
    FROM web_sales
    WHERE ws_sold_date_sk IN (SELECT d_date_sk FROM valid_dates)
    UNION
    SELECT DISTINCT cs_ship_customer_sk AS c_customer_sk
    FROM catalog_sales
    WHERE cs_sold_date_sk IN (SELECT d_date_sk FROM valid_dates)
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
INNER JOIN customer_address ca
  ON c.c_current_addr_sk = ca.ca_address_sk
INNER JOIN customer_demographics
  ON cd_demo_sk = c.c_current_cdemo_sk
INNER JOIN store_eligible_customers sec
  ON c.c_customer_sk = sec.c_customer_sk
INNER JOIN web_catalog_eligible_customers wcec
  ON c.c_customer_sk = wcec.c_customer_sk
WHERE ca_county IN (
  'Rush County', 'Toole County', 'Jefferson County',
  'Dona Ana County', 'La Porte County'
)
GROUP BY 
  cd_gender,
  cd_marital_status,
  cd_education_status,
  cd_purchase_estimate,
  cd_credit_rating,
  cd_dep_count,
  cd_dep_employed_count,
  cd_dep_college_count
ORDER BY 
  cd_gender,
  cd_marital_status,
  cd_education_status,
  cd_purchase_estimate,
  cd_credit_rating,
  cd_dep_count,
  cd_dep_employed_count,
  cd_dep_college_count
LIMIT 100