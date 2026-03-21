\set ON_ERROR_STOP on
\pset pager off

WITH expected(tab, expected_count) AS (
  VALUES
    ('call_center', 6::bigint),
    ('catalog_page', 11718::bigint),
    ('catalog_sales', 1441548::bigint),
    ('customer', 100000::bigint),
    ('customer_address', 50000::bigint),
    ('customer_demographics', 1920800::bigint),
    ('date_dim', 73049::bigint),
    ('household_demographics', 7200::bigint),
    ('income_band', 20::bigint),
    ('inventory', 11745000::bigint),
    ('item', 18000::bigint),
    ('promotion', 300::bigint),
    ('reason', 75::bigint),
    ('ship_mode', 20::bigint),
    ('store', 12::bigint),
    ('store_sales', 2880404::bigint),
    ('time_dim', 86400::bigint),
    ('warehouse', 5::bigint),
    ('web_page', 60::bigint),
    ('web_sales', 719384::bigint),
    ('web_site', 30::bigint)
),
actual(tab, actual_count) AS (
  SELECT 'call_center', count(*)::bigint FROM call_center
  UNION ALL SELECT 'catalog_page', count(*)::bigint FROM catalog_page
  UNION ALL SELECT 'catalog_sales', count(*)::bigint FROM catalog_sales
  UNION ALL SELECT 'customer', count(*)::bigint FROM customer
  UNION ALL SELECT 'customer_address', count(*)::bigint FROM customer_address
  UNION ALL SELECT 'customer_demographics', count(*)::bigint FROM customer_demographics
  UNION ALL SELECT 'date_dim', count(*)::bigint FROM date_dim
  UNION ALL SELECT 'household_demographics', count(*)::bigint FROM household_demographics
  UNION ALL SELECT 'income_band', count(*)::bigint FROM income_band
  UNION ALL SELECT 'inventory', count(*)::bigint FROM inventory
  UNION ALL SELECT 'item', count(*)::bigint FROM item
  UNION ALL SELECT 'promotion', count(*)::bigint FROM promotion
  UNION ALL SELECT 'reason', count(*)::bigint FROM reason
  UNION ALL SELECT 'ship_mode', count(*)::bigint FROM ship_mode
  UNION ALL SELECT 'store', count(*)::bigint FROM store
  UNION ALL SELECT 'store_sales', count(*)::bigint FROM store_sales
  UNION ALL SELECT 'time_dim', count(*)::bigint FROM time_dim
  UNION ALL SELECT 'warehouse', count(*)::bigint FROM warehouse
  UNION ALL SELECT 'web_page', count(*)::bigint FROM web_page
  UNION ALL SELECT 'web_sales', count(*)::bigint FROM web_sales
  UNION ALL SELECT 'web_site', count(*)::bigint FROM web_site
)
SELECT
  e.tab,
  e.expected_count,
  a.actual_count,
  (a.actual_count = e.expected_count) AS ok
FROM expected e
JOIN actual a USING (tab)
ORDER BY e.tab;

SELECT 'catalog_returns' AS tab, count(*)::bigint AS actual_count FROM catalog_returns
UNION ALL SELECT 'store_returns' AS tab, count(*)::bigint AS actual_count FROM store_returns
UNION ALL SELECT 'web_returns' AS tab, count(*)::bigint AS actual_count FROM web_returns
ORDER BY tab;
