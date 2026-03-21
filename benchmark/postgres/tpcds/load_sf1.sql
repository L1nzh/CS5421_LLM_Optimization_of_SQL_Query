\set ON_ERROR_STOP on
\pset pager off
\timing on

SET client_encoding TO 'UTF8';
SET synchronous_commit TO off;

TRUNCATE TABLE
  call_center,
  catalog_page,
  catalog_returns,
  catalog_sales,
  customer,
  customer_address,
  customer_demographics,
  date_dim,
  household_demographics,
  income_band,
  inventory,
  item,
  promotion,
  reason,
  ship_mode,
  store,
  store_returns,
  store_sales,
  time_dim,
  warehouse,
  web_page,
  web_returns,
  web_sales,
  web_site;

\copy call_center FROM 'datasets/tpcds/sf1/call_center.dat' WITH (FORMAT csv, DELIMITER '|', NULL '');
\copy catalog_page FROM 'datasets/tpcds/sf1/catalog_page.dat' WITH (FORMAT csv, DELIMITER '|', NULL '');
\copy customer_address FROM 'datasets/tpcds/sf1/customer_address.dat' WITH (FORMAT csv, DELIMITER '|', NULL '');
\copy customer_demographics FROM 'datasets/tpcds/sf1/customer_demographics.dat' WITH (FORMAT csv, DELIMITER '|', NULL '');
\copy date_dim FROM 'datasets/tpcds/sf1/date_dim.dat' WITH (FORMAT csv, DELIMITER '|', NULL '');
\copy household_demographics FROM 'datasets/tpcds/sf1/household_demographics.dat' WITH (FORMAT csv, DELIMITER '|', NULL '');
\copy income_band FROM 'datasets/tpcds/sf1/income_band.dat' WITH (FORMAT csv, DELIMITER '|', NULL '');
\copy item FROM 'datasets/tpcds/sf1/item.dat' WITH (FORMAT csv, DELIMITER '|', NULL '');
\copy promotion FROM 'datasets/tpcds/sf1/promotion.dat' WITH (FORMAT csv, DELIMITER '|', NULL '');
\copy reason FROM 'datasets/tpcds/sf1/reason.dat' WITH (FORMAT csv, DELIMITER '|', NULL '');
\copy ship_mode FROM 'datasets/tpcds/sf1/ship_mode.dat' WITH (FORMAT csv, DELIMITER '|', NULL '');
\copy store FROM 'datasets/tpcds/sf1/store.dat' WITH (FORMAT csv, DELIMITER '|', NULL '');
\copy time_dim FROM 'datasets/tpcds/sf1/time_dim.dat' WITH (FORMAT csv, DELIMITER '|', NULL '');
\copy warehouse FROM 'datasets/tpcds/sf1/warehouse.dat' WITH (FORMAT csv, DELIMITER '|', NULL '');
\copy web_page FROM 'datasets/tpcds/sf1/web_page.dat' WITH (FORMAT csv, DELIMITER '|', NULL '');
\copy web_site FROM 'datasets/tpcds/sf1/web_site.dat' WITH (FORMAT csv, DELIMITER '|', NULL '');
\copy customer FROM 'datasets/tpcds/sf1/customer.dat' WITH (FORMAT csv, DELIMITER '|', NULL '');

\copy inventory FROM 'datasets/tpcds/sf1/inventory.dat' WITH (FORMAT csv, DELIMITER '|', NULL '');
\copy store_sales FROM 'datasets/tpcds/sf1/store_sales.dat' WITH (FORMAT csv, DELIMITER '|', NULL '');
\copy store_returns FROM 'datasets/tpcds/sf1/store_returns.dat' WITH (FORMAT csv, DELIMITER '|', NULL '');
\copy catalog_sales FROM 'datasets/tpcds/sf1/catalog_sales.dat' WITH (FORMAT csv, DELIMITER '|', NULL '');
\copy catalog_returns FROM 'datasets/tpcds/sf1/catalog_returns.dat' WITH (FORMAT csv, DELIMITER '|', NULL '');
\copy web_sales FROM 'datasets/tpcds/sf1/web_sales.dat' WITH (FORMAT csv, DELIMITER '|', NULL '');
\copy web_returns FROM 'datasets/tpcds/sf1/web_returns.dat' WITH (FORMAT csv, DELIMITER '|', NULL '');
