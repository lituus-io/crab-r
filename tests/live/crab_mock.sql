-- Author: spicyzhug@gmail.com
-- Copyright: lituus-io, all rights reserved
--
-- The live mock warehouse: four layers built from bigquery-public-data's
-- thelook_ecommerce by real CTAS, INSERT, MERGE, a view, a dbt-style script
-- (DECLARE, temporary table, MERGE), an UPDATE and a copy. Two raw
-- descriptions are planted wrong (raw_orders.returned_at "never NULL";
-- raw_orders.status omits Cancelled and Returned) for Describe to flag.
--
-- Each block after a `-- job` line runs as its own query job (so the job
-- history shows one producer per table); `${DS}` is the dataset, e.g.
-- `my-project.crab_mock`. Run with tests/live/run_sql.py.

-- job
CREATE OR REPLACE TABLE `${DS}.raw_orders` AS
SELECT order_id, user_id, status, created_at, returned_at, shipped_at, delivered_at, num_of_item
FROM `bigquery-public-data.thelook_ecommerce.orders`;

-- job
ALTER TABLE `${DS}.raw_orders`
  ALTER COLUMN order_id SET OPTIONS (description = 'Order identifier.'),
  ALTER COLUMN user_id SET OPTIONS (description = 'Customer who placed the order.'),
  ALTER COLUMN status SET OPTIONS (description = 'One of: Shipped, Complete, Processing.'),
  ALTER COLUMN created_at SET OPTIONS (description = 'When the order was placed (UTC).'),
  ALTER COLUMN returned_at SET OPTIONS (description = 'Return timestamp; never NULL.');

-- job
CREATE OR REPLACE TABLE `${DS}.raw_users` AS
SELECT id, email, age, country, city, traffic_source, created_at
FROM `bigquery-public-data.thelook_ecommerce.users`;

-- job
ALTER TABLE `${DS}.raw_users`
  ALTER COLUMN id SET OPTIONS (description = 'Customer identifier.'),
  ALTER COLUMN country SET OPTIONS (description = 'Customer country as recorded at sign-up.'),
  ALTER COLUMN traffic_source SET OPTIONS (description = 'Marketing channel the customer arrived through.');

-- job
CREATE OR REPLACE TABLE `${DS}.raw_order_items` AS
SELECT id, order_id, product_id, status, sale_price, created_at, shipped_at
FROM `bigquery-public-data.thelook_ecommerce.order_items`;

-- job
CREATE OR REPLACE TABLE `${DS}.stg_orders` AS
SELECT order_id, user_id AS customer_id, status AS order_status, created_at AS ordered_at,
       DATE(created_at) AS order_date, returned_at
FROM `${DS}.raw_orders`
WHERE status != 'Cancelled';

-- job
CREATE OR REPLACE TABLE `${DS}.stg_customers` AS
SELECT id AS customer_id, email, country, traffic_source AS acquisition_channel
FROM `${DS}.raw_users`;

-- job
CREATE TABLE IF NOT EXISTS `${DS}.mart_customer_month` (
  customer_id INT64, month DATE, orders INT64, revenue NUMERIC);

-- job
INSERT INTO `${DS}.mart_customer_month` (customer_id, month, orders, revenue)
SELECT o.customer_id, DATE_TRUNC(o.order_date, MONTH) AS month, COUNT(DISTINCT o.order_id),
       CAST(SUM(i.sale_price) AS NUMERIC)
FROM `${DS}.stg_orders` o JOIN `${DS}.raw_order_items` i ON i.order_id = o.order_id
GROUP BY 1, 2;

-- job
CREATE TABLE IF NOT EXISTS `${DS}.dim_customer` (
  customer_id INT64, email STRING, country STRING, first_order_at TIMESTAMP, orders INT64);

-- job
MERGE `${DS}.dim_customer` d
USING (
  SELECT c.customer_id, c.email, c.country, MIN(o.ordered_at) AS first_order_at, COUNT(o.order_id) AS orders
  FROM `${DS}.stg_customers` c LEFT JOIN `${DS}.stg_orders` o USING (customer_id)
  GROUP BY 1, 2, 3
) s
ON d.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET email = s.email, country = s.country, first_order_at = s.first_order_at, orders = s.orders
WHEN NOT MATCHED THEN INSERT (customer_id, email, country, first_order_at, orders)
  VALUES (s.customer_id, s.email, s.country, s.first_order_at, s.orders);

-- job
CREATE OR REPLACE VIEW `${DS}.v_customer_revenue` AS
SELECT customer_id, SUM(revenue) AS lifetime_revenue, SUM(orders) AS lifetime_orders
FROM `${DS}.mart_customer_month`
GROUP BY customer_id;

-- job
CREATE OR REPLACE TABLE `${DS}.mart_customer_profile` AS
SELECT c.customer_id, c.country, COUNT(o.order_id) AS orders, MAX(o.ordered_at) AS last_order_at,
       CAST(NULL AS STRING) AS segment
FROM `${DS}.stg_customers` c LEFT JOIN `${DS}.stg_orders` o ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.country;

-- job
DECLARE cutoff DATE DEFAULT DATE '2024-01-01';
CREATE TEMP TABLE profile__tmp AS (
  WITH recent AS (
    SELECT customer_id, COUNT(*) AS n, MAX(ordered_at) AS last_at
    FROM `${DS}.stg_orders` WHERE order_date >= cutoff GROUP BY customer_id)
  SELECT c.customer_id, c.country, r.n AS orders, r.last_at AS last_order_at
  FROM recent r JOIN `${DS}.stg_customers` c USING (customer_id)
);
MERGE `${DS}.mart_customer_profile` T
USING (SELECT customer_id, country, orders, last_order_at FROM profile__tmp) S
ON T.customer_id = S.customer_id
WHEN MATCHED THEN UPDATE SET orders = S.orders, last_order_at = S.last_order_at
WHEN NOT MATCHED THEN INSERT (customer_id, country, orders, last_order_at)
  VALUES (customer_id, country, orders, last_order_at);

-- job
UPDATE `${DS}.mart_customer_profile` p
SET segment = c.acquisition_channel
FROM `${DS}.stg_customers` c
WHERE c.customer_id = p.customer_id AND p.orders > 0;
