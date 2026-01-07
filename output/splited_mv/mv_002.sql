-- ============================================================
-- MV: mv_002
-- Fact Table: store_sales
-- Tables: customer, customer_address, date_dim, item, store, store_sales
-- Edges: customer.c_current_addr_sk=customer_address.ca_address_sk, customer.c_customer_sk=store_sales.ss_customer_sk, date_dim.d_date_sk=store_sales.ss_sold_date_sk, ... (5 total)
-- QBs: q19.sql::qb::main:0::root, q61.sql::qb::subquery:1::root.from.subquery1, q61.sql::qb::subquery:2::root.join.subquery2
-- Columns: 24 columns
-- ============================================================
CREATE VIEW mv_002 AS
SELECT
  i.i_brand,
  i.i_brand_id,
  i.i_manufact,
  i.i_manufact_id,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price
FROM customer AS c
INNER JOIN customer_address AS ca
  ON ca.ca_address_sk = c.c_current_addr_sk
INNER JOIN store_sales AS ss
  ON ss.ss_customer_sk = c.c_customer_sk
INNER JOIN date_dim AS d
  ON d.d_date_sk = ss.ss_sold_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = ss.ss_item_sk
INNER JOIN store AS s
  ON s.s_store_sk = ss.ss_store_sk
GROUP BY
  i.i_brand,
  i.i_brand_id,
  i.i_manufact,
  i.i_manufact_id;