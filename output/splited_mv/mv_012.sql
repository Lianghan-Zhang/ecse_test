-- ============================================================
-- MV: mv_012
-- Fact Table: store_sales
-- Tables: customer, item, store, store_sales
-- Edges: customer.c_customer_sk=store_sales.ss_customer_sk, item.i_item_sk=store_sales.ss_item_sk, store.s_store_sk=store_sales.ss_store_sk
-- QBs: q19.sql::qb::main:0::root, q24a.sql::qb::cte:ssales::root.with.ssales, q24b.sql::qb::cte:ssales::root.with.ssales, q61.sql::qb::subquery:1::root.from.subquery1, q61.sql::qb::subquery:2::root.join.subquery2, ... (6 total)
-- Columns: 41 columns
-- ============================================================
CREATE VIEW mv_012 AS
SELECT
  c.c_first_name,
  c.c_last_name,
  i.i_brand,
  i.i_brand_id,
  i.i_color,
  i.i_current_price,
  i.i_item_sk,
  i.i_manager_id,
  i.i_manufact,
  i.i_manufact_id,
  i.i_product_name,
  i.i_size,
  i.i_units,
  s.s_state,
  s.s_store_name,
  s.s_zip,
  COUNT(*) AS cnt,
  SUM(ss.ss_wholesale_cost) AS s1,
  SUM(ss.ss_list_price) AS s2,
  SUM(ss.ss_coupon_amt) AS s3,
  SUM(ss.ss_net_paid) AS netpaid,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price
FROM customer AS c
INNER JOIN store_sales AS ss
  ON ss.ss_customer_sk = c.c_customer_sk
INNER JOIN item AS i
  ON i.i_item_sk = ss.ss_item_sk
INNER JOIN store AS s
  ON s.s_store_sk = ss.ss_store_sk
GROUP BY
  c.c_first_name,
  c.c_last_name,
  i.i_brand,
  i.i_brand_id,
  i.i_color,
  i.i_current_price,
  i.i_item_sk,
  i.i_manager_id,
  i.i_manufact,
  i.i_manufact_id,
  i.i_product_name,
  i.i_size,
  i.i_units,
  s.s_state,
  s.s_store_name,
  s.s_zip;