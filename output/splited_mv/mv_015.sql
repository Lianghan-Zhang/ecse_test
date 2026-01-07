-- ============================================================
-- MV: mv_015
-- Fact Table: store_sales
-- Tables: date_dim, item, store, store_sales
-- Edges: d1.d_date_sk=store_sales.ss_sold_date_sk, item.i_item_sk=store_sales.ss_item_sk, store.s_store_sk=store_sales.ss_store_sk
-- QBs: q36.sql::qb::main:0::root, q50.sql::qb::main:0::root, q64.sql::qb::cte:cross_sales::root.with.cross_sales, q70.sql::qb::main:0::root
-- Columns: 39 columns
-- ============================================================
CREATE VIEW mv_015 AS
SELECT
  d1.d_year AS d1__d_year,
  d2.d_year AS d2__d_year,
  d3.d_year AS d3__d_year,
  i.i_item_sk,
  i.i_product_name,
  s.s_city,
  s.s_company_id,
  s.s_county,
  s.s_state,
  s.s_store_name,
  s.s_street_name,
  s.s_street_number,
  s.s_street_type,
  s.s_suite_number,
  s.s_zip,
  SUM(ss.ss_net_profit) AS sum_ss__ss_net_profit,
  SUM(ss.ss_ext_sales_price) AS sum_ss__ss_ext_sales_price,
  COUNT(*) AS cnt,
  SUM(ss.ss_wholesale_cost) AS s1,
  SUM(ss.ss_list_price) AS s2,
  SUM(ss.ss_coupon_amt) AS s3
FROM date_dim AS d1
INNER JOIN store_sales AS ss
  ON ss.ss_sold_date_sk = d1.d_date_sk
INNER JOIN item AS i
  ON i.i_item_sk = ss.ss_item_sk
INNER JOIN store AS s
  ON s.s_store_sk = ss.ss_store_sk
GROUP BY
  d1.d_year,
  d2.d_year,
  d3.d_year,
  i.i_item_sk,
  i.i_product_name,
  s.s_city,
  s.s_company_id,
  s.s_county,
  s.s_state,
  s.s_store_name,
  s.s_street_name,
  s.s_street_number,
  s.s_street_type,
  s.s_suite_number,
  s.s_zip;